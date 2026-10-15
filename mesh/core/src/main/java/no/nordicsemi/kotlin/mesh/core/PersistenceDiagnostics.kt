package no.nordicsemi.kotlin.mesh.core

import java.util.concurrent.atomic.AtomicLong

/**
 * simdo-fork (2026-10-15) — 망 저장(CDB export + 파일 쓰기) 과 수신 경로 대기 진단 (프로세스 전역). 동작에는 영향이 없다.
 *
 * 앱이 설정 구간 앞에서 [mark] 를 받고 끝에서 [since] 로 "이번 설정 동안 저장을 몇 번 했나, export 가 얼마 걸렸나, 응답이 수신 경로에서
 * 얼마나 기다렸나" 를 `설정 측정` 줄에 남긴다 (2026-10-15 진단: 설정 왕복이 CDB 노드 수에 비례해 느려진 근원 = 수신 메시지마다 망 전체 저장).
 *
 *  - 저장: [MeshNetworkManager] 가 실제로 export·쓰기를 마칠 때마다 1회 ([recordSave]). 합침 저장·명시 저장 모두.
 *  - 수신 대기: 메시지의 마지막 Network PDU 가 하위 전송 계층(`LowerTransportLayer.handle`) 에 들어온 시각부터 수신 흐름
 *    (`NetworkManager._incomingMeshMessages`) 에 내보내기 직전까지 ([recordRxWait]). 하위 전송 잠금 대기·재조립·상위 전송 복호·
 *    Access 처리(CDB 잠금 대기 포함)가 들어간다.
 *
 * 표본은 최근 [CAPACITY] 개만 둔다 (오래된 것부터 덮어씀). 구간이 그보다 길면 남아 있는 표본만 돌려준다.
 */
object PersistenceDiagnostics {
    private const val CAPACITY = 4096

    /** 실제로 끝난 저장 수 (단조 증가). */
    val saves = AtomicLong(0)

    private val exportRing = Ring(CAPACITY)
    private val rxWaitRing = Ring(CAPACITY)

    /** 구간 시작점. */
    data class Mark internal constructor(val saves: Long, val exportSeq: Long, val rxWaitSeq: Long)

    /**
     * [mark] 이후 구간.
     *
     * @property saves     저장 수.
     * @property exportMs  저장마다 export(망 전체 직렬화) 에 걸린 시간 (ms).
     * @property rxWaitMs  수신 메시지마다 하위 전송 진입 → 수신 흐름 내보내기 직전 (ms). [since] 의 `sources` 로 거른 것.
     */
    data class Window(val saves: Long, val exportMs: List<Double>, val rxWaitMs: List<Double>)

    fun mark(): Mark = Mark(saves.get(), exportRing.seq(), rxWaitRing.seq())

    /** [mark] 이후 구간. [sources] 가 있으면 그 주소에서 온 수신 메시지만 센다 (동시 설정에서 다른 노드와 섞이지 않게). */
    fun since(mark: Mark, sources: ((Int) -> Boolean)? = null): Window = Window(
        saves = saves.get() - mark.saves,
        exportMs = exportRing.since(mark.exportSeq) { true },
        rxWaitMs = rxWaitRing.since(mark.rxWaitSeq) { sources?.invoke(it) ?: true },
    )

    internal fun recordSave(exportNanos: Long) {
        saves.incrementAndGet()
        exportRing.add(key = 0, nanos = exportNanos)
    }

    internal fun recordRxWait(source: Int, nanos: Long) = rxWaitRing.add(key = source, nanos = nanos)

    /** (키, 값) 고정 크기 고리. 쓰기는 드물고(저장) 짧아서(수신) 단순 잠금으로 충분하다. */
    private class Ring(private val capacity: Int) {
        private val keys = IntArray(capacity)
        private val values = LongArray(capacity)
        private var next = 0L

        @Synchronized
        fun add(key: Int, nanos: Long) {
            val i = (next % capacity).toInt()
            keys[i] = key
            values[i] = nanos
            next++
        }

        @Synchronized
        fun seq(): Long = next

        @Synchronized
        fun since(from: Long, accept: (Int) -> Boolean): List<Double> {
            val start = maxOf(from, next - capacity)
            val out = ArrayList<Double>((next - start).toInt().coerceAtLeast(0))
            var s = start
            while (s < next) {
                val i = (s % capacity).toInt()
                if (accept(keys[i])) out += values[i] / 1_000_000.0
                s++
            }
            return out
        }
    }
}
