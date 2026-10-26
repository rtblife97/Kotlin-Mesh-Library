package no.nordicsemi.kotlin.mesh.core

import java.util.concurrent.atomic.AtomicLong

/**
 * simdo-fork (2026-10-26, #80) — 망 전체 Node Identity 해석([no.nordicsemi.kotlin.mesh.core.model.MeshNetwork.node] 의 NodeIdentity 판)
 * 진단 (프로세스 전역). 동작에는 영향이 없다.
 *
 * 2026-10-26 S21U 597노드: 광고 1건 해석이 노드 잠금(nodesMonitor)을 쥔 채 130~450 ms 걸려, 그동안 수신·송신 경로의 노드 찾기가
 * 섰다 (ART `Long monitor contention`, 조명 2번째부터 메시지 750~1,000 ms). 앱이 설정 구간 앞에서 [mark], 끝에서 [since] 로
 * "이 구간에 전체 해석을 몇 번, 몇 ms, 그중 잠금을 쥔 시간" 을 `설정 측정` 줄에 남긴다.
 */
object NodeIdentityDiagnostics {
    private val resolves = AtomicLong(0)
    private val matchNanos = AtomicLong(0)
    private val lockNanos = AtomicLong(0)

    data class Mark internal constructor(val resolves: Long, val matchNanos: Long, val lockNanos: Long)

    /**
     * @property resolves 전체 해석 수.
     * @property matchMs  해시 대조에 쓴 시간 합 (ms, 잠금 밖).
     * @property lockMs   노드 목록 스냅샷(노드 잠금 대기·보유)에 든 시간 합 (ms). 해시 대조는 잠금 밖이라 여기 안 든다.
     */
    data class Window(val resolves: Long, val matchMs: Double, val lockMs: Double)

    fun mark(): Mark = Mark(resolves.get(), matchNanos.get(), lockNanos.get())

    fun since(mark: Mark): Window = Window(
        resolves = resolves.get() - mark.resolves,
        matchMs = (matchNanos.get() - mark.matchNanos) / 1_000_000.0,
        lockMs = (lockNanos.get() - mark.lockNanos) / 1_000_000.0,
    )

    internal fun record(matchNanos: Long, lockNanos: Long) {
        resolves.incrementAndGet()
        this.matchNanos.addAndGet(matchNanos)
        this.lockNanos.addAndGet(lockNanos)
    }
}
