package no.nordicsemi.kotlin.mesh.core.layers.lowertransport

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * simdo 2026-09-28 — SAR 송신 진단 카운터 (프로세스 전역, 단조 증가). 동작에는 영향이 없다.
 *
 * 앱이 설정 구간 앞뒤 값의 차로 "이번 설정에서 폰이 unicast 분할을 몇 개 다시 보냈나" 를 로그로 남긴다
 * (연결 간격 실험의 부작용 측정).
 *
 * 2026-09-29 (동시 GATT 설정) — 여러 노드를 동시에 설정하면 전역 값이 섞인다. 목적지(unicast) 별 값을 함께 센다
 * ([retransmissionsTo], [giveUpsTo]). 노드 하나의 값 = 그 노드 엘리먼트 주소들의 합.
 */
object SarDiagnostics {
    /** SAR Unicast Retransmissions 타이머가 다시 보낸 분할 수 (첫 송신 제외). */
    val unicastSegmentRetransmissions = AtomicLong(0)

    /** 재전송 횟수를 다 써서 unicast 분할 송신을 포기한 수. */
    val unicastGiveUps = AtomicLong(0)

    private val retransmissionsByDestination = ConcurrentHashMap<Int, AtomicLong>()
    private val giveUpsByDestination = ConcurrentHashMap<Int, AtomicLong>()

    /** [destination] 으로 다시 보낸 분할 수 (단조 증가). */
    fun retransmissionsTo(destination: Int): Long = retransmissionsByDestination[destination]?.get() ?: 0L

    /** [destination] 으로의 분할 송신 포기 수 (단조 증가). */
    fun giveUpsTo(destination: Int): Long = giveUpsByDestination[destination]?.get() ?: 0L

    internal fun recordRetransmissions(destination: Int, segments: Long) {
        unicastSegmentRetransmissions.addAndGet(segments)
        retransmissionsByDestination.getOrPut(destination) { AtomicLong(0) }.addAndGet(segments)
    }

    internal fun recordGiveUp(destination: Int) {
        unicastGiveUps.incrementAndGet()
        giveUpsByDestination.getOrPut(destination) { AtomicLong(0) }.incrementAndGet()
    }
}
