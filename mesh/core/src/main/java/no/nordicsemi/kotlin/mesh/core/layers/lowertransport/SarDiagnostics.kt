package no.nordicsemi.kotlin.mesh.core.layers.lowertransport

import java.util.concurrent.atomic.AtomicLong

/**
 * simdo 2026-09-28 — SAR 송신 진단 카운터 (프로세스 전역, 단조 증가). 동작에는 영향이 없다.
 *
 * 앱이 설정 구간 앞뒤 값의 차로 "이번 설정에서 폰이 unicast 분할을 몇 개 다시 보냈나" 를 로그로 남긴다
 * (연결 간격 실험의 부작용 측정). 여러 노드를 동시에 설정하면 서로 섞인다 — 순차 설정에서만 노드별 값이 정확하다.
 */
object SarDiagnostics {
    /** SAR Unicast Retransmissions 타이머가 다시 보낸 분할 수 (첫 송신 제외). */
    val unicastSegmentRetransmissions = AtomicLong(0)

    /** 재전송 횟수를 다 써서 unicast 분할 송신을 포기한 수. */
    val unicastGiveUps = AtomicLong(0)
}
