package no.nordicsemi.kotlin.mesh.core

import no.nordicsemi.kotlin.mesh.core.model.Address

/**
 * simdo-fork (2026-10-09) — 수신한 Heartbeat 메시지 (Mesh Profile 1.0.1 §3.6.5.10, §3.6.7) 한 건.
 *
 * 라이브러리는 Heartbeat 를 로컬 노드의 Heartbeat Subscription 카운트 갱신에만 썼다. SIG 구독은
 * source 가 unicast 하나라 여러 노드의 Heartbeat 를 한 sink 그룹으로 모아 받는 용도에 쓸 수 없다.
 * 그래서 구독 일치 여부와 **무관하게** 복호에 성공한 Heartbeat 를 [MeshNetworkManager.heartbeats]
 * 로 모두 내보낸다. 구독 카운트 갱신(기존 동작)은 그대로다.
 *
 * @property source        송신 노드의 Primary Element unicast 주소.
 * @property destination   Heartbeat 목적지 (unicast 또는 그룹, 예: sink 그룹 0xC0F0).
 * @property initialTtl    송신 노드가 실은 InitTTL (7 bit).
 * @property receivedTtl   Network PDU 의 TTL (수신 시점, 중계마다 1 감소).
 * @property hops          InitTTL - RecvTTL + 1 (§3.6.7.1, 직결 = 1).
 * @property features      Features 필드 원값 (16 bit, big-endian 해석). bit0 Relay, bit1 Proxy,
 *                         bit2 Friend, bit3 Low Power — 1 = 그 기능 사용 중.
 * @property ivIndex       메시지를 복호한 IV Index.
 * @property receivedAtMillis 이 스택이 받은 시각 (epoch ms).
 */
data class ReceivedHeartbeat(
    val source: Address,
    val destination: Address,
    val initialTtl: UByte,
    val receivedTtl: UByte,
    val hops: UByte,
    val features: UShort,
    val ivIndex: UInt,
    val receivedAtMillis: Long,
) {
    val isRelayActive: Boolean get() = features.toInt() and 0x01 != 0
    val isProxyActive: Boolean get() = features.toInt() and 0x02 != 0
    val isFriendActive: Boolean get() = features.toInt() and 0x04 != 0
    val isLowPowerActive: Boolean get() = features.toInt() and 0x08 != 0
}
