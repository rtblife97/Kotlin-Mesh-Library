package no.nordicsemi.kotlin.mesh.core.layers.network

import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.AccessMessage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPduType
import no.nordicsemi.kotlin.mesh.core.layers.uppertransport.UpperTransportPdu
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.crypto.Crypto

/**
 * simdo 2026-10-17 — **공중 수동 청취용** 순수 복호기 (작업자 앱 갱내 무IP 주변 차량, 경로 A).
 *
 * 폰이 광고 베어러에서 들은 Network PDU(AD type 0x2A 본문)를 lib 수신과 같은 복호기([NetworkPduDecoder]·[AccessMessage]·
 * [UpperTransportPdu])로 접근 계층까지 푼다. lib 의 정상 수신(`NetworkLayer.handle`)과 달리:
 *  - 로컬 노드·구독·프록시 필터를 보지 않는다 → 남의 목적지(그룹·unicast) 메시지도 풀린다 (Mesh Profile 1.0.1 §3.4.6.3 의
 *    "로컬 요소 대상인가" 검사가 없다).
 *  - 망 상태를 바꾸지 않는다 — Network Message Cache·RPL(§3.8.8)·IV Index 갱신·SAR 상태·SEQ 어느 것도 건드리지 않는다.
 *    재생 공격 방어(RPL)가 없으므로 결과는 **표시 참고값**으로만 쓴다 (제어 판단 금지).
 *  - 범위: **비분할 Access 메시지 + AppKey(AKF=1)** 만 (§3.5.2.1, 접근 PDU ≤ 11 B). 분할(SEG=1)·제어(CTL=1)·DevKey 메시지는 null.
 *
 * 암호 연산은 lib 와 같은 [Crypto] (호출마다 새 AES 엔진 — 2026-09-29 경합 수정 이후 공유 가변 상태 없음).
 */
object AirAccessDecoder {

    /**
     * 난독화만 푼 헤더 (§3.8.7.3). **NetMIC 는 검증하지 않았다** — NID 가 우연히 같은 남의 망 PDU(1/128)면 값이 쓰레기다.
     * 중복 제거 키(src+SEQ)를 싸게 얻는 용도이고, 진짜 여부는 [decodeAccess] 가 MIC 로 가린다.
     */
    data class Header(
        val nid: Int,
        val ctl: Boolean,
        val ttl: Int,
        val sequence: Long,
        val source: Int,
        /** 이 PDU 에 쓴 IV Index (IVI 비트로 고른 값). */
        val ivIndex: Long,
    )

    /** 접근 계층까지 풀린 메시지 하나. [accessPdu] = opcode 포함 접근 페이로드 원문. */
    class Access(
        val source: Int,
        val destination: Int,
        val sequence: Long,
        val ivIndex: Long,
        val ttl: Int,
        val accessPdu: ByteArray,
    )

    /** 망의 NID(7비트) 집합 — 현재 키 + Key Refresh 중이면 옛 키. 첫 바이트 하위 7비트와 비교해 남의 망 사본을 즉시 버린다. */
    fun networkIds(network: MeshNetwork): Set<Int> = buildSet {
        for (k in network.networkKeys) {
            add(k.derivatives.nid.toInt() and 0x7F)
            k.oldDerivatives?.let { add(it.nid.toInt() and 0x7F) }
        }
    }

    /** NID 가 맞는 키로 난독화만 푼다. 우리 NID 가 아니거나 길이가 모자라면 null. */
    fun peekHeader(pdu: ByteArray, network: MeshNetwork): Header? {
        if (pdu.size < MIN_PDU) return null
        val ivi = ((pdu[0].toInt() ushr 7) and 0x01).toByte()
        val nid = pdu[0].toInt() and 0x7F
        val iv = network.ivIndex.index(ivi = ivi)
        for (k in network.networkKeys) {
            val d = listOfNotNull(k.derivatives, k.oldDerivatives).firstOrNull { (it.nid.toInt() and 0x7F) == nid } ?: continue
            val h = runCatching {
                Crypto.obfuscate(
                    data = pdu.copyOfRange(1, 7),
                    random = pdu.copyOfRange(7, 14),
                    ivIndex = iv,
                    privacyKey = d.privacyKey,
                )
            }.getOrNull() ?: continue
            val b = IntArray(6) { h[it].toInt() and 0xFF }
            return Header(
                nid = nid,
                ctl = (b[0] and 0x80) != 0,
                ttl = b[0] and 0x7F,
                sequence = ((b[1] shl 16) or (b[2] shl 8) or b[3]).toLong(),
                source = (b[4] shl 8) or b[5],
                ivIndex = iv.toLong(),
            )
        }
        return null
    }

    /**
     * Network PDU → 접근 페이로드. 범위 밖(분할·제어·DevKey)이거나 우리 키로 안 풀리면(NetMIC·TransMIC 실패) null.
     * AppKey 는 PDU 를 푼 NetKey 에 묶인 것 중 AID 가 맞는 것 (가상 주소면 망의 가상 그룹 Label UUID 로).
     */
    fun decodeAccess(pdu: ByteArray, network: MeshNetwork): Access? = runCatching {
        val net = NetworkPduDecoder.decode(pdu, PduType.NETWORK_PDU, network) ?: return null
        if (net.type != LowerTransportPduType.ACCESS_MESSAGE || net.isSegmented) return null
        // AKF(0x40) = 0 이면 DevKey — 설정 메시지는 이 경로의 범위가 아니다.
        if ((net.transportPdu[0].toInt() and 0x40) == 0) return null
        val msg = AccessMessage.init(net)
        val (upper, _) = UpperTransportPdu.decode(msg, network) ?: return null
        Access(
            source = net.source.address.toInt(),
            destination = net.destination.address.toInt(),
            sequence = net.sequence.toLong(),
            ivIndex = net.ivIndex.toLong(),
            ttl = net.ttl.toInt(),
            accessPdu = upper.accessPdu,
        )
    }.getOrNull()

    /** IVI·NID 1 + 난독화 6 + DST 2 + 최소 전송 1 + NetMIC 4 (§3.4.4). */
    private const val MIN_PDU = 14
}
