package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer

/**
 * Firmware Distribution Receivers Delete All message is an acknowledged message sent by a
 * Firmware Distribution Client to remove all entries from the Distribution Receivers List state of
 * a Firmware Distribution Server.
 *
 * The message has no parameters and is answered with a
 * [FirmwareDistributionReceiversStatus] message.
 */
// simdo-patch(mesh-dfu backport): upstream feature/mesh-dfu 의 이 클래스는
//   FirmwareDistributionReceiversGet 의 복사본이었다. 두 군데가 틀렸다.
//   ① parameters — 스펙/펌웨어상 이 메시지는 **파라미터가 없다**.
//      NCS dfd_srv.c: { BT_MESH_DFD_OP_RECEIVERS_DELETE_ALL, BT_MESH_LEN_EXACT(0), ... }
//      기존 구현은 firstIndex+entriesLimit 4 바이트를 보내 길이 검사에서 드롭됐다.
//   ② responseOpCode — 응답은 ReceiversList(0x8315) 가 아니라 **ReceiversStatus(0x8313)** 다.
//      NCS dfd_srv.c handle_receivers_delete_all() → receivers_status_rsp() →
//      BT_MESH_DFD_OP_RECEIVERS_STATUS. (같은 백포트의 FirmwareDistributionReceiversStatus
//      KDoc 도 "ReceiversAdd 또는 ReceiversDeleteAll 의 응답" 이라고 스스로 명시한다.)
//      → 기존 값으로는 응답을 영영 못 맞춰 타임아웃한다.
//   회귀 가드: FirmwareDistributionWireEncodingTest.
class FirmwareDistributionReceiversDeleteAll : AcknowledgedMeshMessage {
    override val opCode: UInt = Initializer.opCode
    override val responseOpCode: UInt = FirmwareDistributionReceiversStatus.opCode
    override val parameters = null

    override fun toString() = "FirmwareDistributionReceiversDeleteAll()"

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode: UInt = 0x8312u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.isEmpty() }
            ?.let { FirmwareDistributionReceiversDeleteAll() }
    }
}
