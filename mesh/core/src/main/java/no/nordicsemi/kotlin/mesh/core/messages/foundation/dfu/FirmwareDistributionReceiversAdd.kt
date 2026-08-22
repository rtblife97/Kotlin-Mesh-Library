package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import java.nio.ByteOrder

/**
 * Firmware Distribution Receivers Add message is an acknowledged message sent by a Firmware
 * Distribution Client to add new entries to the Distribution Receivers List state of a Firmware
 * Distribution Server.
 *
 * The message carries a list of Updating Node Entries. Each entry is 3 octets long and consists of
 * the target node's Address (2 octets, little endian) followed by the Update Firmware Image Index
 * (1 octet).
 *
 * @property receivers List of nodes to be added to the Distribution Receivers List state.
 *                     Must not be empty.
 */
// simdo-patch(mesh-dfu backport): upstream feature/mesh-dfu 의 이 클래스는
//   FirmwareDistributionReceiversGet 의 복사본이었다 (firstIndex/entriesLimit = 4 바이트 전송).
//   실제 스펙/펌웨어는 3옥텟 Updating Node Entry 의 가변 리스트를 요구한다.
//   NCS zephyr/subsys/bluetooth/mesh/dfd_srv.c:
//     { BT_MESH_DFD_OP_RECEIVERS_ADD, BT_MESH_LEN_MIN(3), handle_receivers_add }
//     handle_receivers_add(): `if (buf->len % 3) return -EINVAL;` 후
//                             `addr = pull_le16(); img_idx = pull_u8();` 를 반복.
//   → 기존 구현은 4바이트를 보내 4 % 3 != 0 으로 **펌웨어가 무응답 드롭**, 클라이언트는
//     ReceiversStatus 를 영영 못 받고 타임아웃한다. Distribution 자체가 성립 불가.
//   회귀 가드: FirmwareDistributionWireEncodingTest.
class FirmwareDistributionReceiversAdd(
    val receivers: List<Receiver>,
) : AcknowledgedMeshMessage {
    override val opCode: UInt = Initializer.opCode
    override val responseOpCode: UInt = FirmwareDistributionReceiversStatus.opCode
    override val parameters: ByteArray = receivers
        .fold(initial = byteArrayOf()) { acc, receiver ->
            acc + receiver.address.address.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    receiver.imageIndex.toByteArray()
        }

    init {
        require(receivers.isNotEmpty()) {
            "Firmware Distribution Receivers Add must contain at least one receiver!"
        }
    }

    override fun toString() =
        "FirmwareDistributionReceiversAdd(receivers: ${receivers.joinToString(separator = ", ")})"

    /**
     * A single Updating Node Entry of the Distribution Receivers List state.
     *
     * @property address    Unicast Address of the target node.
     * @property imageIndex Index of the firmware image on the target node, that is to be updated.
     */
    data class Receiver(
        val address: UnicastAddress,
        val imageIndex: UByte,
    ) {
        override fun toString() = "(address: $address, imageIndex: $imageIndex)"
    }

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode: UInt = 0x8311u

        /** Length of a single Updating Node Entry, in octets. */
        private const val ENTRY_LENGTH = 3

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.isNotEmpty() && it.size % ENTRY_LENGTH == 0 }
            ?.let { params ->
                FirmwareDistributionReceiversAdd(
                    receivers = (params.indices step ENTRY_LENGTH).map { offset ->
                        Receiver(
                            address = UnicastAddress(
                                address = params.getUShort(
                                    offset = offset,
                                    order = ByteOrder.LITTLE_ENDIAN
                                )
                            ),
                            imageIndex = params[offset + 2].toUByte()
                        )
                    }
                )
            }
    }
}
