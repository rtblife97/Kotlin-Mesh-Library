@file:Suppress("unused")

package no.nordicsemi.kotlin.mesh.core.layers.lowertransport

import no.nordicsemi.kotlin.data.and
import no.nordicsemi.kotlin.data.hasBitSet
import no.nordicsemi.kotlin.data.shl
import no.nordicsemi.kotlin.data.shr
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkPdu
import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import kotlin.experimental.or

/**
 * Data class defining a Segmented Control Message.
 *
 * @property opCode             Message Op Code.
 */
internal class SegmentedControlMessage(
    // Control Message
    override val opCode: UByte,
    override val source: MeshAddress,
    override val destination: MeshAddress,
    override val networkKey: NetworkKey,
    override val ivIndex: UInt,
    override val upperTransportPdu: ByteArray,
    // Segmented Message
    override val message: MeshMessage? = null,
    override val userInitialized: Boolean = false,
    override val sequenceZero: UShort,
    override val segmentOffset: UByte,
    override val lastSegmentNumber: UByte,
) : ControlMessage(opCode, source, destination, networkKey, ivIndex, upperTransportPdu),
    SegmentedMessage {

    override val transportPdu: ByteArray
        get() {
            val octet0 = 0x80.toByte() or (opCode.toByte() and 0x7F)
            val octet1 = (sequenceZero shr 5).toByte()
            val octet2 =
                ((sequenceZero and 0x3Fu) shl 2).toByte() or (segmentOffset.toByte() and 0x07)
            val octet3 = (segmentOffset.toByte() and 0x07) or (lastSegmentNumber.toByte() and 0x07)
            return byteArrayOf(octet0, octet1, octet2, octet3) + upperTransportPdu
        }

    override val type = LowerTransportPduType.CONTROL_MESSAGE

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString() = "Segmented $type (opCode: ${
            opCode.toHexString(
                format = HexFormat {
                    number.prefix = "0x"
                    upperCase = true
                }
            )
        }, seqZero: $sequenceZero, " +
          "segO: $segmentOffset, segN: $lastSegmentNumber, " +
          "data: 0x${upperTransportPdu.toHexString(HexFormat.UpperCase)})"

    internal companion object {

        /**
         * Creates a Segmented Control Message using the given Network PDU.
         *
         * @param pdu Network pdu containing the segmented control message.
         * @return Segmented Control Message containing the decoded data.
         */
        fun init(pdu: NetworkPdu): SegmentedControlMessage? {
            // Minimum length of a Access Message is 6 bytes:
            // * 1 byte for SEG | AKF | AID
            // * 3 bytes for SZMIC | SeqZero | SegO | SegN
            // * At least 1 byte of segment payload
            require(pdu.transportPdu.size >= 5) { return null }

            // Make sure the SEG is 0, that is the message is segmented.
            require(pdu.transportPdu[0] hasBitSet 7) { return null } // TODO Change exception?

            val opCode = pdu.transportPdu[0].toUByte() and 0x7Fu
            require(opCode != 0x00.toUByte()) { return null } // Op Code 0 is reserved for future use.

            // simdo-fork (2026-09-28) — SeqZero 하위 6비트는 octet 2 의 상위 6비트다. 종전 `Byte.toUShort()` 는
            // 부호를 확장해(0xF4 → 0xFFF4) 하위 6비트가 32 이상인 SeqZero(전체의 절반)를 0x3FFx 로 읽었다 →
            // 그 ACK 는 어느 송신에도 맞지 않아 버려지고 분할 송신은 재전송 횟수를 다 쓸 때까지 계속됐다
            // (실기기 14:46:21 seqZero 125 → 16381). 옥텟을 부호 없이 읽는다.
            val sequenceZero = (((pdu.transportPdu[1].toInt() and 0x7F) shl 6) or
                    ((pdu.transportPdu[2].toInt() and 0xFF) ushr 2)).toUShort()
            val segmentOffset = ((pdu.transportPdu[2].toUByte() and 0x03u) shl 3) or
                    (pdu.transportPdu[3].toUByte() shr 5)
            val lastSegmentNumber = pdu.transportPdu[3].toUByte() and 0x1Fu

            // Make sure SegO is less than or equal to SegN.
            require(segmentOffset <= lastSegmentNumber) { return null } // TODO Change exception?

            return SegmentedControlMessage(
                opCode = opCode,
                source = pdu.source,
                destination = pdu.destination,
                networkKey = pdu.key,
                ivIndex = pdu.ivIndex,
                upperTransportPdu = pdu.transportPdu.copyOfRange(
                    fromIndex = 4,
                    toIndex = pdu.transportPdu.size
                ),
                sequenceZero = sequenceZero,
                segmentOffset = segmentOffset,
                lastSegmentNumber = lastSegmentNumber,
            )
        }
    }
}
