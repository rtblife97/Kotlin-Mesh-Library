@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package no.nordicsemi.kotlin.mesh.core.layers.lowertransport

import no.nordicsemi.kotlin.data.and
import no.nordicsemi.kotlin.data.getUInt
import no.nordicsemi.kotlin.data.hasBitCleared
import no.nordicsemi.kotlin.data.hasBitSet
import no.nordicsemi.kotlin.data.shl
import no.nordicsemi.kotlin.data.shr
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.exception.InvalidPdu
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkPdu
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey

/**
 * Internal data class defining a Segment Acknowledgement Message.
 *
 * @property opCode                    Message Op Code.
 * @property isOnBehalfOfLowePowerNode Flag indicating whether the message is on behalf of a low
 *                                     power node.
 * @property sequenceZero              13 least significant bits of SeqAuth.
 * @property ackedSegments             Block acknowledgement for segments, bit field.
 * @property isBusy                    Flag indicating whether the node is already busy processing a
 *                                     message and should ignore any incoming messages.
 */
internal class SegmentAcknowledgementMessage(
    // Control Message
    override val source: MeshAddress,
    override val destination: MeshAddress,
    override val networkKey: NetworkKey,
    override val ivIndex: UInt,
    override val upperTransportPdu: ByteArray,
    // Additional
    val isOnBehalfOfLowePowerNode: Boolean = false, // Friend feature not supported
    val sequenceZero: UShort,
    val ackedSegments: UInt,
) : ControlMessage(OP_CODE, source, destination, networkKey, ivIndex, upperTransportPdu) {

    override val type = LowerTransportPduType.CONTROL_MESSAGE

    val isBusy: Boolean
        get() = ackedSegments == 0u

    override val transportPdu: ByteArray
        get() {
            val octet0 = opCode.toByte() and 0x7F // Always 0 for SAM
            val octet1 = if (isOnBehalfOfLowePowerNode)
                0x80u.toByte() else (sequenceZero shr 6).toByte()
            val octet2 = (sequenceZero and 0x3Fu).toByte() shl 2
            return byteArrayOf(octet0, octet1, octet2) + upperTransportPdu
        }

    /**
     * Checks if the segment with the given sequence number has been received.
     *
     * @param m Segment number.
     * @return true if the segment has been received or false otherwise.
     */
    fun isSegmentReceived(m: Int) = ackedSegments hasBitSet m

    /**
     * Checks if all segments have been received.
     *
     * @param segments List of segmented messages.
     * @return true if all segments have been received or false otherwise.
     */
    fun areAllSegmentsReceived(segments: List<SegmentedMessage?>): Boolean =
        areAllSegmentsReceived(segments.size - 1)

    /**
     * Checks if all segments have been received.
     *
     * @param lastSegmentNumber Last segment number.
     * @return true if all segments have been received or false otherwise.
     */
    fun areAllSegmentsReceived(lastSegmentNumber: Int) =
        ackedSegments == ((1 shl (lastSegmentNumber + 1)) - 1).toUInt()

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString() = "ACK (seqZero: $sequenceZero, acked segments: ${
        ackedSegments.toHexString(
            format = HexFormat {
                number.prefix = "0x"
                upperCase = true
            }
        )
    })"

    internal companion object {
        val OP_CODE: UByte = 0x00u

        /**
         * Creates a segmented acknowledgement message using the given [NetworkPdu].
         *
         * @param pdu The network pdu containing the segment acknowledgement message.
         * @return The decoded [SegmentAcknowledgementMessage] or null if the pdu was invalid.
         */
        fun init(pdu: NetworkPdu): SegmentAcknowledgementMessage {
            // Length of a Segment Acknowledgment Message is 7 bytes:
            // * 1 byte for SEG | AKF | AID
            // * 2 byte for OBO | SeqZero | RFU
            // * 4 bytes for the block acknowledgment
            require(pdu.transportPdu.size == 7) { throw InvalidPdu() }

            // Make sure the SEG is 0, that is the message is unsegmented.
            require(pdu.transportPdu[0] hasBitCleared 7) { throw InvalidPdu() }

            // OpCode for Segment Acknowledgement Message is 0x00.
            val opCode = (pdu.transportPdu[0] and 0x7F).toUByte()
            require(opCode == 0x00.toUByte()) { throw InvalidPdu() }

            val isOnBehalfOfLowePowerNode = pdu.transportPdu[1] hasBitSet 7
            // simdo-fork (2026-09-28) — SeqZero 하위 6비트는 octet 2 의 상위 6비트다. 종전 `Byte.toUShort()` 는
            // 부호를 확장해(0xF4 → 0xFFF4) 하위 6비트가 32 이상인 SeqZero(전체의 절반)를 0x3FFx 로 읽었다 →
            // 그 ACK 는 어느 송신에도 맞지 않아 버려지고 분할 송신은 재전송 횟수를 다 쓸 때까지 계속됐다
            // (실기기 14:46:21 seqZero 125 → 16381). 옥텟을 부호 없이 읽는다.
            val sequenceZero: UShort = (((pdu.transportPdu[1].toInt() and 0x7F) shl 6) or
                    ((pdu.transportPdu[2].toInt() and 0xFF) ushr 2)).toUShort()
            val ackedSegments = pdu.transportPdu.getUInt(offset = 3)
            val upperTransportPdu = pdu.transportPdu.copyOfRange(3, pdu.transportPdu.size)
            return SegmentAcknowledgementMessage(
                source = pdu.source,
                destination = pdu.destination,
                networkKey = pdu.key,
                ivIndex = pdu.ivIndex,
                upperTransportPdu = upperTransportPdu,
                isOnBehalfOfLowePowerNode = isOnBehalfOfLowePowerNode,
                sequenceZero = sequenceZero,
                ackedSegments = ackedSegments,
            )
        }

        /**
         * Creates a Segment Acknowledgement Message using the given List of [SegmentedMessage]s.
         *
         * @param segments List of segmented messages containing the segment acknowledgement
         *                 message.
         * @return The decoded [SegmentAcknowledgementMessage] or null if the pdu was invalid.
         */
        fun init(segments: List<SegmentedMessage?>): SegmentAcknowledgementMessage {
            val segment = segments.firstOrNull { it != null }
            require(segment != null) { throw InvalidPdu() }

            // Create the block acknowledgement
            val ack = segments.fold(0u) { acc, seg ->
                seg?.let { acc or ((1u shl seg.segmentOffset.toInt())) } ?: acc
            }

            return SegmentAcknowledgementMessage(
                source = segment.destination,
                destination = segment.source,
                networkKey = segment.networkKey,
                ivIndex = segment.ivIndex,
                upperTransportPdu = ack.toByteArray(),
                sequenceZero = segment.sequenceZero,
                ackedSegments = ack,
            )
        }
    }
}
