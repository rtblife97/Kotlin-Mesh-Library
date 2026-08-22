package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.data.bigEndian
import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.or
import no.nordicsemi.kotlin.data.shl
import no.nordicsemi.kotlin.data.shr
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.BLOBTransferMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareUpdateMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.MeshResponse
import no.nordicsemi.kotlin.mesh.core.messages.RetrievedUpdatePhase
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import java.nio.ByteOrder
import kotlin.collections.fold


/**
 * The Firmware Distribution Receivers List message is an unacknowledged message sent by the
 * Firmware Distribution Server to report the firmware distribution status of each receiver.
 *
 * Firmware Distribution Receivers List message is sent as a response to a
 * [FirmwareDistributionReceiversGet] message.
 *
 * @property totalCount   Total number of entries in the Distribution Receivers List state.
 * @property firstIndex   Index of the first requested entry from the Distribution Receivers List state.
 * @property receivers    List of receivers requested from the Distribution Receivers List state.
 *                        The list starts at [FirmwareDistributionReceiversGet.firstIndex] and contains
 *                        at most [FirmwareDistributionReceiversGet.entriesLimit] entries.
 *                        The list is empty if no Entries were found within the requested range.
 */
class FirmwareDistributionReceiversList(
    val totalCount: UShort,
    val firstIndex: UShort,
    val receivers: List<ReceiverStatus>,
) : MeshResponse {
    override val opCode: UInt = Initializer.opCode

    @OptIn(ExperimentalUnsignedTypes::class)
    override val parameters: ByteArray
        get() {
            val initial = totalCount.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    firstIndex.toByteArray(order = ByteOrder.LITTLE_ENDIAN)

            return receivers.fold(initial) { data, target ->
                // Encoding is specified in Table 5.20 in Mesh DFU Specification.
                val byte0: UByte = (target.address and 0xFFu).toUByte()
                val byte1: UByte =
                    (((target.address shr 8) and 0x7Fu).toUByte() or (target.phase.value shl 7))
                val byte2: UByte =
                    ((target.phase.value shr 1) or
                            (target.updateStatus.value shl 3) or
                            (target.transferStatus.value shl 6))
                val byte3: UByte =
                    ((target.transferStatus.value shr 2) or
                            ((target.transferProgress shr 1) shl 2))
                val byte4: UByte = target.imageIndex

                data + ubyteArrayOf(byte0, byte1, byte2, byte3, byte4).toByteArray()
            }
        }

    constructor(
        request: FirmwareDistributionReceiversGet,
        receivers: List<ReceiverStatus>,
    ) : this(
        totalCount = receivers.size.toUShort(),
        firstIndex = request.firstIndex,
        receivers = if (request.firstIndex < receivers.size.toUInt()) {
            val start = request.firstIndex
            val end = (start + request.entriesLimit)
                .coerceAtMost(maximumValue = receivers.size.toUInt())
            receivers.subList(start.toInt(), end.toInt())
        } else {
            emptyList()
        },
    )

    /**
     *
     */
    data class ReceiverStatus(
        val address: Address,
        val phase: RetrievedUpdatePhase,
        val updateStatus: FirmwareUpdateMessageStatus,
        val transferStatus: BLOBTransferMessageStatus,
        val transferProgress: Int,
        val imageIndex: UByte,
    ) {
        val debugDescription = "ReceiverStatus(address: ${
            address.toHexString(
                HexFormat {
                    number.prefix = "0x"
                    upperCase = true
                }
            )
        }, phase: ${phase.debugDescription}, " +
                "updateStatus: ${updateStatus.debugDescription}, " +
                "transferStatus: ${transferStatus.debugDescription}, " +
                "transferProgress: $transferProgress, " +
                "imageIndex: $imageIndex)"

    }

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode: UInt = 0x8315u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size >= 4 && (it.size - 4) % 5 == 0 }
            ?.let { params ->
                val totalCount = params.getUShort(offset = 0, order = ByteOrder.LITTLE_ENDIAN)
                val firstIndex = params.getUShort(offset = 2, order = ByteOrder.LITTLE_ENDIAN)
                val receivers = mutableListOf<ReceiverStatus>()
                for (i in 4 until params.size step 5) {
                    val address =
                        (params[i].toInt() and 0xFF) or
                                ((params[i + 1].toInt() and 0x7F) shl 8)

                    // simdo-patch(mesh-dfu backport): `Byte.toInt()` 는 부호확장을 한다.
                    //   byte1 의 bit7 = Retrieved Update Phase 의 bit0 이므로, phase 가 홀수
                    //   (TRANSFER_ERROR/VERIFYING_UPDATE/VERIFICATION_FAILED/TRANSFER_CANCELED/
                    //    APPLY_FAILED) 이면 byte1 >= 0x80 이 되어 upstream 의 마스크 없는
                    //   `ushr 7` 이 0x01FFFFFF 를 내고 → from() null → 아래 `?: return null` 로
                    //   **메시지 전체가 폐기**된다. 실패 phase 일수록 못 본다.
                    //   shift 전에 0xFF 로 마스크해야 한다.
                    val phaseValue =
                        (((params[i + 1].toInt() and 0xFF) ushr 7) or
                                ((params[i + 2].toInt() and 0x07) shl 1))

                    val phase =
                        RetrievedUpdatePhase.from(value = phaseValue.toUByte()) ?: return null
                    val updateStateValue = (params[i + 2].toInt() ushr 3) and 0x07

                    val updateStatus =
                        FirmwareUpdateMessageStatus.from(value = updateStateValue.toUByte())
                            ?: return null

                    // simdo-patch(mesh-dfu backport): 위와 동일한 부호확장 결함.
                    //   byte2 의 bit7 = Transfer(BLOB) Status 의 bit1 → INVALID_BLOCK_SIZE(0x02)/
                    //   WRONG_BLOB_ID(0x06)/BLOB_TOO_LARGE(0x07)/INFORMATION_UNAVAILABLE(0x0A)
                    //   에서 byte2 >= 0x80 → 메시지 전체 폐기. 운영자가 가장 봐야 할 실패값들이다.
                    val transferStateValue =
                        (((params[i + 2].toInt() and 0xFF) ushr 6) or
                                ((params[i + 3].toInt() and 0x03) shl 2))

                    val transferStatus =
                        BLOBTransferMessageStatus.from(value = transferStateValue.toUByte())
                            ?: return null

                    // simdo-patch(mesh-dfu backport): 같은 부호확장 결함. byte3 은 진행률 >= 64%
                    //   에서 0x80 을 넘는다. 100% (펌웨어는 progress/2 = 50 → byte3 = 0xC8) 일 때
                    //   upstream 은 100 대신 2147483620 을 냈다. 드롭이 아니라 **조용한 오값**이다.
                    val transferProgress = (((params[i + 3].toInt() and 0xFF) ushr 2) * 2)

                    val imageIndex = params[i + 4]

                    receivers.add(
                        ReceiverStatus(
                            address = address.toUShort(),
                            phase = phase,
                            updateStatus = updateStatus,
                            transferStatus = transferStatus,
                            transferProgress = transferProgress,
                            imageIndex = imageIndex.toUByte()
                        )
                    )

                }
                FirmwareDistributionReceiversList(
                    totalCount = totalCount,
                    firstIndex = firstIndex,
                    receivers = receivers
                )
            }
    }
}