package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.data.shl
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.data.ushr
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareUpdateAdditionalInformation
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareUpdateMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.MeshResponse
import kotlin.experimental.and


/**
 * Firmware Update Firmware Metadata Status message is an unacknowledged message sent to a Firmware
 * Update Client that is used to report whether a Firmware Update Server can accept a firmware
 * update.
 *
 * The Firmware Update Firmware Metadata Status message is sent in response to a
 * [FirmwareUpdateFirmwareMetadataCheck] message.
 */
// simdo-patch(mesh-dfu backport): Additional Information 은 5비트 필드라 0x04~0x1F 가
//   wire 상 합법(RFU)이다. upstream 은 매핑 실패 시 `?: return@let null` 로 **메시지 전체**를
//   버렸다 — 유효한 Status 옥텟과 image index 까지 같이. 그러면 응답이 유실돼 타임아웃만 남는다.
//   NCS 는 raw 값을 저장하고 계속 진행한다. 같은 백포트의 FirmwareUpdateStatus 도 이 필드를
//   nullable 로 관대하게 다룬다 — 여기만 엄격했다.
//   ★ 이 메시지는 배포 전에 EFFECT_UNPROV(= 업데이트 후 노드가 unprovisioned 가 되는지) 를
//     알아내는 유일한 프로토콜 수단이므로, "모르면 조용히 버린다" 가 최악의 동작이다.
//     매핑 실패 시에도 [rawAdditionalInformation] 으로 원값을 노출해 호출자가 보수적으로
//     판단(= 모르면 위험한 것으로 취급)할 수 있게 한다.
class FirmwareUpdateFirmwareMetadataStatus(
    val status: FirmwareUpdateMessageStatus,
    val additionalInformation: FirmwareUpdateAdditionalInformation?,
    val imageIndex: UByte,
    /** Additional Information 의 원본 5비트 값. 알려진 enum 으로 매핑되지 않아도 보존된다. */
    val rawAdditionalInformation: UByte = additionalInformation?.value ?: 0x00u,
) : MeshResponse {
    override val opCode: UInt = Initializer.opCode
    override val parameters: ByteArray
        get() {
            val byte0 = status.value and 0x07u or (rawAdditionalInformation shl 3)
            return byte0.toByteArray() + imageIndex.toByteArray()
        }

    /**
     * Convenience constructor to create a Firmware Update Firmware Metadata Status message.
     *
     * @param request               Firmware Update Firmware Metadata Check message to response to.
     * @param status                Status from the firmware metadata check. This should be one of
     *                              [FirmwareUpdateMessageStatus/success],
     *                              [FirmwareUpdateMessageStatus/metadataCheckFailed], or
     *                              [FirmwareUpdateMessageStatus/wrongFirmwareIndex].
     * @param additionalInformation Firmware Update Additional Information state from the Firmware
     *                              Update Server.
     */
    @Suppress("unused")
    constructor(
        request: FirmwareUpdateFirmwareMetadataCheck,
        status: FirmwareUpdateMessageStatus,
        additionalInformation: FirmwareUpdateAdditionalInformation,
    ) : this(
        status = status,
        additionalInformation = additionalInformation,
        imageIndex = request.imageIndex
    )

    override fun toString() = "FirmwareUpdateFirmwareMetadataStatus(status: ${status.debugDescription}, " +
            "additionalInformation: ${
                additionalInformation?.debugDescription
                    ?: "RFU(0x${rawAdditionalInformation.toString(16)})"
            }, " +
            "imageIndex: $imageIndex)"

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode: UInt = 0x830Bu

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 2 }
            ?.let { params ->
                FirmwareUpdateFirmwareMetadataStatus(
                    status = FirmwareUpdateMessageStatus.from(
                        value = (params[0] and 0x07).toUByte()
                    ) ?: return@let null,
                    additionalInformation = FirmwareUpdateAdditionalInformation.from(
                        value = (params[0] ushr 3).toUByte()
                    ),
                    imageIndex = params[1].toUByte(),
                    rawAdditionalInformation = (params[0] ushr 3).toUByte()
                )
            }
    }
}