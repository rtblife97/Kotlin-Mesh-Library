package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.data.getUInt
import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.MeshResponse
import no.nordicsemi.kotlin.mesh.core.model.UriScheme
import java.nio.ByteOrder

/**
 * Firmware Distribution Capabilities Status message is an unacknowledged message sent by a Firmware
 * Distribution Server to report Distributor capabilities.
 *
 * This message is sent as a response to a [FirmwareDistributionCapabilitiesGet] message.
 *
 * @property maxReceiversCount          Maximum number of entries in the Distribution Receivers List
 *                                      state.
 * @property maxFirmwareImagesListSize  Maximum number of entries in the Firmware Images List state.
 * @property maxFirmwareImageSize       Maximum size of a firmware image in octets.
 * @property maxUploadSpace             Total space dedicated to storage of firmware images in
 *                                      octets.
 * @property remainingUploadSpace       Remaining available space in firmware image storage in
 *                                      octets.
 * @property supportedUriSchemes        Supported Out-of-Band URI schemes. An empty array means, OOB
 *                                      Retrieval is not supported.
 */
class FirmwareDistributionCapabilitiesStatus(
    val maxReceiversCount: UShort,
    val maxFirmwareImagesListSize: UShort,
    val maxFirmwareImageSize: UInt,
    val maxUploadSpace: UInt,
    val remainingUploadSpace: UInt,
    val supportedUriSchemes: List<UriScheme>,
) : MeshResponse {
    override val opCode: UInt = Initializer.opCode
    override val parameters: ByteArray
        get() {
            val data = maxReceiversCount.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    maxFirmwareImagesListSize.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    maxFirmwareImageSize.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    maxUploadSpace.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    remainingUploadSpace.toByteArray(order = ByteOrder.LITTLE_ENDIAN)

            // simdo-patch(mesh-dfu backport): OOB Supported 플래그는 **1 옥텟**이다
            //   (NCS dfd_srv.c handle_capabilities_get(): net_buf_simple_add_u8).
            //   upstream 은 `0x00u` / `0x01u` 를 썼는데 이는 UInt 리터럴이고
            //   `UInt.toByteArray()` 의 기본값이 (BIG_ENDIAN, length = 4) 라서 4 옥텟 빅엔디언이
            //   나갔다 → 메시지가 17 이 아니라 20 옥텟이 되고 플래그가 offset 16 이 아닌 19 에
            //   놓인다. 같은 클래스의 디코더(offset 16 고정)조차 자기 출력을 못 읽는다.
            //   UByte 로 명시해 1 옥텟을 강제한다.
            return when (supportedUriSchemes.isEmpty()) {
                true -> data + OOB_RETRIEVAL_UNSUPPORTED.toByteArray()
                false -> data + supportedUriSchemes
                    .fold(initial = OOB_RETRIEVAL_SUPPORTED.toByteArray()) { acc, uri ->
                        acc + uri.rawValue.toByteArray()
                    }
            }
        }

    override fun toString() = "FirmwareDistributionCapabilitiesStatus(" +
            "Max Receivers Count: $maxReceiversCount, " +
            "Max Firmware Images List Size: $maxFirmwareImagesListSize, " +
            "Max Firmware Image Size: $maxFirmwareImageSize, " +
            "Max Upload Space: $maxUploadSpace, " +
            "Remaining Upload Space: $remainingUploadSpace, " +
            "Supported URI Schemes: ${supportedUriSchemes.joinToString(separator = ", ")})"

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode: UInt = 0x8317u

        /** Out-of-Band 펌웨어 이미지 조회 미지원. 1 옥텟. */
        private const val OOB_RETRIEVAL_UNSUPPORTED: UByte = 0x00u

        /** Out-of-Band 펌웨어 이미지 조회 지원. 1 옥텟. 뒤에 지원 URI scheme 목록이 붙는다. */
        private const val OOB_RETRIEVAL_SUPPORTED: UByte = 0x01u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size >= 17 }
            ?.let { params ->
                FirmwareDistributionCapabilitiesStatus(
                    maxReceiversCount = params.getUShort(
                        offset = 0,
                        order = ByteOrder.LITTLE_ENDIAN
                    ),
                    maxFirmwareImagesListSize = params.getUShort(
                        offset = 2,
                        order = ByteOrder.LITTLE_ENDIAN
                    ),
                    maxFirmwareImageSize = params.getUInt(
                        offset = 4,
                        order = ByteOrder.LITTLE_ENDIAN
                    ),
                    maxUploadSpace = params.getUInt(offset = 8, order = ByteOrder.LITTLE_ENDIAN),
                    remainingUploadSpace = params.getUInt(
                        offset = 12,
                        order = ByteOrder.LITTLE_ENDIAN
                    ),
                    supportedUriSchemes = when (params[16].toUByte() == 0x01.toUByte()) {
                        true -> if (params.size >= 18) {
                            params
                                .slice(indices = 17 until parameters.size)
                                .mapNotNull { UriScheme.from(rawValue = it.toUByte()) }
                        } else return@let null

                        else -> emptyList()
                    }
                )
            }
    }
}