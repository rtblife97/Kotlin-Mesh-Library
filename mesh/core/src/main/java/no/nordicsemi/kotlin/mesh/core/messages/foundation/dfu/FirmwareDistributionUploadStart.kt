package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.data.getUInt
import no.nordicsemi.kotlin.data.getULong
import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareId
import java.nio.ByteOrder

/**
 * Firmware Distribution Upload Start message is an acknowledged message sent by Firmware
 * Distribution Client to start a firmware image upload to a Firmware Distribution Server.
 *
 * @property ttl            Time To Live (TTL) value used in a firmware image upload.
 *                          Valid values are in the range 0...127 (`0x00 - 0x7F`). Value 255 (`0xFF`)
 *                          means that the default TTL value is to be used. Other values are Prohibited.
 * @property timeoutBase    Value that is used to calculate when firmware image upload will be suspended.
 *                          The Timeout is calculated using the following formula:
 *                          `Timeout = 10 × (Timeout Base + 1)` seconds.
 * @property blobId         BLOB identifier for the firmware image to be uploaded.
 * @property firmwareSize   Size of the firmware image, in octets.
 * @property metadata       Optional vendor-specific firmware metadata.
 * @property firmwareId     Firmware ID identifying the firmware image being uploaded.
 */
class FirmwareDistributionUploadStart(
    val ttl: UByte = 0xFFu,
    val timeoutBase: UShort,
    val blobId: ULong,
    val firmwareSize: UInt,
    val metadata: ByteArray? = null,
    val firmwareId: FirmwareId,
) : AcknowledgedMeshMessage {
    override val opCode: UInt = Initializer.opCode
    override val responseOpCode = FirmwareDistributionUploadStatus.opCode
    // simdo-patch(mesh-dfu backport): 인코더가 두 군데 틀렸다.
    //   ① Metadata Length 옥텟(U8)이 통째로 빠짐 ② metadata 와 firmwareId 순서가 뒤바뀜.
    //   NCS dfd_srv.c handle_upload_start():
    //     ttl(u8) | timeout_base(le16) | blob_id(le64) | size(le32) |
    //     meta_len(u8) | meta[meta_len] | fwid[나머지]
    //   고정부 16옥텟 = BT_MESH_LEN_MIN(16). 이 클래스의 **자기 디코더**(params[15] 를
    //   metadataLength 로, offset 16 을 metadata 로, 16+len 을 fwid 로 읽음)가 이미 올바른
    //   레이아웃을 가정하고 있었다 — 인코더만 어긋나 round-trip 이 깨져 있었다.
    //   ※ 우리 배포 모델은 SMP ingest 라 in-band upload 를 쓰지 않지만, 자기모순은 남기지 않는다.
    override val parameters: ByteArray
        get() {
            val meta = metadata ?: byteArrayOf()
            require(meta.size <= 255) {
                "Firmware metadata must not exceed 255 bytes!"
            }
            return ttl.toByteArray() +
                    timeoutBase.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    blobId.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    firmwareSize.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                    meta.size.toUByte().toByteArray() +
                    meta +
                    firmwareId.bytes
        }

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode = 0x831Fu

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size >= 18 }
            ?.let { params ->
                val ttl = params[0].toUByte()
                val timeoutBase = params.getUShort(
                    offset = 1,
                    order = ByteOrder.LITTLE_ENDIAN
                )
                val blobId = params.getULong(offset = 3, order = ByteOrder.LITTLE_ENDIAN)
                val firmwareSize = params.getUInt(
                    offset = 11,
                    order = ByteOrder.LITTLE_ENDIAN
                )
                val metadataLength = params[15].toInt()
                FirmwareDistributionUploadStart(
                    ttl = ttl,
                    timeoutBase = timeoutBase,
                    blobId = blobId,
                    firmwareSize = firmwareSize,
                    metadata = if (metadataLength > 0) {
                        require(params.size >= 18 + metadataLength) {
                            return null
                        }
                        params.copyOfRange(fromIndex = 16, toIndex = 16 + metadataLength)
                    } else {
                        null
                    },
                    firmwareId = if (params.size > 18 + metadataLength) {
                        FirmwareId(
                            companyIdentifier = params.getUShort(
                                offset = 16 + metadataLength,
                                order = ByteOrder.LITTLE_ENDIAN
                            ),
                            version = params.copyOfRange(
                                fromIndex = 18 + metadataLength,
                                toIndex = params.size
                            )
                        )
                    } else {
                        FirmwareId(
                            companyIdentifier = params.getUShort(
                                offset = 16 + metadataLength,
                                order = ByteOrder.LITTLE_ENDIAN
                            )
                        )
                    }
                )
            }
    }
}