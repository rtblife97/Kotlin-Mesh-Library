package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer
import java.nio.ByteOrder

/**
 * Firmware Distribution Receivers Get message is an acknowledged message sent by the Firmware
 * Distribution Client to get the firmware distribution status of each Target Node.
 *
 * simdo-patch (2026-08-12, 감사 P1-3): `entriesLimit = 0` 가드 추가.
 * NCS `dfd_srv.c handle_receivers_get()` 의 첫 검사가 `if (cnt == 0) return -EINVAL;` 이라
 * **응답 자체가 오지 않는다**. 배포 진행률 폴링 루프가 원인 불명 타임아웃으로 굳는 자리.
 * RPR 메시지들은 같은 부류(서버 `-EINVAL` = 무응답)를 전부 `require` 로 선차단했는데
 * 백포트된 DFD 쪽만 빠져 있었다.
 *
 * @property firstIndex   Index of the first Target Node entry to return.
 * @property entriesLimit Maximum number of entries to return. **Must be greater than 0** — the
 *                        Firmware Distribution Server drops a request with 0 without responding.
 */
class FirmwareDistributionReceiversGet(
    val firstIndex: UShort,
    val entriesLimit: UShort
) : AcknowledgedMeshMessage {

    init {
        require(entriesLimit > 0u) {
            "entriesLimit must be greater than 0; the Firmware Distribution Server drops a " +
                    "Receivers Get with Entries Limit 0 without sending any response"
        }
    }

    override val opCode: UInt = Initializer.opCode
    override val responseOpCode: UInt = FirmwareDistributionReceiversList.opCode
    override val parameters = firstIndex.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            entriesLimit.toByteArray(order = ByteOrder.LITTLE_ENDIAN)

    override fun toString() = "FirmwareDistributionReceiversGet(" +
            "firstIndex: $firstIndex, entriesLimit: $entriesLimit)"

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode: UInt = 0x8314u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf {
                // 서버가 -EINVAL 로 버리는 PDU 는 디코더도 받아들이지 않는다.
                it.size == 4 && it.getUShort(offset = 2, order = ByteOrder.LITTLE_ENDIAN) > 0u
            }
            ?.let { params ->
                FirmwareDistributionReceiversGet(
                    firstIndex = params.getUShort(
                        offset = 0,
                        order = ByteOrder.LITTLE_ENDIAN
                    ),
                    entriesLimit = params.getUShort(
                        offset = 2,
                        order = ByteOrder.LITTLE_ENDIAN
                    )
                )
            }
    }
}