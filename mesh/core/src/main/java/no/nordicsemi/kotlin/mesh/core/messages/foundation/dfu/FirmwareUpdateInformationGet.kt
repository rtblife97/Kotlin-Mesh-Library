package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer


/**
 * Firmware Update Information Get message is an acknowledged message used to get information about
 * the firmware images installed on a Node.
 *
 * @property firstIndex   First Index field shall indicate the first entry on the Firmware
 *                        Information List state of the Firmware Update Server to return in the
 *                        Firmware Update Information Status message.
 * @property entriesLimit Entries Limit field shall indicate the maximum number of Firmware
 *                        Information Entry fields to return in the Firmware Update Information
 *                        Status message.
 */
class FirmwareUpdateInformationGet(
    val firstIndex: UByte,
    val entriesLimit: UByte,
) : AcknowledgedMeshMessage {
    override val opCode: UInt = Initializer.opCode
    override val responseOpCode = FirmwareUpdateInformationStatus.opCode
    override val parameters = firstIndex.toByteArray() + entriesLimit.toByteArray()

    override fun toString() =
        "FirmwareUpdateInformationGet(firstIndex: $firstIndex, entriesLimit: $entriesLimit)"

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode: UInt = 0x8308u

        /**
         * Requests the firmware information entries starting at [firstIndex].
         *
         * simdo-patch (2026-08-12, 감사 P1-3 부수): 백포트가 딸려온 무인자 생성자
         * `FirmwareUpdateInformationGet()` 는 `entriesLimit = 0` 이었다. NCS
         * `dfu_srv.c handle_info_get()` 는 그 경우 헤더(`img_count`, `idx`)만 담은 응답을
         * 보내고 **엔트리를 하나도 싣지 않는다**(`limit > 0` 루프 조건). 드롭은 아니지만
         * "정보를 가져온다"는 이름과 달리 조용히 빈 목록이 오는 함정이라 제거하고,
         * 의도를 이름으로 드러내는 팩토리 둘로 대체했다. 기존 호출처 0건.
         *
         * @param firstIndex   First entry to return.
         * @param entriesLimit Maximum number of entries to return. Defaults to 0xFF (all).
         */
        fun all(firstIndex: UByte = 0u, entriesLimit: UByte = 0xFFu) =
            FirmwareUpdateInformationGet(firstIndex = firstIndex, entriesLimit = entriesLimit)

        /**
         * Requests only the total number of entries in the Firmware Information List state.
         *
         * The response carries `listCount` and `firstIndex` but **no entries** — use [all] to
         * actually read the firmware IDs.
         */
        fun count() = FirmwareUpdateInformationGet(firstIndex = 0u, entriesLimit = 0u)

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 2 }
            ?.let {
                FirmwareUpdateInformationGet(
                    firstIndex = it[0].toUByte(),
                    entriesLimit = it[1].toUByte()
                )
            }
    }
}