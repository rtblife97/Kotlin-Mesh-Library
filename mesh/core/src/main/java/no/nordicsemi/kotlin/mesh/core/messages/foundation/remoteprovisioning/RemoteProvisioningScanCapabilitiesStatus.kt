package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningResponse

/**
 * A Remote Provisioning Scan Capabilities Status message is an unacknowledged message used by
 * the Remote Provisioning Server to report the current value of its Remote Provisioning Scan
 * Capabilities state.
 *
 * It is sent as a response to a [RemoteProvisioningScanCapabilitiesGet] message.
 *
 * Wire: `MaxScannedItems (1) | ActiveScan (1)` — `BT_MESH_LEN_EXACT(2)` in `rpr_cli.c`.
 *
 * @property maxScannedItems     The maximum number of Device UUIDs the server can report during
 *                               a single scan. The minimum defined by the specification is 4,
 *                               the maximum 255. Our `neo_mesh_commissioner` dongle reports 32
 *                               (`CONFIG_BT_MESH_RPR_SRV_SCANNED_ITEMS_MAX`).
 * @property activeScanSupported Whether the server supports active scanning (i.e. sending
 *                               SCAN_REQ, which is required to collect scan-response data such
 *                               as the device name during an Extended Scan).
 */
class RemoteProvisioningScanCapabilitiesStatus(
    val maxScannedItems: UByte,
    val activeScanSupported: Boolean,
) : RemoteProvisioningResponse {
    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(
        maxScannedItems.toByte(),
        if (activeScanSupported) 0x01 else 0x00
    )

    override fun toString() = "RemoteProvisioningScanCapabilitiesStatus(" +
            "maxScannedItems: $maxScannedItems, " +
            "activeScanSupported: $activeScanSupported)"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8050u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 2 }
            ?.let { params ->
                RemoteProvisioningScanCapabilitiesStatus(
                    maxScannedItems = params[0].toUByte(),
                    // The server writes `true` (0x01); treat any non-zero as supported rather
                    // than discarding the message, as iOS does with a strict `== 0x01`.
                    activeScanSupported = params[1].toInt() != 0x00
                )
            }
    }
}
