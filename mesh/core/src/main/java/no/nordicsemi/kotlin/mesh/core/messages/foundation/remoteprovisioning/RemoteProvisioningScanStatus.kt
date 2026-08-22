package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningResponse
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningScanState
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningStatusMessage
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A Remote Provisioning Scan Status message is an unacknowledged message used by the Remote
 * Provisioning Server to report the current value of its Remote Provisioning Scan Parameters
 * state and Remote Provisioning Scan state.
 *
 * It is sent as a response to [RemoteProvisioningScanGet], [RemoteProvisioningScanStart] and
 * [RemoteProvisioningScanStop].
 *
 * Wire: `Status (1) | ScanState (1) | ScannedItemsLimit (1) | Timeout (1)` —
 * `BT_MESH_LEN_EXACT(4)` in `rpr_cli.c`.
 *
 * @property status            Status of the requested operation.
 * @property scanningState     The current Remote Provisioning Scan state of the server.
 * @property scannedItemsLimit The scanned items limit currently in effect. Note that after a
 *                             successful Scan Start with limit `0` the server echoes **its own**
 *                             maximum here, not `0`.
 * @property timeout           Remaining time limit for the running scan.
 */
class RemoteProvisioningScanStatus(
    override val status: RemoteProvisioningMessageStatus,
    val scanningState: RemoteProvisioningScanState,
    val scannedItemsLimit: UByte,
    val timeout: Duration,
) : RemoteProvisioningResponse, RemoteProvisioningStatusMessage {

    /** The Timeout field value as encoded on the wire, in seconds. */
    val timeoutSeconds: UByte = timeout.inWholeSeconds.coerceIn(0, 255).toInt().toUByte()

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(
        status.value.toByte(),
        scanningState.value.toByte(),
        scannedItemsLimit.toByte(),
        timeoutSeconds.toByte()
    )

    override fun toString() = "RemoteProvisioningScanStatus(" +
            "status: $status, " +
            "scanningState: $scanningState, " +
            "scannedItemsLimit: $scannedItemsLimit, " +
            "timeout: $timeoutSeconds s)"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8054u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 4 }
            ?.let { params ->
                val status = RemoteProvisioningMessageStatus.from(params[0].toUByte())
                    ?: return@let null
                val scanState = RemoteProvisioningScanState.from(params[1].toUByte())
                    ?: return@let null
                RemoteProvisioningScanStatus(
                    status = status,
                    scanningState = scanState,
                    scannedItemsLimit = params[2].toUByte(),
                    timeout = (params[3].toInt() and 0xFF).seconds
                )
            }
    }
}
