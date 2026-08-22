package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.data.getUuid
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * A Remote Provisioning Scan Start message is an acknowledged message used by the Remote
 * Provisioning Client to start the Remote Provisioning Scan procedure, which finds
 * unprovisioned devices within immediate radio range of the Remote Provisioning Server.
 *
 * The response to this message is a [RemoteProvisioningScanStatus].
 *
 * Wire: `ScannedItemsLimit (1) | Timeout (1) | UUID (16, optional)` —
 * `BT_MESH_LEN_MIN(2)`, and the server rejects (`-EINVAL`, i.e. **no response at all**) any
 * trailing data that is not exactly 16 octets, or a Timeout of 0.
 *
 * ### The scan is a *session*, not a subscription
 *
 * The server scans for [timeout] seconds and reports **each device at most once per session**
 * (`BT_MESH_RPR_UNPROV_REPORTED` flag, cleared on every Scan Start). To keep a live view — and
 * to obtain a fresh RSSI sample per device — the client must restart the session repeatedly.
 * Use `RemoteProvisioningScanner` in the `provisioning` module, which does the rolling restart
 * and surfaces a session-completed signal.
 *
 * @property scannedItemsLimit Maximum number of scanned items the server should report.
 *                             `0` (default) means "no client-imposed limit", in which case the
 *                             server uses its own maximum
 *                             ([RemoteProvisioningScanCapabilitiesStatus.maxScannedItems]).
 *                             ⚠️ A value **larger** than the server maximum is rejected with
 *                             [no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus.SCANNING_CANNOT_START].
 * @property timeout           Time limit for the scan. Rounded down to whole seconds; the wire
 *                             field is a single octet, so the range is 1..255 seconds.
 * @property uuid              When present, the server performs a Single Device Scan for this
 *                             Device UUID only; when `null`, a Multiple Devices Scan.
 */
@OptIn(ExperimentalUuidApi::class)
class RemoteProvisioningScanStart(
    val scannedItemsLimit: UByte = 0u,
    val timeout: Duration,
    val uuid: Uuid? = null,
) : AcknowledgedRemoteProvisioningMessage {

    init {
        require(timeout.inWholeSeconds in 1..255) {
            "Remote Provisioning Scan timeout must be in range 1..255 seconds, was $timeout"
        }
    }

    /** The Timeout field value as encoded on the wire, in seconds. */
    val timeoutSeconds: UByte = timeout.inWholeSeconds.toInt().toUByte()

    override val opCode = Initializer.opCode
    override val responseOpCode = RemoteProvisioningScanStatus.opCode
    override val parameters = byteArrayOf(
        scannedItemsLimit.toByte(),
        timeoutSeconds.toByte()
    ) + (uuid?.toByteArray() ?: byteArrayOf())

    override fun toString() = "RemoteProvisioningScanStart(" +
            "scannedItemsLimit: $scannedItemsLimit, " +
            "timeout: $timeoutSeconds s" +
            (uuid?.let { ", uuid: $it" } ?: "") + ")"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8052u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { (it.size == 2 || it.size == 18) && it[1].toInt() != 0 }
            ?.let { params ->
                RemoteProvisioningScanStart(
                    scannedItemsLimit = params[0].toUByte(),
                    timeout = (params[1].toInt() and 0xFF).seconds,
                    uuid = if (params.size == 18) params.getUuid(offset = 2) else null
                )
            }
    }
}
