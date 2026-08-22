package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.data.getUuid
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * A Remote Provisioning Link Open message is an acknowledged message used by the Remote
 * Provisioning Client to establish a provisioning bearer between a Node supporting the Remote
 * Provisioning Server model and an unprovisioned device, or to open the Node Provisioning
 * Protocol Interface (NPPI) on that Node itself.
 *
 * The response to this message is a [RemoteProvisioningLinkStatus]; the link becoming active is
 * signalled later by an unsolicited [RemoteProvisioningLinkReport].
 *
 * Wire, one of three shapes (`BT_MESH_LEN_MIN(1)`, server accepts only 1, 16 or 17 octets):
 *  - `UUID (16)` — open a PB-ADV link to the unprovisioned device, server default timeout
 *    (10 s in NCS),
 *  - `UUID (16) | Timeout (1)` — same, with an explicit timeout of 1..60 seconds,
 *  - `NPPIProcedure (1)` — run a Node Provisioning Protocol Interface procedure on the server.
 *
 * @property uuid          Device UUID of the unprovisioned device, or `null` for an NPPI
 *                         procedure.
 * @property timeout       Link open timeout, 1..60 seconds. Only valid together with [uuid];
 *                         `null` means the server default.
 * @property nppiProcedure The NPPI procedure to run. Mandatory when [uuid] is `null`, and must
 *                         be `null` otherwise.
 */
@OptIn(ExperimentalUuidApi::class)
class RemoteProvisioningLinkOpen private constructor(
    val uuid: Uuid?,
    val timeout: Duration?,
    val nppiProcedure: NodeProvisioningProtocolInterfaceProcedure?,
) : AcknowledgedRemoteProvisioningMessage {

    /**
     * Creates a Remote Provisioning Link Open message opening a provisioning bearer to an
     * unprovisioned device.
     *
     * @param uuid    Device UUID of the unprovisioned device, as reported by a
     *                [RemoteProvisioningScanReport].
     * @param timeout Optional link open timeout, 1..60 seconds. `null` uses the server default.
     */
    constructor(uuid: Uuid, timeout: Duration? = null) : this(
        uuid = uuid,
        timeout = timeout?.also {
            require(it.inWholeSeconds in TIMEOUT_MIN_SECONDS..TIMEOUT_MAX_SECONDS) {
                "Link open timeout must be in range " +
                        "$TIMEOUT_MIN_SECONDS..$TIMEOUT_MAX_SECONDS seconds, was $it"
            }
        },
        nppiProcedure = null
    )

    /**
     * Creates a Remote Provisioning Link Open message starting a Node Provisioning Protocol
     * Interface procedure on the Remote Provisioning Server's own Node.
     *
     * @param nppiProcedure The procedure to run.
     */
    constructor(nppiProcedure: NodeProvisioningProtocolInterfaceProcedure) : this(
        uuid = null,
        timeout = null,
        nppiProcedure = nppiProcedure
    )

    override val opCode = Initializer.opCode
    override val responseOpCode = RemoteProvisioningLinkStatus.opCode
    override val parameters: ByteArray = when {
        uuid != null -> uuid.toByteArray() +
                (timeout?.let { byteArrayOf(it.inWholeSeconds.toInt().toByte()) } ?: byteArrayOf())

        nppiProcedure != null -> byteArrayOf(nppiProcedure.value.toByte())
        else -> byteArrayOf()
    }

    override fun toString() = "RemoteProvisioningLinkOpen(" +
            (uuid?.let {
                "uuid: $it" + (timeout?.let { t -> ", timeout: ${t.inWholeSeconds} s" } ?: "")
            } ?: "nppiProcedure: $nppiProcedure") + ")"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8059u

        /** Minimum link open timeout accepted by the NCS server. */
        const val TIMEOUT_MIN_SECONDS = 1L

        /** Maximum link open timeout accepted by the NCS server (`0x3c`). */
        const val TIMEOUT_MAX_SECONDS = 60L

        override fun init(parameters: ByteArray?): RemoteProvisioningLinkOpen? {
            val params = parameters ?: return null
            return when (params.size) {
                1 -> NodeProvisioningProtocolInterfaceProcedure.from(params[0].toUByte())
                    ?.let { RemoteProvisioningLinkOpen(nppiProcedure = it) }

                16 -> RemoteProvisioningLinkOpen(uuid = params.getUuid(offset = 0))

                17 -> {
                    val timeoutSeconds = (params[16].toInt() and 0xFF).toLong()
                    if (timeoutSeconds !in TIMEOUT_MIN_SECONDS..TIMEOUT_MAX_SECONDS) null
                    else RemoteProvisioningLinkOpen(
                        uuid = params.getUuid(offset = 0),
                        timeout = timeoutSeconds.seconds
                    )
                }

                else -> null
            }
        }
    }
}
