package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkCloseReason
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer

/**
 * A Remote Provisioning Link Close message is an acknowledged message used by the Remote
 * Provisioning Client to close the provisioning bearer between the Remote Provisioning Server
 * and an unprovisioned device, or to close the Node Provisioning Protocol Interface.
 *
 * The response to this message is a [RemoteProvisioningLinkStatus], immediately followed by an
 * unsolicited [RemoteProvisioningLinkReport] once the link has actually gone idle. The NCS
 * server disables its random transmission delay for the Link Status specifically so that the
 * two cannot be reordered (`rpr_srv.c handle_link_close`, MshPRT 1.1 §4.4.5.5.3.3).
 *
 * Wire: `Reason (1)` — `BT_MESH_LEN_EXACT(1)`. Only
 * [RemoteProvisioningLinkCloseReason.SUCCESS] and [RemoteProvisioningLinkCloseReason.FAIL] are
 * accepted; anything else is dropped without a response, so the constructor coerces
 * [RemoteProvisioningLinkCloseReason.UNRECOGNIZED] to `FAIL`.
 *
 * @property reason Provisioning bearer link close reason.
 */
class RemoteProvisioningLinkClose(
    reason: RemoteProvisioningLinkCloseReason,
) : AcknowledgedRemoteProvisioningMessage {

    val reason: RemoteProvisioningLinkCloseReason =
        if (reason == RemoteProvisioningLinkCloseReason.UNRECOGNIZED) {
            RemoteProvisioningLinkCloseReason.FAIL
        } else reason

    override val opCode = Initializer.opCode
    override val responseOpCode = RemoteProvisioningLinkStatus.opCode
    override val parameters = byteArrayOf(this.reason.value.toByte())

    override fun toString() = "RemoteProvisioningLinkClose(reason: $reason)"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x805Au

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 1 }
            ?.let { params ->
                RemoteProvisioningLinkClose(
                    reason = RemoteProvisioningLinkCloseReason.from(params[0].toUByte())
                )
            }
    }
}
