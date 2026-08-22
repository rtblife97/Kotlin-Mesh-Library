package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkCloseReason
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkState
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkStateMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningStatusMessage
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedRemoteProvisioningMessage

/**
 * A Remote Provisioning Link Report message is an unacknowledged message used by the Remote
 * Provisioning Server to report a state change of the provisioning bearer link or of the Node
 * Provisioning Protocol Interface.
 *
 * This is the message that tells the client the link has become
 * [RemoteProvisioningLinkState.LINK_ACTIVE] after a Link Open, and the message that reports an
 * **asynchronous** link teardown (device gone, timeout, subnet deleted, …) at any point during
 * provisioning.
 *
 * Wire: `Status (1) | LinkState (1) | Reason (1, optional)` — `BT_MESH_LEN_MIN(2)`, and the NCS
 * client rejects anything longer than 3 octets. The Reason field is present only when [status]
 * is [RemoteProvisioningMessageStatus.LINK_CLOSED_BY_DEVICE] or
 * [RemoteProvisioningMessageStatus.LINK_CLOSED_BY_SERVER].
 *
 * @property status    Status of the provisioning bearer or the NPPI procedure.
 * @property linkState The Remote Provisioning Link state.
 * @property reason    Link close reason, or `null` when not applicable.
 */
class RemoteProvisioningLinkReport(
    override val status: RemoteProvisioningMessageStatus,
    override val linkState: RemoteProvisioningLinkState,
    val reason: RemoteProvisioningLinkCloseReason? = null,
) : UnacknowledgedRemoteProvisioningMessage,
    RemoteProvisioningStatusMessage,
    RemoteProvisioningLinkStateMessage {

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(status.value.toByte(), linkState.value.toByte()) +
            (reason?.let { byteArrayOf(it.value.toByte()) } ?: byteArrayOf())

    /**
     * MshPRT 1.1 requires the Link Report to be delivered reliably; the NCS server sends it
     * with `send_rel = true` (`LINK_CTX(&srv.link.cli, true)`).
     */
    override val isSegmented: Boolean = true

    override fun toString() = "RemoteProvisioningLinkReport(" +
            "status: $status, " +
            "linkState: $linkState" +
            (reason?.let { ", reason: $it" } ?: "") + ")"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x805Cu

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 2 || it.size == 3 }
            ?.let { params ->
                val status = RemoteProvisioningMessageStatus.from(params[0].toUByte())
                    ?: return@let null
                val linkState = RemoteProvisioningLinkState.from(params[1].toUByte())
                    ?: return@let null
                RemoteProvisioningLinkReport(
                    status = status,
                    linkState = linkState,
                    // An unknown reason must not discard the report — the link state is the
                    // load-bearing part of this message.
                    reason = if (params.size == 3) {
                        RemoteProvisioningLinkCloseReason.from(params[2].toUByte())
                    } else null
                )
            }
    }
}
