package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkState
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkStateMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningResponse
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningStatusMessage

/**
 * A Remote Provisioning Link Status message is an unacknowledged message used by the Remote
 * Provisioning Server to acknowledge a [RemoteProvisioningLinkGet], [RemoteProvisioningLinkOpen]
 * or [RemoteProvisioningLinkClose] message.
 *
 * Wire: `Status (1) | LinkState (1)` — `BT_MESH_LEN_EXACT(2)` in `rpr_cli.c`.
 *
 * Note that a successful Link Open normally reports
 * [RemoteProvisioningLinkState.LINK_OPENING], **not** `LINK_ACTIVE`: the PB-ADV link to the
 * unprovisioned device is established asynchronously and its completion is signalled later by
 * an unsolicited [RemoteProvisioningLinkReport].
 *
 * @property status    Status of the requested operation.
 * @property linkState The Remote Provisioning Link state after the operation.
 */
class RemoteProvisioningLinkStatus(
    override val status: RemoteProvisioningMessageStatus,
    override val linkState: RemoteProvisioningLinkState,
) : RemoteProvisioningResponse, RemoteProvisioningStatusMessage, RemoteProvisioningLinkStateMessage {

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(status.value.toByte(), linkState.value.toByte())

    override fun toString() =
        "RemoteProvisioningLinkStatus(status: $status, linkState: $linkState)"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x805Bu

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 2 }
            ?.let { params ->
                val status = RemoteProvisioningMessageStatus.from(params[0].toUByte())
                    ?: return@let null
                val linkState = RemoteProvisioningLinkState.from(params[1].toUByte())
                    ?: return@let null
                RemoteProvisioningLinkStatus(status = status, linkState = linkState)
            }
    }
}
