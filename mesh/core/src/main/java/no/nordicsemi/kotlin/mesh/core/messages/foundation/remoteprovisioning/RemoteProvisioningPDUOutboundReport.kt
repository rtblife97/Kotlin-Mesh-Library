package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedRemoteProvisioningMessage

/**
 * A Remote Provisioning PDU Outbound Report message is an unacknowledged message used by the
 * Remote Provisioning Server to report completion of the delivery of a Provisioning PDU that it
 * either sent to the device being provisioned, or processed locally during a Node Provisioning
 * Protocol Interface procedure.
 *
 * This is the flow-control signal of the PB-Remote bearer: the client must not send the next
 * [RemoteProvisioningPDUSend] until the previous one has been reported here.
 *
 * Wire: `OutboundPDUNumber (1)` — `BT_MESH_LEN_EXACT(1)` in `rpr_cli.c`.
 *
 * @property outboundPduNumber The current value of the server's Remote Provisioning Outbound
 *                             PDU Count state.
 */
class RemoteProvisioningPDUOutboundReport(
    val outboundPduNumber: UByte,
) : UnacknowledgedRemoteProvisioningMessage {

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(outboundPduNumber.toByte())

    /** Sent by the NCS server with `send_rel = true` to guarantee delivery. */
    override val isSegmented: Boolean = true

    override fun toString() =
        "RemoteProvisioningPDUOutboundReport(outboundPduNumber: $outboundPduNumber)"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x805Eu

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 1 }
            ?.let { params ->
                RemoteProvisioningPDUOutboundReport(outboundPduNumber = params[0].toUByte())
            }
    }
}
