package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedRemoteProvisioningMessage

/**
 * A Remote Provisioning PDU Report message is an unacknowledged message used by the Remote
 * Provisioning Server to report a Provisioning PDU that was either received from the device
 * being provisioned, or generated locally during a Node Provisioning Protocol Interface
 * procedure.
 *
 * Wire: `InboundPDUNumber (1) | ProvisioningPDU (n)` — `BT_MESH_LEN_MIN(2)` in `rpr_cli.c`.
 *
 * The payload is passed through as a raw Provisioning PDU for the same layering reason as in
 * [RemoteProvisioningPDUSend]: `ProvisioningResponse` lives in the `provisioning` module, which
 * depends on `core`. `PBRemoteBearer` hands these bytes to `ProvisioningManager` unchanged.
 *
 * @property inboundPduNumber The value of the server's Remote Provisioning Inbound PDU Count
 *                            state. The NCS client discards a report whose number is not
 *                            greater than the last one seen (duplicate suppression).
 * @property provisioningPdu  The Provisioning PDU, starting with its type octet.
 */
class RemoteProvisioningPDUReport(
    val inboundPduNumber: UByte,
    val provisioningPdu: ByteArray,
) : UnacknowledgedRemoteProvisioningMessage {

    init {
        require(provisioningPdu.isNotEmpty()) { "Provisioning PDU must not be empty" }
    }

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(inboundPduNumber.toByte()) + provisioningPdu

    /** Sent by the NCS server with `send_rel = true` to guarantee delivery. */
    override val isSegmented: Boolean = true

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString() = "RemoteProvisioningPDUReport(" +
            "inboundPduNumber: $inboundPduNumber, " +
            "provisioningPdu: ${provisioningPdu.toHexString()})"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x805Fu

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size >= 2 }
            ?.let { params ->
                RemoteProvisioningPDUReport(
                    inboundPduNumber = params[0].toUByte(),
                    provisioningPdu = params.copyOfRange(1, params.size)
                )
            }
    }
}
