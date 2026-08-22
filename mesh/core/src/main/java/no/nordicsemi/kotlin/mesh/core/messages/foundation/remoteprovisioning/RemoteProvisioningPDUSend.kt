package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedRemoteProvisioningMessage

/**
 * A Remote Provisioning PDU Send message is an unacknowledged message used by the Remote
 * Provisioning Client to deliver a Provisioning PDU to an unprovisioned device, or to the Node
 * Provisioning Protocol Interface.
 *
 * Delivery is confirmed out of band by a [RemoteProvisioningPDUOutboundReport] carrying the
 * same [outboundPduNumber].
 *
 * Wire: `OutboundPDUNumber (1) | ProvisioningPDU (n)` — `BT_MESH_LEN_MIN(1)` in `rpr_srv.c`.
 *
 * ### simdo-fork: the payload is a raw Provisioning PDU, not a parsed type
 *
 * iOS models this field as a `ProvisioningRequest` and re-encodes it when serialising. In this
 * library `ProvisioningRequest` lives in the `provisioning` module, which **depends on** `core`
 * — modelling it here would invert the module dependency.
 *
 * Independently of layering, decoding-then-re-encoding would also be lossy:
 * `ProvisioningRequest.from()` has no branch for the Provisioning Data PDU (type `0x07`) and
 * throws `InvalidPdu` for it, so an iOS-shaped port would fail at the very last provisioning
 * step. Passing the PDU through verbatim avoids both problems.
 *
 * @property outboundPduNumber The value of the Remote Provisioning Outbound PDU Count state
 *                             after successful delivery. The first PDU of a link uses `1` and
 *                             the value increments by one per PDU; the NCS server drops any PDU
 *                             whose number is not exactly `tx_pdu + 1` (but still answers with
 *                             an Outbound Report carrying its own current count, which makes a
 *                             retry with the same number safe).
 * @property provisioningPdu   The Provisioning PDU, starting with its type octet.
 */
class RemoteProvisioningPDUSend(
    val outboundPduNumber: UByte,
    val provisioningPdu: ByteArray,
) : UnacknowledgedRemoteProvisioningMessage {

    init {
        require(provisioningPdu.isNotEmpty()) { "Provisioning PDU must not be empty" }
    }

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(outboundPduNumber.toByte()) + provisioningPdu

    /**
     * The NCS Remote Provisioning Client sends this message with `send_rel = true`
     * (`rpr_cli.c send()`), i.e. always segmented, to guarantee delivery of provisioning PDUs.
     */
    override val isSegmented: Boolean = true

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString() = "RemoteProvisioningPDUSend(" +
            "outboundPduNumber: $outboundPduNumber, " +
            "provisioningPdu: ${provisioningPdu.toHexString()})"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x805Du

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size >= 2 }
            ?.let { params ->
                RemoteProvisioningPDUSend(
                    outboundPduNumber = params[0].toUByte(),
                    provisioningPdu = params.copyOfRange(1, params.size)
                )
            }
    }
}
