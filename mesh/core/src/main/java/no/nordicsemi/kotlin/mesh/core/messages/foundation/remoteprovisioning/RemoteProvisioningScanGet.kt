package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer

/**
 * A Remote Provisioning Scan Get message is an acknowledged message used by the Remote
 * Provisioning Client to get the current scanning state of a Remote Provisioning Server model.
 *
 * The response to this message is a [RemoteProvisioningScanStatus]. This is the cheapest way
 * to find out whether a scan session started earlier has already expired on the server.
 *
 * Wire: no parameters (`BT_MESH_LEN_EXACT(0)`).
 */
class RemoteProvisioningScanGet : AcknowledgedRemoteProvisioningMessage {
    override val opCode = Initializer.opCode
    override val responseOpCode = RemoteProvisioningScanStatus.opCode
    override val parameters = null

    override fun toString() = "RemoteProvisioningScanGet"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8051u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.isEmpty() }
            ?.let { RemoteProvisioningScanGet() }
    }
}
