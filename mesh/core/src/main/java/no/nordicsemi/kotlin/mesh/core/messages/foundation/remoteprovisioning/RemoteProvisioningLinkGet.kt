package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer

/**
 * A Remote Provisioning Link Get message is an acknowledged message used by the Remote
 * Provisioning Client to get the Remote Provisioning Link state of a Remote Provisioning
 * Server model.
 *
 * The response to this message is a [RemoteProvisioningLinkStatus].
 *
 * Wire: no parameters (`BT_MESH_LEN_EXACT(0)`).
 */
class RemoteProvisioningLinkGet : AcknowledgedRemoteProvisioningMessage {
    override val opCode = Initializer.opCode
    override val responseOpCode = RemoteProvisioningLinkStatus.opCode
    override val parameters = null

    override fun toString() = "RemoteProvisioningLinkGet"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8058u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.isEmpty() }
            ?.let { RemoteProvisioningLinkGet() }
    }
}
