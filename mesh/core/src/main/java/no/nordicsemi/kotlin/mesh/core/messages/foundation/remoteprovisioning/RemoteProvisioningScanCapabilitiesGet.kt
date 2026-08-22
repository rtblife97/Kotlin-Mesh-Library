package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer

/**
 * A Remote Provisioning Scan Capabilities Get message is an acknowledged message used by the
 * Remote Provisioning Client to get the value of the Remote Provisioning Scan Capabilities
 * state of a Remote Provisioning Server.
 *
 * The response to this message is a [RemoteProvisioningScanCapabilitiesStatus].
 *
 * Wire: no parameters (`BT_MESH_LEN_EXACT(0)` in `rpr_srv.c`).
 */
class RemoteProvisioningScanCapabilitiesGet : AcknowledgedRemoteProvisioningMessage {
    override val opCode = Initializer.opCode
    override val responseOpCode = RemoteProvisioningScanCapabilitiesStatus.opCode
    override val parameters = null

    override fun toString() = "RemoteProvisioningScanCapabilitiesGet"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x804Fu

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.isEmpty() }
            ?.let { RemoteProvisioningScanCapabilitiesGet() }
    }
}
