package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer

/**
 * A Remote Provisioning Scan Stop message is an acknowledged message used by the Remote
 * Provisioning Client to terminate the Remote Provisioning Scan procedure.
 *
 * The response to this message is a [RemoteProvisioningScanStatus] reporting
 * [no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningScanState.IDLE].
 *
 * Note: before going idle, the NCS server flushes one last pending Scan Report
 * (`rpr_srv.c handle_scan_stop` calls `scan_report_send()` first), so a report may still be
 * received *after* the Scan Status.
 *
 * Wire: no parameters (`BT_MESH_LEN_EXACT(0)`).
 */
class RemoteProvisioningScanStop : AcknowledgedRemoteProvisioningMessage {
    override val opCode = Initializer.opCode
    override val responseOpCode = RemoteProvisioningScanStatus.opCode
    override val parameters = null

    override fun toString() = "RemoteProvisioningScanStop"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8053u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.isEmpty() }
            ?.let { RemoteProvisioningScanStop() }
    }
}
