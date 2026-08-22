package no.nordicsemi.kotlin.mesh.core.layers.foundation

import no.nordicsemi.kotlin.mesh.core.MessageComposer
import no.nordicsemi.kotlin.mesh.core.ModelError
import no.nordicsemi.kotlin.mesh.core.ModelEvent
import no.nordicsemi.kotlin.mesh.core.ModelEventHandler
import no.nordicsemi.kotlin.mesh.core.messages.HasInitializer
import no.nordicsemi.kotlin.mesh.core.messages.MeshResponse
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningExtendedScanReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUOutboundReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanCapabilitiesStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanStatus

/**
 * The Remote Provisioning Client model is used to provision devices into a mesh network by
 * interacting with a Node that supports the Remote Provisioning Server model.
 *
 * The Remote Provisioning Client is a root model and does not extend any other model. Access
 * layer security on this model uses the **Device Key** of the Node hosting the Remote
 * Provisioning Server, which the library applies automatically because all Remote Provisioning
 * messages extend `ConfigMessage`.
 *
 * This handler only declares which messages the model can decode; the state machines live one
 * layer up:
 *  - `PBRemoteBearer` (module `provisioning`) consumes Link Status / Link Report /
 *    PDU Outbound Report / PDU Report,
 *  - `RemoteProvisioningScanner` (module `provisioning`) consumes Scan Status /
 *    Scan Capabilities Status / Scan Report / Extended Scan Report.
 *
 * Both observe `MeshNetworkManager.networkEvents`, which is where every message decoded here is
 * surfaced.
 *
 * simdo-fork (2026-08-12): upstream shipped this class as an empty stub
 * (`messageTypes = mapOf()`), which silently downgraded every Remote Provisioning message to
 * `UnknownMessage`. The eight receive types below mirror the iOS 4.1.0 handler.
 */
class RemoteProvisioningClientHandler : ModelEventHandler() {
    override val messageTypes: Map<UInt, HasInitializer> = mapOf(
        RemoteProvisioningScanCapabilitiesStatus.opCode to RemoteProvisioningScanCapabilitiesStatus,
        RemoteProvisioningScanStatus.opCode to RemoteProvisioningScanStatus,
        RemoteProvisioningScanReport.opCode to RemoteProvisioningScanReport,
        RemoteProvisioningExtendedScanReport.opCode to RemoteProvisioningExtendedScanReport,
        RemoteProvisioningLinkStatus.opCode to RemoteProvisioningLinkStatus,
        RemoteProvisioningLinkReport.opCode to RemoteProvisioningLinkReport,
        RemoteProvisioningPDUOutboundReport.opCode to RemoteProvisioningPDUOutboundReport,
        RemoteProvisioningPDUReport.opCode to RemoteProvisioningPDUReport,
    )
    override val isSubscriptionSupported: Boolean = false
    override val publicationMessageComposer: MessageComposer? = null

    override suspend fun handle(event: ModelEvent): MeshResponse? {
        when (event) {
            is ModelEvent.AcknowledgedMessageReceived -> {
                // The Remote Provisioning Client model does not accept acknowledged messages.
                throw ModelError.InvalidMessage(msg = event.request)
            }

            is ModelEvent.ResponseReceived -> {
                // Ignore. There are no CDB fields matching these parameters; the response is
                // returned to the caller of MeshNetworkManager.send().
            }

            is ModelEvent.UnacknowledgedMessageReceived -> {
                // Ignore. Reports are surfaced through NetworkEvent.MeshMessageReceived and
                // consumed by PBRemoteBearer / RemoteProvisioningScanner.
            }
        }
        return null
    }
}
