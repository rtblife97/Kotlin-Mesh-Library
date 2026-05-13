package no.nordicsemi.kotlin.mesh.core

import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress

/**
 * Defines events the MeshNetwork that can be observed.
 */
sealed class NetworkEvent {

    /**
     * An event that's emitted when the network is updated. This event is emitted when the network
     * is loaded, saved or cleared.
     *
     * Any changes that may occur due to the behavior of a Mesh Network will also trigger this event.
     * For example, when a node is added to the network where [MeshNetworkManager.save] is invoked.
     */
    data object NetworkUpdated : NetworkEvent()

    /**
     * An event that's emitted when a mesh message is received from the network.
     *
     * @property source      Address of the node that sent the message.
     * @property destination Address to which the message is destined to.
     * @property message     Mesh message that was received by the node.
     * @property sequence    Sequence number (24-bit) read from the underlying Network PDU
     *                       — simdo-patch: required for cloud sync idempotency.
     * @property ivIndex     IV Index (32-bit) of the network beacon used to decrypt the PDU
     *                       — simdo-patch: identifies the IV epoch this sequence is scoped to.
     * @property ttl         TTL value from the Network PDU header at receipt time
     *                       — simdo-patch: original TTL, not the residual hop count.
     */
    data class MeshMessageReceived(
        val source: Address,
        val destination: MeshAddress,
        val message: MeshMessage,
        val sequence: UInt,
        val ivIndex: UInt,
        val ttl: UByte,
    ) : NetworkEvent()
}