@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.onSubscription
import no.nordicsemi.kotlin.mesh.core.NetworkEvent
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress

/**
 * An in-memory [MeshMessageTransport] that lets a test play the role of a Remote Provisioning
 * Server: it records what the bearer sent and injects arbitrary "received" messages at arbitrary
 * moments.
 *
 * 이것이 감사 권고의 "fake MeshNetworkManager 하네스" 다. [no.nordicsemi.kotlin.mesh.core.MeshNetworkManager]
 * 는 final class 이고 `networkEvents` 의 backing flow 가 private 이라 그 자체로는 주입이 불가능해,
 * [MeshMessageTransport] seam 을 대신 구현한다.
 *
 * @property server Unicast address the fake server answers from.
 */
internal class FakeMeshMessageTransport(private val server: Address = 0x0002u) :
    MeshMessageTransport {

    /** Every acknowledged message the component under test sent, in order. */
    val sentAcknowledged = mutableListOf<AcknowledgedConfigMessage>()

    /** Every unacknowledged message the component under test sent, in order. */
    val sentUnacknowledged = mutableListOf<UnacknowledgedConfigMessage>()

    /**
     * Responder for acknowledged messages. Returning `null` models an acknowledged-message
     * timeout, which is what `MeshNetworkManager.send()` does when no response arrives.
     */
    var responder: suspend (AcknowledgedConfigMessage) -> MeshMessage? = { null }

    /**
     * Invoked after each unacknowledged send, so a test can inject the server's asynchronous
     * reaction (e.g. a PDU Outbound Report) at exactly that point.
     */
    var onUnacknowledgedSent: suspend (UnacknowledgedConfigMessage) -> Unit = {}

    private val events = MutableSharedFlow<NetworkEvent.MeshMessageReceived>(
        replay = 0,
        extraBufferCapacity = 64,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    /** Number of live subscribers — lets a test await the observer being wired up. */
    val subscriberCount get() = events.subscriptionCount.value

    override fun events(onSubscribed: () -> Unit): Flow<NetworkEvent.MeshMessageReceived> =
        events.onSubscription { onSubscribed() }

    override suspend fun send(
        message: AcknowledgedConfigMessage,
        destination: Address,
    ): MeshMessage? {
        sentAcknowledged.add(message)
        return responder(message)
    }

    override suspend fun send(message: UnacknowledgedConfigMessage, destination: Address) {
        sentUnacknowledged.add(message)
        onUnacknowledgedSent(message)
    }

    /** Injects a message as if it had been received from [server]. */
    suspend fun receive(message: MeshMessage, from: Address = server) {
        events.emit(
            NetworkEvent.MeshMessageReceived(
                source = from,
                destination = MeshAddress.create(address = 0x0001u) as UnicastAddress,
                message = message,
                sequence = 0u,
                ivIndex = 0u,
                ttl = 5u,
            )
        )
    }
}
