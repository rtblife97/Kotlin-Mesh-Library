package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.onSubscription
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.NetworkEvent
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.model.Address

/**
 * The minimal slice of [MeshNetworkManager] that the Remote Provisioning components need.
 *
 * simdo-fork (2026-08-12, 감사 권고 테스트): [MeshNetworkManager] 는 final class 이고
 * `networkEvents` 의 backing `MutableSharedFlow` 가 private 이라 테스트에서 수신 이벤트를 주입할
 * 방법이 없다. 이 프로젝트에는 mockk/mockito 도 없다. 그래서 라이브러리를 mock 프레임워크에
 * 의존시키는 대신 **가장 얇은 seam** 을 하나 두고, 프로덕션 구현은
 * [MeshNetworkManagerTransport] 가 그대로 위임한다.
 *
 * 부수 효과로 "replay 된 stale 이벤트를 거른다"는 규칙이 [PBRemoteBearer] 와
 * [RemoteProvisioningScanner] 두 곳에 복제돼 있던 것이 여기 한 곳으로 모인다.
 *
 * `internal` 이므로 공개 API 표면은 늘지 않는다.
 */
internal interface MeshMessageTransport {

    /**
     * Messages received from the mesh network.
     *
     * @param onSubscribed Invoked once the underlying subscription is live. Callers must await
     *                     this before sending the request whose reply they want to observe,
     *                     otherwise a fast reply can be missed.
     */
    fun events(onSubscribed: () -> Unit): Flow<NetworkEvent.MeshMessageReceived>

    /**
     * Sends an acknowledged Configuration-class message and awaits its response.
     *
     * @return The response, or `null` on timeout.
     */
    suspend fun send(message: AcknowledgedConfigMessage, destination: Address): MeshMessage?

    /** Sends an unacknowledged Configuration-class message. */
    suspend fun send(message: UnacknowledgedConfigMessage, destination: Address)
}

/**
 * The production [MeshMessageTransport], backed by a real [MeshNetworkManager].
 *
 * @property manager Mesh network manager to delegate to.
 */
internal class MeshNetworkManagerTransport(
    private val manager: MeshNetworkManager,
) : MeshMessageTransport {

    override fun events(onSubscribed: () -> Unit): Flow<NetworkEvent.MeshMessageReceived> {
        // `networkEvents` is a SharedFlow with replay = 1, so a brand new subscriber is handed one
        // event that predates the subscription. Identity comparison drops exactly that one without
        // risking a false positive on a genuinely repeated message.
        val stale = manager.networkEvents.replayCache.firstOrNull()
        return manager.networkEvents
            .onSubscription { onSubscribed() }
            .filterIsInstance<NetworkEvent.MeshMessageReceived>()
            .filter { it !== stale }
    }

    override suspend fun send(
        message: AcknowledgedConfigMessage,
        destination: Address,
    ): MeshMessage? = manager.send(message = message, destination = destination)

    override suspend fun send(message: UnacknowledgedConfigMessage, destination: Address) =
        manager.send(message = message, destination = destination)
}
