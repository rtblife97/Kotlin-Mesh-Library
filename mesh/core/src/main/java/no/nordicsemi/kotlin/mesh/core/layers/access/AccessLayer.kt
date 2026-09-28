@file:Suppress("UNUSED_PARAMETER", "MemberVisibilityCanBePrivate", "unused")

package no.nordicsemi.kotlin.mesh.core.layers.access

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import no.nordicsemi.kotlin.mesh.core.ModelEvent
import no.nordicsemi.kotlin.mesh.core.ModelEventHandler
import no.nordicsemi.kotlin.mesh.core.layers.AccessKeySet
import no.nordicsemi.kotlin.mesh.core.layers.DeviceKeySet
import no.nordicsemi.kotlin.mesh.core.layers.KeySet
import no.nordicsemi.kotlin.mesh.core.layers.MessageHandle
import no.nordicsemi.kotlin.mesh.core.layers.NetworkManager
import no.nordicsemi.kotlin.mesh.core.layers.NetworkManagerEvent
import no.nordicsemi.kotlin.mesh.core.layers.ReceivedMessage
import no.nordicsemi.kotlin.mesh.core.layers.foundation.SceneClientHandler
import no.nordicsemi.kotlin.mesh.core.layers.uppertransport.UpperTransportPdu
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.ConfigAnyModelMessage
import no.nordicsemi.kotlin.mesh.core.messages.ConfigMessage
import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.messages.MeshResponse
import no.nordicsemi.kotlin.mesh.core.messages.TransactionMessage
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.UnknownMessage
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigHeartbeatPublicationSet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelPublicationSet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelPublicationVirtualAddressSet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNodeReset
import no.nordicsemi.kotlin.mesh.core.messages.generic.GenericLevelSet
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.AllNodes
import no.nordicsemi.kotlin.mesh.core.model.ApplicationKey
import no.nordicsemi.kotlin.mesh.core.model.Element
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Model
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.PrimaryGroupAddress
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.logger.LogCategory
import no.nordicsemi.kotlin.mesh.logger.Logger
import java.util.Timer
import java.util.TimerTask
import kotlin.concurrent.schedule
import kotlin.random.Random
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.Instant
import kotlin.time.toDuration

/**
 * Defines a Transaction object that is responsible for generating transaction identifiers for
 * Transaction Messages such as [GenericLevelSet].
 *
 * @property lastTid       Last transaction identifier used.
 * @property timestamp     Timestamp of the last transaction message was sent.
 * @property currentTid    Current transaction identifier.
 * @property nextTid       Next transaction identifier.
 * @property isActive      Whether the transaction can be continued.
 */
@OptIn(ExperimentalTime::class)
private data class Transaction(
    var lastTid: UByte = Random.nextInt(0, UByte.MAX_VALUE.toInt()).toUByte(),
    var timestamp: Instant = Clock.System.now(),
) {

    val currentTid: UByte
        get() {
            timestamp = Clock.System.now()
            return lastTid
        }

    val nextTid: UByte
        get() {
            lastTid = if (lastTid < UByte.MAX_VALUE) (lastTid + 1u).toUByte() else 0u
            timestamp = Clock.System.now()
            return lastTid
        }

    val isActive: Boolean
        get() = Clock.System.now() - timestamp > 6.toDuration(DurationUnit.SECONDS)
}

internal class AcknowledgementContext(
    val request: AcknowledgedMeshMessage,
    val source: Address,
    val destination: Address,
    val delay: Duration,
    val repeatBlock: () -> Unit,
    val timeout: Duration,
    val timeoutBlock: () -> Unit,
) {

    var timeoutTimer: Timer? = Timer()
    private var timeoutTask: TimerTask? = timeoutTimer
        ?.schedule(delay = timeout.inWholeMilliseconds) {
            invalidate()
            timeoutBlock()
        }

    var retryTimer: Timer? = Timer()
    private var retryTimerTask: TimerTask? = retryTimer
        ?.schedule(delay = delay.inWholeMilliseconds) {
            repeatBlock()
        }

    init {
        initializeRetryTimer(delay = delay, callback = repeatBlock)
    }

    fun invalidate() {
        timeoutTask?.cancel()
        timeoutTask = null
        timeoutTimer?.cancel()
        timeoutTimer?.purge()
        timeoutTask = null

        retryTimerTask?.cancel()
        retryTimer?.cancel()
        retryTimer?.purge()
    }

    private fun initializeRetryTimer(delay: Duration, callback: () -> Unit) {
        retryTimerTask?.cancel()
        retryTimer?.cancel()
        retryTimer?.purge()
        retryTimer = Timer()
        retryTimerTask = retryTimer?.schedule(delay = delay.inWholeMilliseconds) {
            if (retryTimer != null) {
                callback()
                initializeRetryTimer(delay = delay * 2, callback = callback)
            }
        }
    }
}

/**
 * Defines the behavior of the Access Layer of the Mesh Networking Stack.
 *
 * @property networkManager  Network manager.
 */
internal class AccessLayer(private val networkManager: NetworkManager) : AutoCloseable {
    private val network: MeshNetwork
        get() = networkManager.meshNetwork

    private val mutex = Mutex()
    private val scope = networkManager.scope
    private val logger: Logger?
        get() = networkManager.logger

    private var transactions = mutableMapOf<Int, Transaction>()
    private var reliableMessageContexts = mutableListOf<AcknowledgementContext>()
    internal val contexts: List<AcknowledgementContext>
        get() = reliableMessageContexts
    private var publishers = mutableMapOf<Model, TimerTask>()

    private fun finalize() {
        transactions.clear()
        reliableMessageContexts.forEach { it.invalidate() }
        reliableMessageContexts.clear()
        publishers.forEach {
            it.value.cancel()
        }
        publishers.clear()
    }

    override fun close() {
        finalize()
    }

    /**
     * Initialize periodic publishing from local Models.
     */
    internal fun reinitializePublishers() {
        network.localElements
            .flatMap { it.models }
            .forEach { refreshPeriodicPublisher(it) }
    }

    /**
     * This method handles the Upper Transport PDU and reads the Opcode. If the Opcode is supported,
     * a message is created and sent to the corresponding Model, otherwise a generic MeshMessage is
     * created for the app to handle.
     *
     * @param upperTransportPdu Upper Transport PDU received.
     * @param keySet            Key set used to decrypt the message.
     * @return MeshMessage if the message was handled, null otherwise.
     */
    suspend fun handle(upperTransportPdu: UpperTransportPdu, keySet: KeySet): MeshMessage? {
        val accessPdu = AccessPdu.init(pdu = upperTransportPdu) ?: return null
        var request: AcknowledgedMeshMessage? = null

        val candidates = mutex.withLock {
            reliableMessageContexts.filter {
                it.source == upperTransportPdu.destination.address &&
                        it.request.responseOpCode == accessPdu.opCode &&
                        it.destination == upperTransportPdu.source
            }
        }

        // simdo-fork (2026-09-29) — 같은 (주소, 응답 opcode) 를 기다리는 문맥 중 **파라미터까지 맞는** 것만 이 응답의 요청이다
        // ([responseMatchesRequest]). 안 맞으면(앞 요청의 늦은 응답) 어떤 문맥도 지우지 않는다 — 기다리는 요청의 재전송이 계속되고,
        // 늦은 응답은 요청 없이(= CDB 반영 없이) 전달된다. 종전: 첫 문맥을 지우고 그 요청으로 CDB 를 고쳤다.
        val context = if (upperTransportPdu.destination is UnicastAddress && candidates.isNotEmpty()) {
            val decoded = decodeForMatching(accessPdu = accessPdu, keySet = keySet)
            candidates.firstOrNull { decoded == null || responseMatchesRequest(it.request, decoded) }
                .also {
                    if (it == null) {
                        logger?.w(LogCategory.ACCESS) {
                            "$accessPdu 의 파라미터가 기다리는 요청(${candidates.size})과 다름 — 늦게 온 앞 요청의 응답으로 보고 " +
                                "요청 문맥을 유지 ($decoded)"
                        }
                    }
                }
        } else null

        if (context != null) {
            mutex.withLock {
                reliableMessageContexts.remove(context)
                request = context.request
                context.invalidate()
            }
            logger?.i(LogCategory.ACCESS) {
                "Response $accessPdu received (decrypted using key: $keySet)"
            }
        } else {
            logger?.i(LogCategory.ACCESS) {
                "$accessPdu received (decrypted using key: $keySet)"
            }
        }
        return handle(accessPdu = accessPdu, keySet = keySet, request = request)
    }

    /**
     * Sends the given Mesh Message to the given destination address. The message is encrypted
     * with the given Application Key and the network key bound to it.
     *
     * Before sending the message, the transaction identifier is updated for messages that extend
     * [TransactionMessage].
     *
     * @param message          Mesh message to be sent.
     * @param element          Local Element.
     * @param destination      Destination address.
     * @param ttl              Initial TTL value of the message. If 'null' the default Node TTL will
     *                         be used.
     * @param applicationKey   Application Key to be used to encrypt the message.
     * @param retransmit       If the message is a retransmission of a previous message.
     */
    @OptIn(ExperimentalTime::class)
    suspend fun send(
        message: MeshMessage,
        element: Element,
        destination: MeshAddress,
        ttl: UByte?,
        applicationKey: ApplicationKey,
        retransmit: Boolean,
    ): MeshMessage? {
        var msg = message
        val transactionMessage = message as? TransactionMessage

        transactionMessage?.takeIf {
            it.tid == null
        }?.let {
            val k = key(element = element, destination = destination)
            mutex.withLock {
                transactions[k] = transactions[k] ?: Transaction()

                if (retransmit || it.continueTransaction && transactions[k]!!.isActive) {
                    it.tid = transactions[k]!!.currentTid
                } else {
                    it.tid = transactions[k]!!.nextTid
                }
            }
            msg = it
        }

        logger?.i(LogCategory.MODEL) {
            "Sending $msg from: ${element.unicastAddress.toHexString()} " +
                    "to: ${destination.toHexString()}"
        }

        val pdu = AccessPdu.init(
            message = msg,
            source = element.unicastAddress.address,
            destination = destination,
            userInitiated = true
        )

        val keySet = AccessKeySet(applicationKey = applicationKey)
        logger?.i(LogCategory.ACCESS) { "Sending $pdu" }

        // Set timers for the acknowledged messages.
        // Acknowledged messages sent to a Group address won't await a Status.
        val ack = if (message is AcknowledgedMeshMessage && destination is UnicastAddress) {
            createReliableContext(pdu = pdu, element = element, initialTtl = ttl, keySet = keySet)
        } else null

        // If ack is null, the message is not acknowledged, hence return null
        return sendThenAwait(
            context = ack,
            awaitResponse = ack?.let {
                {
                    networkManager.awaitMeshMessageResponse(
                        destination = destination,
                        responseOpcode = it.request.responseOpCode,
                        timeout = it.timeout,
                        matches = { response -> responseMatchesRequest(it.request, response) },
                    )
                }
            },
            send = { networkManager.upperTransportLayer.send(accessPdu = pdu, ttl = ttl, keySet = keySet) },
        )
    }

    /**
     * simdo-fork (2026-09-28) — 응답 구독을 **송신 전에** 등록하고 송신한 뒤 기다린다.
     *
     * 종전에는 `upperTransportLayer.send` 가 끝난 뒤에 [NetworkManager.awaitMeshMessageResponse] 를 불렀다. 분할 메시지는
     * `send` 가 마지막 분할 뒤에도 분할 간격(기본 60 ms)만큼 기다렸다 반환하므로, 노드가 그 안에 응답하면(1홉·짧은 연결
     * 간격) 응답은 수신·복호되고 reliable context 까지 소비된 뒤 구독 없는 SharedFlow 로 사라졌다 → 재전송도 없이 access
     * 타임아웃 전체를 기다렸다 (실기기 2026-09-28 14:46:22 ConfigAppKeyAdd, Status 22.320 수신·송신 반환 ~22.325).
     * `UNDISPATCHED` 로 시작하면 [NetworkManager.awaitMeshMessageResponse] 가 첫 suspend 전에 SharedFlow 에 구독한다.
     */
    private suspend fun sendThenAwait(
        context: AcknowledgementContext?,
        awaitResponse: (suspend () -> ReceivedMessage?)?,
        send: suspend () -> Unit,
    ): MeshMessage? = try {
        coroutineScope {
            val response = awaitResponse?.let { async(start = CoroutineStart.UNDISPATCHED) { it() } }
            send()
            response?.await()?.message as? MeshMessage
        }
    } finally {
        // simdo-fork (2026-09-29) — 이 요청을 기다리던 쪽이 끝났다(응답·시간 초과·호출자 취소). 응답이면 [handle] 이 이미 문맥을
        // 지웠다. 아니면 여기서 지워 **재전송을 멈춘다**. 종전: 앱이 15 s 에 포기해도(호출자 withTimeout 취소) 문맥의 재전송 타이머가
        // 30 s 까지 계속 보냈고, 그 늦은 응답이 다음 요청에 붙었다(실기기 2026-09-28 17:59:26.798 — 앱 포기 0.36 s 뒤 도착).
        // lib 자체 시간 초과 경로의 cancel(handle) 은 문맥을 찾지 못해(조건이 뒤집혀 있었다 — [cancel]) 목록에 남았다.
        if (context != null) {
            withContext(NonCancellable) {
                val removed = mutex.withLock { reliableMessageContexts.remove(context) }
                if (removed) {
                    context.invalidate()
                    logger?.i(LogCategory.ACCESS) { "${context.request} 응답 대기 끝(응답 없음) — 재전송 중단" }
                }
            }
        }
    }

    /**
     * Sends the [ConfigMessage] to the given destination. The message is encrypted using the Device
     * Key which belongs to the target Node, and first Network Key known to this Node.
     *
     * @param message          Config message to be sent.
     * @param localElement     Local Element.
     * @param destination      Destination address.
     * @param initialTtl       Initial TTL value of the message. If 'null' the default Node TTL will
     *                         be used.
     * @throws IllegalArgumentException if the message is not a ConfigMessage.
     */
    @OptIn(ExperimentalStdlibApi::class)
    suspend fun send(
        message: ConfigMessage,
        localElement: Element,
        destination: Address,
        initialTtl: UByte?,
        networkKey: NetworkKey,
    ): MeshMessage? {
        val node = network.node(destination) ?: throw InvalidDestination()

        val keySet = DeviceKeySet.init(
            networkKey = networkKey, node = node
        ) ?: return null
        logger?.i(LogCategory.FOUNDATION_MODEL) {
            "Sending $message to ${
                destination.toHexString(
                    format = HexFormat {
                        upperCase = true
                        number.prefix = "0x"
                    }
                )
            }"
        }
        val pdu = AccessPdu.init(
            message = message,
            source = localElement.unicastAddress.address,
            destination = MeshAddress.create(destination),
            userInitiated = true
        )
        logger?.i(LogCategory.ACCESS) { "Sending $pdu" }

        val ack = createReliableContext(
            pdu = pdu,
            element = localElement,
            initialTtl = initialTtl,
            keySet = keySet
        )

        return sendThenAwait(
            context = ack,
            awaitResponse = {
                networkManager.awaitMeshMessageResponse(
                    destination = destination,
                    responseOpcode = ack.request.responseOpCode,
                    timeout = ack.timeout,
                    matches = { response -> responseMatchesRequest(ack.request, response) },
                )
            },
            send = { networkManager.upperTransportLayer.send(accessPdu = pdu, ttl = initialTtl, keySet = keySet) },
        )
    }

    /**
     * Replies to the received message, which was sent with the given key set, with the given
     * message.
     *
     * @param origin       Destination address of the message that the reply is for.
     * @param message      Response message to be sent.
     * @param element      Source Element.
     * @param destination  Destination address. This must be a Unicast Address.
     * @param keySet       Set of keys that the message was encrypted with.
     */
    @OptIn(ExperimentalStdlibApi::class)
    suspend fun reply(
        origin: Address,
        destination: Address,
        message: MeshMessage,
        element: Element,
        keySet: KeySet,
    ) {
        val category = if (message is ConfigMessage)
            LogCategory.FOUNDATION_MODEL
        else LogCategory.MODEL
        logger?.i(category) {
            "Replying with $message from: $element to ${
                destination.toHexString(
                    format = HexFormat {
                        upperCase = true
                        number.prefix = "0x"
                    }
                )
            }"
        }
        val dst = MeshAddress.create(address = destination)
        val pdu = AccessPdu.init(
            message = message,
            source = origin,
            destination = dst,
            userInitiated = false
        )

        // If the message is sent in response to a received message that was sent to a Unicast
        // Address, the node should transmit the response message with a random delay between 20 and
        // 50 milliseconds. If the message is sent in response to a received message that was sent
        // to a group address or a virtual address, the node should transmit the response message
        // with a random delay between 20 and 500 milliseconds. This reduces the probability of
        // multiple nodes responding to this message at exactly the same time, and therefore
        // increases the probability of message delivery rather than message collisions.
        val delay = if (dst is UnicastAddress) {
            Random.nextInt(20, 50).toDuration(DurationUnit.MILLISECONDS)
        } else {
            Random.nextInt(20, 500).toDuration(DurationUnit.MILLISECONDS)
        }

        delay(duration = delay)
        logger?.i(LogCategory.ACCESS) { "Sending $pdu" }
        networkManager.upperTransportLayer.send(accessPdu = pdu, ttl = null, keySet = keySet)
    }

    internal suspend fun cancel(handle: MessageHandle) {
        logger?.i(LogCategory.ACCESS) {
            "Cancelling messages with op code: ${handle.opCode}, " + "sent from: " +
                    "${
                        handle.source.address.toHexString(
                            format = HexFormat {
                                upperCase = true
                                number.prefix = "0x"
                            })
                    } " +
                    "to: ${
                        handle.destination.address.toHexString(
                            format = HexFormat {
                                upperCase = true
                                number.prefix = "0x"
                            })
                    }"
        }

        mutex.withLock {
            // simdo-fork (2026-09-29) — 조건 정정. 문맥의 source = 로컬(보낸 쪽), destination = 대상, 그리고 handle 은 **요청** 의
            // opCode 를 준다. 종전 조건(source==handle.destination, responseOpCode==handle.opCode)은 한 번도 맞지 않아, lib 시간
            // 초과 뒤에도 문맥이 목록에 남았다(타이머만 멈춤) → 뒤에 온 같은 opcode 응답이 그 옛 요청에 붙을 수 있었다.
            reliableMessageContexts.indexOfFirst {
                it.source == handle.source.address &&
                        it.request.opCode == handle.opCode &&
                        it.destination == handle.destination.address
            }.takeIf { it > -1 }?.let {
                reliableMessageContexts.removeAt(index = it).invalidate()
            }
        }
        networkManager.upperTransportLayer.cancel(handle)
    }

    /**
     *
     * @param accessPdu Access PDU received.
     * @param keySet    Key set used to decrypt the message.
     * @param request   Request message if the message was sent as a response to a request.
     * @return MeshMessage if the message was handled, null otherwise.
     */
    @OptIn(ExperimentalStdlibApi::class)
    private suspend fun handle(
        accessPdu: AccessPdu,
        keySet: KeySet,
        request: AcknowledgedMeshMessage?,
    ): MeshMessage? {
        val localNode = network.localProvisioner?.node ?: return null

        // The access PDU is decoded in to a Mesh Message
        var newMessage: MeshMessage? = null

        if (keySet is AccessKeySet) {
            for (element in localNode.elements) {
                val models = element.models.filter { !it.requiresDeviceKey }

                for (model in models) {
                    val eventHandler = model.eventHandler ?: continue
                    val message = eventHandler.decode(accessPdu = accessPdu) ?: continue
                    // Save and log only the first decoded message
                    if (newMessage == null) {
                        logger?.i(LogCategory.MODEL) {
                            "$message received from: ${
                                accessPdu.source.toHexString(
                                    format = HexFormat {
                                        upperCase = true
                                        number.prefix = "0x"
                                    })
                            }, to: ${
                                accessPdu.destination.address.toHexString(
                                    format = HexFormat {
                                        upperCase = true
                                        number.prefix = "0x"
                                    }
                                )
                            }"
                        }
                        newMessage = message
                    } else if (message::class != newMessage::class) {
                        // If another model's delegate decoded the same message to a different type,
                        // log this with a warning. This other type will be delivered to the
                        // delegate, but not to the global network delegate.
                        logger?.w(LogCategory.MODEL) { "$message already decoded as $newMessage" }
                    }
                    // Deliver the message to the Model if it was signed with an Application Key
                    // bound to this Model and the message is targeting this Element, or the
                    // Model is subscribed to the destination address.
                    //
                    // Note:   Messages sent to .allNodes address shall be processed only by
                    //         Models on the Primary Element. See Bluetooth Mesh Profile 1.0.1,
                    //         chapter 3.4.2.4.
                    // Note 2: As the iOS implementation does not support Relay, Proxy or Friend
                    //         Features, the messages sent to those addresses shall only be
                    //         processed if the Model is explicitly subscribed to these
                    //         addresses.

                    if ((accessPdu.destination is AllNodes && element.isPrimary) ||
                        accessPdu.destination.address == element.unicastAddress.address ||
                        model.isSubscribedTo(accessPdu.destination as PrimaryGroupAddress)
                    ) {
                        if (keySet.applicationKey.isBoundTo(model = model)) {
                            // simdo-fork (2026-06-05) — Fork-3 CDB mutation race.
                            // onMeshMessageReceived 가 handleResponses 로 CDB mutable collection 을
                            // write 하므로 cdbMutex 로 직렬화한다. reply(PDU 송신)는 lock 밖.
                            networkManager.cdbMutex.withLock {
                                eventHandler.onMeshMessageReceived(
                                    model = model,
                                    message = message,
                                    source = accessPdu.source,
                                    destination = accessPdu.destination.address,
                                    request = request
                                )
                            }?.let { response ->
                                networkManager.reply(
                                    origin = accessPdu.destination.address,
                                    destination = accessPdu.source,
                                    message = response,
                                    element = element,
                                    keySet = keySet
                                )

                                if (eventHandler is SceneClientHandler) {
                                    networkManager.emitNetworkManagerEvent(
                                        NetworkManagerEvent.OnNetworkChanged
                                    )
                                }
                            }
                        } else {
                            logger?.w(LogCategory.MODEL) {
                                "Local ${model.name} model on ${model.parentElement!!.unicastAddress} " +
                                        "not bound to ${keySet.applicationKey.name}"
                            }
                        }
                    }
                    break
                }
            }
        } else {
            // otherwise, the Device Key was used.
            val models = localNode.elements
                .flatMap { it.models }
                .filter { it.supportsDeviceKey }

            for (model in models) {
                val eventHandler = model.eventHandler ?: continue
                val message = eventHandler.decode(accessPdu = accessPdu) ?: continue
                newMessage = message
                // Is this message targeting the local Node?
                if (localNode.containsElementWithAddress(address = accessPdu.destination.address)) {
                    logger?.i(LogCategory.FOUNDATION_MODEL) {
                        "$message received from: ${
                            accessPdu.source.toHexString(
                                format = HexFormat {
                                    number.prefix = "0x"
                                    upperCase = true
                                }
                            )
                        }"
                    }
                    // simdo-fork (2026-06-05) — Fork-3 CDB mutation race.
                    // device-key 경로(Config 응답 포함)의 onMeshMessageReceived 가 handleResponses 로
                    // CDB write 하므로 cdbMutex 로 직렬화. reply(PDU 송신)는 lock 밖,
                    // 단 handle(message)(local node state mutation)는 다시 lock 안에서 수행.
                    networkManager.cdbMutex.withLock {
                        eventHandler.onMeshMessageReceived(
                            model = model,
                            message = message,
                            source = accessPdu.source,
                            destination = accessPdu.destination.address,
                            request = request
                        )
                    }?.let { response ->
                        networkManager.reply(
                            origin = accessPdu.destination.address,
                            destination = accessPdu.source,
                            message = response,
                            element = model.parentElement!!,
                            keySet = keySet
                        )
                        // Some Config Messages require special handling.
                        networkManager.cdbMutex.withLock {
                            handle(message = message)
                        }
                    }
                    // simdo-fork (2026-08-12, 감사 P1-4) — RPR RX 의 save 증폭 차단.
                    // 이 emit 은 MeshNetworkManager 에서 곧바로 save() 로 이어진다
                    // (MeshNetworkManager.observeNetworkManagerEvents: OnNetworkChanged -> save()).
                    // Device Key 경로가 원래 Config* 응답 전용이었을 때는 "상태가 바뀌었으니 저장"이
                    // 참이었지만, RPR Client 모델이 requiresDeviceKey 에 포함되어 있어
                    // Scan Report / PDU Report / Link Report 도 이 분기에 들어온다.
                    // 그것들은 CDB 를 한 글자도 바꾸지 않으므로(RemoteProvisioningClientHandler
                    // 는 순수 pass-through) 저장은 의미상으로도 틀렸고, 10초 스캔 세션 30기기 =
                    // 네트워크 전체 직렬화 30회, 기기 1대 PB-Remote 프로비저닝 ≈ 10회가 된다.
                    // 판정은 mutatesNetworkState() 한 곳에 모아 테스트로 봉인한다.
                    if (message.mutatesNetworkState()) {
                        networkManager.emitNetworkManagerEvent(event = NetworkManagerEvent.OnNetworkChanged)
                    }
                } else {
                    logger?.i(LogCategory.FOUNDATION_MODEL) {
                        "$message received from: ${
                            accessPdu.source.toHexString(
                                format = HexFormat {
                                    number.prefix = "0x"
                                    upperCase = true
                                }
                            )
                        }, to: ${
                            accessPdu.destination.address.toHexString(
                                format = HexFormat {
                                    number.prefix = "0x"
                                    upperCase = true
                                }
                            )
                        }"
                    }
                }
                break
            }
        }
        // If the message has not been decoded and handled by any of the ModelEventHandlers return
        // it to the user as an Unknown Message.
        // To add support to any new message, create a ModelEventHandler and add it to the local
        // Element.
        return newMessage ?: UnknownMessage(accessPdu = accessPdu)
    }

    /**
     * simdo-fork (2026-09-29) — 요청·응답 짝짓기용 디코드 (부수효과 없음). [handle] 의 디코드와 같은 모델 집합을 쓴다
     * (AccessKeySet 이면 DeviceKey 가 필요 없는 모델, 아니면 DeviceKey 모델). 디코드 못 하면 null — 그때는 종전처럼 첫 문맥.
     */
    private suspend fun decodeForMatching(accessPdu: AccessPdu, keySet: KeySet): MeshMessage? {
        val localNode = network.localProvisioner?.node ?: return null
        val models = localNode.elements.flatMap { it.models }.filter {
            if (keySet is AccessKeySet) !it.requiresDeviceKey else it.supportsDeviceKey
        }
        for (model in models) {
            val handler = model.eventHandler ?: continue
            return handler.decode(accessPdu = accessPdu) ?: continue
        }
        return null
    }

    /**
     * Handles selected config messages in a special way.
     *
     * @param message Config message to be handled.
     */
    private suspend fun handle(message: MeshMessage) {
        if (message is ConfigHeartbeatPublicationSet) {
            networkManager.upperTransportLayer.refreshHeartbeatPublisher()
        }

        if (message is ConfigModelPublicationSet ||
            message is ConfigModelPublicationVirtualAddressSet
        ) {
            val request = message as? ConfigAnyModelMessage
            request?.let { req ->
                network.localProvisioner?.node?.let { localNode ->
                    localNode.element(address = req.elementAddress)?.let { element ->
                        element.model(modelId = message.modelId)?.let {
                            refreshPeriodicPublisher(model = it)
                        }
                    }
                }
            }
        }
        if (message is ConfigNodeReset) {
            networkManager.emitNetworkManagerEvent(event = NetworkManagerEvent.OnNetworkReset)
        }
    }

    /**
     * Creates a key consisting of the source address and the destination address.
     *
     * @param element       Element to which the message was sent.
     * @param destination   Destination address of the message.
     * @return Key for the transaction which is an Int value.
     */
    private fun key(element: Element, destination: MeshAddress) =
        element.unicastAddress.address.toInt() shl 16 or destination.address.toInt()

    /**
     * Creates the context of an Acknowledged message.
     *
     * The context contains timers responsible for resending the message until a status is received,
     * and allows the message to be canceled.
     *
     * @param pdu           Access PDU received.
     * @param element       Element to which the message was sent.
     * @param initialTtl    Initial TTL value of the message.
     * @param keySet        Key set used to encrypt the message.
     */
    @OptIn(ExperimentalStdlibApi::class)
    private suspend fun createReliableContext(
        pdu: AccessPdu,
        element: Element,
        initialTtl: UByte?,
        keySet: KeySet,
    ): AcknowledgementContext {
        val request = pdu.message as AcknowledgedMeshMessage
        /*val request = pdu.message as? AcknowledgedMeshMessage ?: return null
        require(pdu.destination is UnicastAddress) { return null }*/

        // The ttl with which the request will be sent.
        val ttl = element.parentNode?.defaultTTL ?: networkManager.networkParameters.defaultTtl

        val initialDelay = networkManager.networkParameters.acknowledgementMessageInterval(
            ttl = ttl,
            segmentCount = pdu.segmentsCount
        )

        val timeout = networkManager.networkParameters.acknowledgementMessageTimeout

        val ack = AcknowledgementContext(
            request = request,
            source = pdu.source,
            destination = pdu.destination.address,
            delay = initialDelay,
            repeatBlock = {
                networkManager.takeIf {
                    !it.upperTransportLayer.isReceivingResponse(address = pdu.destination.address)
                }?.let {
                    scope.launch {
                        logger?.w(LogCategory.ACCESS) { "Resending $pdu" }
                        it.upperTransportLayer.send(accessPdu = pdu, ttl = ttl, keySet = keySet)
                    }
                }
            },
            timeout = timeout,
            timeoutBlock = {
                logger?.w(LogCategory.ACCESS) {
                    "Response to $pdu not received (timed out)"
                }
                val category = if (request is AcknowledgedConfigMessage)
                    LogCategory.FOUNDATION_MODEL
                else LogCategory.MODEL
                logger?.w(category) {
                    "$request sent from: ${
                        pdu.source.toHexString(
                            format = HexFormat {
                                number.prefix = "0x"
                                upperCase = true
                            }
                        )
                    } to: ${
                        pdu.destination.address.toHexString(
                            format = HexFormat {
                                number.prefix = "0x"
                                upperCase = true
                            }
                        )
                    } timed out"
                }
                scope.launch {
                    // simdo fork (2026-06-04, mode2 P0): timeout 시 해당 메시지의 reliable
                    // context 만 제거한다. 바로 위 cancel(handle) 이 source/responseOpCode/
                    // destination 매칭으로 이 한 건만 surgical removeAt+invalidate (cancel():436)
                    // 하므로 그것으로 충분하다. 종전 `reliableMessageContexts.clear()` 는 cancel
                    // 이후에 오는 **전 노드 공유 리스트 전역 쓸기** 로, 직렬에선 무해(in-flight
                    // 1건)했으나 병렬 config 시 한 노드의 timeout 이 형제 노드의 살아있는 await
                    // context 까지 파괴 → false-timeout 을 유발했다. 삭제.
                    cancel(
                        handle = MessageHandle(
                            message = request,
                            source = pdu.source,
                            destination = pdu.destination,
                            manager = networkManager
                        )
                    )
                }
            }
        )
        mutex.withLock {
            reliableMessageContexts.add(ack)
        }
        return ack
    }

    /**
     * Invalidates the current and optionally creates a new publisher that will send periodic
     * publications, when they are set up in the Model.
     *
     * @param model The Model for which the publisher is to be refreshed.
     */
    private fun refreshPeriodicPublisher(model: Model) {
        publishers[model]?.cancel()

        val publish = requireNotNull(model.publish) { return }
        require(publish.period.interval > Duration.ZERO) { return }
        val composer = model.eventHandler?.publicationMessageComposer ?: return
        publishers[model] = Timer().schedule(delay = publish.period.interval.inWholeSeconds) {
            val manager = networkManager
            scope.launch {
                manager.publish(composer(), model)
            }
        }
    }
}

/**
 * Whether receiving this message on the Device Key path can have changed persisted network state.
 *
 * simdo-fork (2026-08-12, 감사 P1-4). `AccessLayer.handle()` 의 Device Key 분기는 수신 메시지마다
 * `NetworkManagerEvent.OnNetworkChanged` 를 emit 하고 `MeshNetworkManager` 가 그것을 `save()`
 * (= 네트워크 전체 직렬화 + `Storage.save`) 로 받는다.
 *
 * Device Key 로 보호되는 모델 중 **Remote Provisioning Client 만** 상태를 바꾸지 않는 순수
 * 리포트 스트림(Scan Report / Extended Scan Report / Link Status / Link Report /
 * PDU Outbound Report / PDU Report)을 만든다. 나머지(Configuration / Private Beacon /
 * SAR Configuration / Large Composition Data / Opcodes Aggregator …)는 전부 CDB 를 갱신하므로
 * **기존 동작을 그대로 유지한다.**
 *
 * 화이트리스트가 아니라 블랙리스트(RPR 만 제외)로 쓴 이유: 새 Config 계열 메시지가 추가될 때
 * 기본값이 "저장한다"여야 안전하기 때문이다. 이 함수는
 * `AccessLayerNetworkStateEmitTest` 로 봉인돼 있다.
 *
 * @return true if the emit (and therefore the save) must happen.
 */
internal fun MeshMessage.mutatesNetworkState(): Boolean = this !is RemoteProvisioningMessage

/**
 * Attempts to decode the given AccessPdu. The Model Handler must support the opcode to specify to
 * which type should the message be decoded.
 *
 * @param accessPdu Access PDU received.
 * @return The decoded mesh message or null if the message is not supported.
 */
private fun ModelEventHandler.decode(accessPdu: AccessPdu): MeshMessage? =
    messageTypes[accessPdu.opCode]?.init(accessPdu.parameters) as MeshMessage?


/**
 * When invoked, the decoded message is processed and is passed to the proper event handler,
 * depending on its type or in case if it was a response to a previously sent request.
 *
 * @param model         Model that received the message.
 * @param message       Message that was received by the model.
 * @param source        Address of the Element from which the message was sent.
 * @param destination   Address to which the message was sent.
 * @param request       Request that was sent.
 */
private suspend fun ModelEventHandler.onMeshMessageReceived(
    model: Model,
    message: MeshMessage,
    source: Address,
    destination: Address,
    request: AcknowledgedMeshMessage?,
) = when {
    request != null -> {
        val response = message as? MeshResponse
            ?: error("$message is not MeshResponse")
        handle(
            event = ModelEvent.ResponseReceived(
                model = model,
                response = response,
                request = request,
                source = source
            )
        )
    }

    message is AcknowledgedMeshMessage -> runCatching {
        handle(
            event = ModelEvent.AcknowledgedMessageReceived(
                model = model,
                request = message,
                source = source,
                destination = MeshAddress.create(address = destination)
            )
        )
    }.getOrNull()

    message is UnacknowledgedMeshMessage -> handle(
        event = ModelEvent.UnacknowledgedMessageReceived(
            model = model,
            message = message,
            source = source,
            destination = MeshAddress.create(address = destination)
        )
    )

    else -> error("$message is neither Acknowledged nor Unacknowledged")
}