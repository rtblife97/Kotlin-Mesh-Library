package no.nordicsemi.kotlin.mesh.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import no.nordicsemi.kotlin.mesh.bearer.MeshBearer
import no.nordicsemi.kotlin.mesh.bearer.Transmitter
import no.nordicsemi.kotlin.mesh.core.exception.ImportError
import no.nordicsemi.kotlin.mesh.core.exception.InvalidKeyLength
import no.nordicsemi.kotlin.mesh.core.exception.NoNetwork
import no.nordicsemi.kotlin.mesh.core.layers.NetworkManager
import no.nordicsemi.kotlin.mesh.core.layers.NetworkManagerEvent
import no.nordicsemi.kotlin.mesh.core.layers.NetworkParameters
import no.nordicsemi.kotlin.mesh.core.layers.access.CannotDelete
import no.nordicsemi.kotlin.mesh.core.layers.access.CannotRelay
import no.nordicsemi.kotlin.mesh.core.layers.access.InvalidDestination
import no.nordicsemi.kotlin.mesh.core.layers.access.InvalidElement
import no.nordicsemi.kotlin.mesh.core.layers.access.InvalidKey
import no.nordicsemi.kotlin.mesh.core.layers.access.InvalidSource
import no.nordicsemi.kotlin.mesh.core.layers.access.InvalidTtl
import no.nordicsemi.kotlin.mesh.core.layers.access.ModelNotBoundToAppKey
import no.nordicsemi.kotlin.mesh.core.layers.access.NoAppKeysBoundToModel
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNetKeyDelete
import no.nordicsemi.kotlin.mesh.core.messages.health.HealthAttentionTimer
import no.nordicsemi.kotlin.mesh.core.messages.proxy.ProxyConfigurationMessage
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.ApplicationKey
import no.nordicsemi.kotlin.mesh.core.model.Element
import no.nordicsemi.kotlin.mesh.core.model.Group
import no.nordicsemi.kotlin.mesh.core.model.Location
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Model
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.Provisioner
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.model.serialization.MeshNetworkSerializer.deserialize
import no.nordicsemi.kotlin.mesh.core.model.serialization.CdbJournal
import no.nordicsemi.kotlin.mesh.core.model.serialization.MeshNetworkSerializer
import no.nordicsemi.kotlin.mesh.core.model.serialization.MeshNetworkSerializer.serialize
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.NetworkConfiguration
import no.nordicsemi.kotlin.mesh.logger.LogCategory
import no.nordicsemi.kotlin.mesh.logger.Logger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * MeshNetworkManager is the entry point to the Mesh library.
 *
 * @param storage                     Custom storage option allowing users to save the mesh network
 *                                    to a custom location.
 * @param secureProperties            Custom storage option allowing users to save the sequence
 *                                    number.
 * @param scope                       The scope in which the mesh network will be created.
 * @property network                  Currently loaded mesh network. It will be null if the network has not
 * @property networkEvents            A shared flow of events related to the mesh network. The events
 *                                    are emitted when the network is loaded, created, updated or
 *                                    cleared.
 * @property meshBearer               Mesh bearer is responsible for sending and receiving mesh
 *                                    messages.
 * @property logger                   The logger is responsible for logging mesh messages.
 * @property networkManager           Handles the mesh networking stack.
 * @property proxyFilter              Proxy filter is responsible for filtering messages sent to the
 *                                    proxy node.
 * @property localElements            List of local elements in the mesh network. The primary
 *                                    element is always the first element in the list.
 *                                    node.
 * @property attentionTimer           Flow that notifies the client app about the health attention
 *                                    timer start and stop events.
 */
class MeshNetworkManager(
    private val storage: Storage,
    internal val secureProperties: SecurePropertiesStorage,
    internal val ioDispatcher: CoroutineDispatcher,
) : Publisher {
    // simdo-patch (2026-05-24): fire-and-forget 코루틴 (ProxyFilter.onNewProxyConnected 의
    // scope.launch, networkManagerEvent 의 save()/clear(), observeMeshMessages 등) 가 던지는
    // 예외를 스레드 기본 핸들러로 전파해 앱을 죽이지 않도록 격리.
    //
    // 배경: SupervisorJob 은 sibling 격리만 할 뿐 CoroutineExceptionHandler 를 설치하지 않는다.
    // top-level scope.launch 에서 던진 미처리 예외는 (handler 부재 시) Thread 의 default uncaught
    // handler 로 propagate → FATAL EXCEPTION. 대표 사례: proxy GATT 연결 직후 lib 자동
    // setup(provisioner) 가 SetFilterType 송신 시점에 다른 코루틴 (concurrent import/clear) 이
    // networkManager 를 null 로 바꾼 race window → MeshNetworkManager.send(ProxyConfigurationMessage)
    // 가 NoNetwork throw → 앱 크래시 (caller 가 collect 하는 proxyFilterStateFlow 와 무관한 별
    // 코루틴이므로 외부에서 잡을 수 없음).
    //
    // 본 handler 는 NoNetwork / CannotRelay 같은 transient mesh-state 예외를 로그만 남기고 삼킨다.
    // 네트워크가 다시 준비되면 (import 완료 → 다음 Secure Network Beacon) lib 가 자동 재시도한다.
    // CancellationException 은 정상 협조 취소이므로 무시. 그 외 예상치 못한 예외도 로그만 남기고
    // 삼켜 앱 안정성을 최우선으로 한다 (앱이 죽는 것보다 mesh 동작 일시 실패가 낫다).
    private val uncaughtHandler = CoroutineExceptionHandler { _, throwable ->
        if (throwable is kotlinx.coroutines.CancellationException) return@CoroutineExceptionHandler
        logger?.e(category = LogCategory.FOUNDATION_MODEL) {
            "Uncaught exception in mesh scope (swallowed to prevent crash): " +
                "${throwable::class.simpleName}: ${throwable.message}"
        }
    }
    internal val scope = CoroutineScope(context = SupervisorJob() + ioDispatcher + uncaughtHandler)
    private val mutex by lazy { Mutex() }
    var networkParameters = NetworkParameters()

    /** The currently loaded mesh network. */
    var network: MeshNetwork? = null
        private set
    private val _networkEvents = MutableSharedFlow<NetworkEvent>(replay = 1)
    val networkEvents = _networkEvents.asSharedFlow()

    internal var networkManagerEventObserver: Job? = null
    internal var observeMeshMessages: Job? = null

    internal var networkManager: NetworkManager? = null
        private set(value) {
            // simdo-fork (2026-09-28) — 교체 시 옛 스택 정리.
            //
            // 종전: 새 인스턴스를 대입만 했다. 옛 인스턴스의 bearer collector 는 살아 남아 두 스택이 같은 PDU 를
            // 처리했고(이중 수신·이중 SAR ACK·이중 Proxy Filter 설정), 관찰자(observeNetworkManagerEvents /
            // observeMeshMessages)는 "null 일 때만 구독" 이라 **옛 인스턴스** flow 에 계속 붙어 있었다(새
            // 인스턴스가 받은 메시지는 networkEvents 로 나가지 않고, 옛 스택이 대신 처리해서 가려졌다). 새
            // 인스턴스에는 bearer 도 없어서 앱이 NetworkUpdated 때 다시 물려 줄 때까지 수신 공백이 있었다.
            //
            // 순서: ① 새 인스턴스가 현재 bearer·per-dest 등록을 넘겨받아 구독을 마친다 → ② 관찰자를 새 인스턴스로
            // 옮긴다 → ③ 옛 인스턴스를 은퇴시킨다(진행 중 송신이 없으면 즉시 수신 종료, 있으면 끝날 때까지 유지).
            // ①이 ③보다 먼저라 교체 중 구독자 0 인 순간이 없다.
            // 동시 load/import 가 서로의 교체를 가로채지 않도록 교체 전체를 직렬화한다.
            synchronized(networkManagerSwapLock) {
                val previous = field
                if (previous === value) return
                field = value
                value?.adoptChannels(
                    defaultBearer = meshBearer,
                    from = previous,
                    advertisingReceivers = synchronized(advertisingReceivers) { advertisingReceivers.toList() },
                )
                val previousObservers = listOfNotNull(networkManagerEventObserver, observeMeshMessages)
                networkManagerEventObserver = null
                observeMeshMessages = null
                if (value != null) {
                    observeNetworkManagerEvents()
                    observeMeshMessages()
                }
                if (previous != null) {
                    previous.retire(observerJobs = previousObservers)
                } else {
                    previousObservers.forEach { it.cancel() }
                }
            }
        }

    private val networkManagerSwapLock = Any()

    var logger: Logger? = null

    var meshBearer: MeshBearer? = null
        set(value) {
            field = value
            networkManager?.bearer = value
        }

    /**
     * simdo-fork (2026-06-07, P6 M=2) — per-destination bearer registry 공개 진입점.
     *
     * config 병렬화(P6)에서 N 노드를 N 개 1-hop GATT 로 **동시** config 하기 위해, 노드 dst 별로
     * 그 노드 1-hop bearer 를 등록한다. [meshBearer](default)는 건드리지 않으므로 평상시 측위 RX·group
     * 제어 채널은 그대로다(미등록 dst 는 default 로 라우팅). [networkManager] 미초기화(network 미로드)
     * 시엔 no-op — 등록할 채널 자체가 없는 상태.
     *
     * @return true = 등록 수행, false = networkManager 미초기화(활성화 전).
     */
    fun registerBearer(destination: Address, bearer: MeshBearer): Boolean = registerBearer(destination, bearer, ttl = null)

    /**
     * simdo-fork (2026-10-05, 커미셔닝 동글 "얇은 무선" 1단계) — [registerBearer] + 이 목적지의 요청 TTL [ttl].
     *
     * [ttl] 이 non-null 이면 이 목적지로 TTL 을 정하지 않고(`initialTtl = null`) 보내는 메시지, 그 메시지의 재전송, 이 목적지가 보낸 분할의
     * Segment Acknowledgment 가 로컬 노드 Default TTL 대신 이 값을 쓴다. 광고 베어러로 직접 닿는 조명에 요청을 작게 flood 하려고 쓴다
     * (동글 경로 — 1홉 GATT 와 달리 요청도 모든 릴레이가 다시 보낸다). null 이면 [registerBearer] 와 같다. 등록 해제하면 사라진다.
     *
     * [bearer] 가 [no.nordicsemi.kotlin.mesh.bearer.AdvertisingBearer] 면 송신 경로만 등록한다 — 수신은 [attachAdvertisingReceiver].
     */
    fun registerBearer(destination: Address, bearer: MeshBearer, ttl: UByte?): Boolean {
        // simdo-fork (2026-10-06) — TTL 0 허용(스펙상 유효: 중계되지 않음, 수신 노드는 TTL 0 으로 응답 — Zephyr net.c:803). 동글 1홉 실험.
        require(ttl == null || ttl == 0.toUByte() || ttl in 2u..127u) { "TTL $ttl (0, 2..127)" }
        val nm = networkManager ?: return false
        nm.registerBearer(destination = destination, meshBearer = bearer, ttl = ttl)
        return true
    }

    /**
     * simdo-fork (2026-10-14, 커미셔닝 앱 1홉 TTL 0) — 베어러 등록 없이 [destination] 의 요청 TTL 만 건다 (null = 지운다). 송신은 기본 베어러
     * ([meshBearer]) 그대로. 메인 프록시 링크가 그 노드에 직접 붙어 있을 때 TTL 0 으로 보내 노드도 TTL 0 으로 응답하게(중계 없음) 하는 용도.
     * 허용 값은 [registerBearer] 와 같다. 네트워크를 다시 불러와도(load/import) 새 스택으로 옮겨진다.
     *
     * @return true = 걸었다, false = 네트워크 미로드.
     */
    fun setDestinationTtl(destination: Address, ttl: UByte?): Boolean {
        require(ttl == null || ttl == 0.toUByte() || ttl in 2u..127u) { "TTL $ttl (0, 2..127)" }
        val nm = networkManager ?: return false
        nm.setTtlOverride(destination = destination, ttl = ttl)
        return true
    }

    /**
     * simdo-fork (2026-10-14) — 지금 스택에서 진행 중인 송신 수 (응답 대기 포함). 0 이 아니면 응답을 기다리는 요청이 있다. 진단·방어용.
     */
    val inFlightMessageCount: Int
        get() = networkManager?.inFlightCount ?: 0

    /** 수신 전용 연결 목록 — 네트워크가 바뀌어(load/import) 새 스택이 생겨도 다시 붙인다. 동일성 기준. */
    private val advertisingReceivers: MutableSet<MeshBearer> =
        java.util.Collections.newSetFromMap(java.util.IdentityHashMap())

    /**
     * simdo-fork (2026-10-05) — 광고 베어러 [receiver] 의 수신을 스택에 **한 번** 붙인다 (송신 경로는 만들지 않는다 — 목적지별
     * [registerBearer] 가 정한다). 들어온 Network PDU 는 해독 직후 목적지로 거르고(로컬 노드가 받을 주소만), Secure Network Beacon 은 IV
     * 판정에만 쓴다(기본 링크 프록시 필터를 건드리지 않음). 네트워크를 아직 불러오지 않았으면 불러온 뒤 붙는다. 멱등.
     */
    fun attachAdvertisingReceiver(receiver: no.nordicsemi.kotlin.mesh.bearer.AdvertisingBearer) {
        synchronized(advertisingReceivers) { advertisingReceivers.add(receiver) }
        networkManager?.attachReceiver(receiver)
    }

    /** [attachAdvertisingReceiver] 를 되돌린다. 멱등. */
    fun detachAdvertisingReceiver(receiver: no.nordicsemi.kotlin.mesh.bearer.AdvertisingBearer) {
        synchronized(advertisingReceivers) { advertisingReceivers.remove(receiver) }
        networkManager?.detachReceiver(receiver)
    }

    /**
     * simdo-fork (2026-06-07, P6 M=2) — config 종료 후 노드 [destination] 의 1-hop bearer 등록 해제.
     * 이후 그 dst 로 가는 TX/RX 는 다시 default [meshBearer] 로 떨어진다. 멱등.
     */
    fun unregisterBearer(destination: Address) {
        networkManager?.unregisterBearer(destination = destination)
    }

    val proxyFilter: ProxyFilter = ProxyFilter(scope = scope, manager = this)

    // simdo-fork (2026-10-09) — 수신 Heartbeat 전부 (구독 일치 무관). NetworkManager 가 load/import 마다
    // 바뀌어도 이 flow 는 이 객체에 있어 구독이 끊기지 않는다. replay 없음, 느린 수집자는 오래된 것부터 버린다.
    private val _heartbeats = MutableSharedFlow<ReceivedHeartbeat>(
        extraBufferCapacity = 256,
        onBufferOverflow = kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST,
    )

    /**
     * 이 스택이 복호한 Heartbeat 메시지 (Mesh Profile §3.6.7). 목적지가 로컬 노드든 그룹(sink)이든,
     * 로컬 노드의 Heartbeat Subscription 과 일치하든 않든 모두 나온다.
     */
    val heartbeats: kotlinx.coroutines.flow.SharedFlow<ReceivedHeartbeat> = _heartbeats.asSharedFlow()

    internal fun emitHeartbeat(heartbeat: ReceivedHeartbeat) {
        _heartbeats.tryEmit(heartbeat)
    }

    private val _attentionTimer = MutableSharedFlow<HealthAttentionTimer>()
    val attentionTimer = _attentionTimer.asSharedFlow()

    var localElements: List<Element>
        get() = network?.localElements ?: emptyList()
        set(value) {
            val network = requireNotNull(network) {
                logger?.e(category = LogCategory.MODEL) {
                    "Error: Mesh Network must be created or imported before setting up local " +
                            "elements."
                }
                throw NoNetwork()
            }
            // Some models, which are supported by the library, will be added automatically.
            // Let's make sure they are not in the array.
            var elements = value.onEach {
                it.removePrimaryElementModels()
            }
            // Remove all empty elements
            elements = elements.filter { it.models.isNotEmpty() }

            // Add the required Models in the Primary Element.
            if (elements.isEmpty()) elements = elements + Element(location = Location.UNKNOWN)

            elements
                .first()
                .addPrimaryElementModels(
                    triggerAttentionTimer = { _attentionTimer.tryEmit(value = it) },
                    logger = logger
                )

            elements.forEach { element ->
                element.models.forEach { model ->
                    model.eventHandler?.let {
                        // Set the mesh network for all [ModelEventHandler]
                        it.meshNetwork = network
                        // Set the model for all [ModelEventHandler]
                        it.model = model
                        // Set the publisher for all [ModelEventHandler]
                        it.publisher = this
                    }
                }
            }

            network._localElements = elements.toMutableList()
            networkManager?.accessLayer?.reinitializePublishers()
        }

    /**
     * Re-attaches the native foundation [ModelEventHandler]s to the **local Provisioner's node**
     * (the exact collection that [no.nordicsemi.kotlin.mesh.core.layers.access.AccessLayer.handle]
     * iterates when decoding a Device-Key encrypted Access PDU, i.e.
     * `network.localProvisioner.node.elements`).
     *
     * ## Why this exists (simdo, 2026-05-26)
     *
     * [Model.eventHandler] is annotated `@Transient`, so it is **not preserved** when the network
     * is serialized/deserialized ([load]/[import]). After a load the Provisioner's node still has
     * its foundation Models (Config Server/Client, Health, SAR, Private Beacon, Remote Provisioning,
     * Scene, Firmware) in its primary Element — but every `eventHandler` is `null`.
     *
     * The [localElements] setter is the only place that attaches these handlers, and it is **not**
     * re-run automatically after a load. Worse, the setter targets `network._localElements` (a
     * separate `@Transient` field whose post-deserialization value is the default
     * `[Element(MAIN)]`), which is a **different collection** from the node's elements. Building a
     * throwaway Element and pushing it through the setter also rebuilds the node's primary Element
     * from scratch, discarding any non-foundation Models the node may legitimately hold.
     *
     * When the foundation handlers are `null`, `AccessLayer.handle` skips every Model in the
     * Device-Key branch and returns `UnknownMessage`, which causes every Configuration response
     * (e.g. Config Composition Data Status, opcode 0x02) to be rejected as an unexpected response
     * type.
     *
     * ## What this does
     *
     * Operates **in place** on the local node's primary Element: strips the library-managed
     * foundation Models ([Element.removePrimaryElementModels]) and re-inserts them with fresh
     * handlers ([Element.addPrimaryElementModels]) — exactly the same handler set the [localElements]
     * setter installs — then wires each handler's `meshNetwork` / `model` / `publisher`. Vendor and
     * Generic Models on the Element are preserved (removePrimaryElementModels only strips Health,
     * Scene Client and Device-Key Models). Finally, [localElements] is re-pointed at the node's
     * elements so the two collections converge.
     *
     * Idempotent and cheap: if the primary Element's Config Server already has a non-null handler
     * this is a no-op, so it is safe to call before each Configuration exchange.
     *
     * @return true if the local node now has foundation handlers attached, false if there is no
     *         network / local Provisioner / node.
     */
    fun rehydrateLocalNodeHandlers(): Boolean {
        val network = network ?: return false
        val node = network.localProvisioner?.node ?: return false
        val primaryElement = node.elements.firstOrNull() ?: return false

        // Fast path: handlers already attached (normal create/setup flow). No-op.
        val configServer = primaryElement.models.firstOrNull { it.isConfigurationServer }
        if (configServer?.eventHandler != null) return true

        logger?.w(category = LogCategory.MODEL) {
            "rehydrateLocalNodeHandlers: foundation handlers are null on local node " +
                    "${node.primaryUnicastAddress} — re-attaching in place " +
                    "(deserialized @Transient eventHandler loss)"
        }

        // Re-attach the library-managed foundation Models + handlers, in place, on the node's
        // primary Element. Reuses the exact same logic the localElements setter uses.
        primaryElement.removePrimaryElementModels()
        primaryElement.addPrimaryElementModels(
            triggerAttentionTimer = { _attentionTimer.tryEmit(value = it) },
            logger = logger
        )

        // Wire the handler dependencies for every Model on every Element of the node, matching the
        // localElements setter.
        node.elements.forEach { element ->
            element.models.forEach { model ->
                model.eventHandler?.let { handler ->
                    handler.meshNetwork = network
                    handler.model = model
                    handler.publisher = this
                }
            }
        }

        // Converge the two collections: point _localElements at the node's elements so that
        // AccessLayer.reinitializePublishers / SceneServer iteration / future setter calls all see
        // the handler-bearing Models.
        network._localElements = node.elements.toMutableList()
        networkManager?.accessLayer?.reinitializePublishers()
        return true
    }

    /**
     * Loads the network from the storage provided by the user.
     *
     * @return true if the configuration was successfully loaded or false otherwise.
     */
    @OptIn(ExperimentalUuidApi::class)
    suspend fun load(): Boolean {
        // simdo-fork (2026-10-31) — 저장소가 저널을 쓰면(노드 단위 증분 저장) 저널을 접어 올린다. 저널이 없거나 비면 종전 파일.
        // 망 교체 전 미뤄 둔 저장을 먼저 쓴다 (2026-10-15 합침 저장) — 읽은 뒤에 쓰면 방금 읽은 것보다 파일이 앞선다.
        flushBeforeReplacing(reason = "load")
        val journal = (storage as? JournalStorage)?.let { js ->
            js.openJournal()?.use { CdbJournal.assemble(it) }
        }
        val bytes = journal ?: storage.load().takeIf { it.isNotEmpty() } ?: return false
        network = deserialize(bytes)
            // Load the IvIndex from the secure properties' storage.
            .apply { ivIndex = secureProperties.ivIndex(uuid = uuid) }
            .also {
                // 저널로 올린 망 = 저장 파일과 같다 → 다음 저장은 증분. 종전 파일로 올렸으면 다음 저장이 저널 스냅숏을 처음 쓴다.
                if (journal != null) {
                    it.changeLog.markClean()
                    lastSnapshotBytes = bytes.size.toLong()
                    lastFullSaveAtNanos = System.nanoTime()
                }
            }
        networkManager = NetworkManager(manager = this)
        proxyFilter.onNewNetworkCreated()
        _networkEvents.emit(NetworkEvent.NetworkUpdated)
        return true
    }

    /**
     * Saves the network in the local storage provided by the user.
     */
    @OptIn(ExperimentalTime::class, ExperimentalUuidApi::class)
    suspend fun save() {
        // simdo-fork (2026-10-15) — 명시 저장 = 즉시 flush. 합쳐 미뤄 둔 저장([markNetworkDirty])이 있으면 그것까지 포함해 지금
        // 한 번 쓰고, 끝날 때까지 기다린다 (호출자는 "저장됐다" 를 전제한다: 조명 1대 설정 완료·프로비저닝 완료·import 뒤 등).
        // 종전과 같이 쓰기가 끝나면 NetworkUpdated 를 낸다.
        saveNow()
    }

    /**
     * simdo-fork (2026-10-15) — 미뤄 둔 저장이 있으면 지금 쓴다 (없으면 아무것도 안 한다). 앱 백그라운드 진입 등 "곧 죽을 수 있다"
     * 지점에서 부른다. 저장했으면 true.
     */
    suspend fun flushPendingSave(): Boolean {
        if (!hasUnsavedChanges) return false
        saveNow()
        return true
    }

    /** simdo-fork (2026-10-15) — 메모리의 망이 파일보다 앞서 있는가 (합침 저장 대기 중). */
    val hasUnsavedChanges: Boolean
        get() = synchronized(dirtyLock) { dirty }

    // ─────────────────────────────────────────────────────────────────────────────
    // simdo-fork (2026-10-15) — 수신 경로에서 영속화 분리 (합침 저장).
    //
    // 배경: 종전엔 Config Status 등 망 상태를 바꾸는 수신 메시지마다 AccessLayer 가 OnNetworkChanged 를 냈고 곧바로 save()(망 전체
    //   export + 파일 통째 쓰기)가 돌았다. 그 대기가 LowerTransportLayer.handle 의 잠금 안에 걸려 다음 수신(SegAck·분할)과 응답 전달이
    //   줄을 섰다. 조명 1대 ≈ 79회 저장, 설정 왕복 p50 = 62 ms + 0.47 ms × CDB 노드 수 (2026-10-15 현장 861건).
    // 지금: 수신 경로는 "더러움" 표시만 한다([markNetworkDirty], 기다리지 않음). 저장은 마지막 변경 뒤 [saveDebounce] 동안 조용하면
    //   1회, 계속 바뀌어도 첫 변경 뒤 [saveMaxDelay] 안에는 반드시 1회. 명시 flush 지점은 [save] / [flushPendingSave] (조명 설정 완료,
    //   프로비저닝 완료, 앱 백그라운드, 망 교체·해제 전).
    // 재생 보호: RPL(SeqAuth)·송신 SEQ·IV Index 는 이 파일이 아니라 SecurePropertiesStorage(앱 = Room)에 메시지마다 따로 쓰인다.
    //   합침 창 안에서 앱이 죽어도 잃는 것은 CDB 의 노드 설정 표시뿐이고 재생 보호는 그대로다.
    // 2026-06-05 cdbMutex: export 트래버설은 cdbMutex 안 (CME 방지) — 2026-10-26(#80)부터 노드 단위로 잡았다 놓는다
    //   (MeshNetworkSerializer.serializeWithCdbLock). 2026-08-12 RPR 증폭 차단: RPR 은 여전히 더러움 표시조차
    //   하지 않는다 (AccessLayer 의 mutatesNetworkState() 판정 그대로).

    /** 마지막 변경 뒤 이만큼 조용하면 저장한다. */
    var saveDebounce: Duration = DEFAULT_SAVE_DEBOUNCE

    /** 변경이 계속 들어와도 첫 변경 뒤 이 안에는 저장한다. */
    var saveMaxDelay: Duration = DEFAULT_SAVE_MAX_DELAY

    /**
     * simdo-fork (2026-10-31) — 노드 단위 증분 저장([JournalStorage]): 마지막 전체 스냅숏 뒤 이만큼 지나면 다음 저장은 전체 스냅숏이다.
     * 바뀐 노드 기록이 놓친 변경이 있어도 저장 파일이 이 안에서 따라잡는다 (6,550노드 폰 전체 저장 ≈ 0.7 s 추정 → 10분에 1회는 무시할 만).
     */
    var fullSaveMaxInterval: Duration = 10.minutes

    /** simdo-fork (2026-10-31) — 스냅숏 뒤 덧붙인 양이 이 크기와 마지막 스냅숏 크기 중 큰 쪽을 넘으면 다음 저장은 전체 스냅숏(저널 압축)이다. */
    var journalMinCompactionBytes: Long = 1L shl 20

    private var lastSnapshotBytes = 0L
    private var lastFullSaveAtNanos = 0L

    /** 쓴 바이트 수를 센다 (저널 스냅숏 크기 = 압축 기준). */
    private class CountingOutputStream(private val out: java.io.OutputStream) : java.io.OutputStream() {
        var count = 0L
            private set

        override fun write(b: Int) {
            out.write(b)
            count++
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            out.write(b, off, len)
            count += len
        }

        override fun flush() = out.flush()
    }

    private val dirtyLock = Any()
    private var dirty = false
    private var firstDirtyAtNanos = 0L
    private var lastDirtyAtNanos = 0L
    private var deferredSaveJob: Job? = null

    /** 저장(export + 쓰기)·해제 쓰기를 한 줄로 세운다. 늦게 끝난 옛 저장이 해제(빈 파일)를 덮어쓰지 않게. */
    private val persistLock = Mutex()

    /**
     * 망 상태가 바뀌었다 — 저장을 예약만 한다 (기다리지 않는다). 수신 경로(AccessLayer, 하위 전송 잠금 안)에서 부른다.
     */
    internal fun markNetworkDirty() {
        synchronized(dirtyLock) {
            val now = System.nanoTime()
            if (!dirty) {
                dirty = true
                firstDirtyAtNanos = now
            }
            lastDirtyAtNanos = now
            if (deferredSaveJob?.isActive != true) {
                deferredSaveJob = scope.launch { runDeferredSaves() }
            }
        }
    }

    private suspend fun runDeferredSaves() {
        while (true) {
            // 조용한 시간·상한 중 먼저 오는 쪽까지 기다린다. 기다리는 사이 명시 저장이 먼저 했으면(dirty=false) 끝.
            while (true) {
                val waitNanos = synchronized(dirtyLock) {
                    if (!dirty) {
                        deferredSaveJob = null
                        return
                    }
                    val now = System.nanoTime()
                    minOf(
                        lastDirtyAtNanos + saveDebounce.inWholeNanoseconds - now,
                        firstDirtyAtNanos + saveMaxDelay.inWholeNanoseconds - now,
                    )
                }
                if (waitNanos <= 0) break
                delay((waitNanos + 999_999) / 1_000_000)
            }
            runCatching { saveNow() }.onFailure {
                if (it is kotlinx.coroutines.CancellationException) throw it
                logger?.e(category = LogCategory.FOUNDATION_MODEL) {
                    "Deferred network save failed (will retry after ${saveDebounce}): ${it::class.simpleName}: ${it.message}"
                }
            }
            // 저장하는 동안 새 변경이 들어왔으면 다음 바퀴, 아니면 끝 (끝내기 판정은 위 잠금 안에서).
        }
    }

    /** 지금 저장한다: 더러움을 지우고 export(CDB 잠금 안) → 파일 쓰기 → NetworkUpdated. 실패하면 더러움을 되살린다. */
    private suspend fun saveNow() {
        val saved = persistLock.withLock {
            synchronized(dirtyLock) { dirty = false }
            try {
                // simdo-fork (2026-06-05) — Fork-3 CDB mutation race.
                // export()=serialize 트래버설이 CDB mutable collection 을 순회한다.
                // 병렬 config RX 가 동시에 write 중이면 CME → cdbMutex 로 트래버설을 직렬화.
                // simdo-fork (2026-10-26, #80) — cdbMutex 를 인코딩 내내 쥐지 않고 노드 단위로 잡는다
                // (MeshNetworkSerializer.serializeWithCdbLock). 종전 `withCdbLock { export() }` 는 600노드에서 저장 1회 = 수신 정지
                // 0.7~3.8 s 였다. 문자열·바이트 변환은 잠금 밖. export_ms 는 노드 사이에 수신 처리를 기다린 시간을 포함한다.
                // simdo-fork (2026-10-31) — 망 전체 JsonElement 트리 → 문자열 → 바이트 배열을 만들지 않고 저장소 스트림에 노드 하나씩 바로
                // 쓴다 (Storage.saveStreaming). 종전 저장 1회 할당 = 출력의 약 75배 (JVM 벤치 1,400노드 243 MB, 3,000노드 519 MB — 그중
                // 망 전체 트리가 한순간 39 / 83 MB 살아 있었다). 2026-10-30 Flip7 1,400노드에서 OOM. 출력 바이트는 종전과 같다.
                // export_ms 는 이제 직렬화 + 저장소 쓰기(파일 IO) 시간이다 (둘이 번갈아 돈다). 종전은 직렬화만이었다.
                val target = network ?: return@withLock false
                val t0 = System.nanoTime()
                var exportNanos = 0L
                val journal = storage as? JournalStorage
                if (journal == null) {
                    mutex.withLock {
                        storage.saveStreaming { out ->
                            MeshNetworkSerializer.writeWithCdbLock(target, out)
                            exportNanos = System.nanoTime() - t0
                        }
                    }
                    PersistenceDiagnostics.recordSave(exportNanos, full = true)
                } else {
                    // simdo-fork (2026-10-31) — 노드 단위 증분 저장. 바뀐 노드 + 껍데기만 저널에 덧붙인다. 전체 스냅숏은 (1) 저장 파일이
                    // 이 망을 대표하지 못할 때(새로 만들거나 들여온 망), (2) 저널이 스냅숏보다 커졌을 때, (3) 마지막 전체 저장 뒤
                    // [fullSaveMaxInterval] 이 지났을 때 — (3) 은 바뀐 노드 기록이 놓친 변경이 있어도 파일이 그만큼 안에서 따라잡게 하는 안전망.
                    val changes = target.changeLog.take()
                    try {
                        // 압축 기준 = 스냅숏 뒤 덧붙인 양 (저널 크기 − 스냅숏 크기) 이 스냅숏만큼 (최소 journalMinCompactionBytes).
                        val compact = changes.full ||
                            journal.journalSize() - lastSnapshotBytes >= maxOf(journalMinCompactionBytes, lastSnapshotBytes) ||
                            System.nanoTime() - lastFullSaveAtNanos >= fullSaveMaxInterval.inWholeNanoseconds
                        mutex.withLock {
                            if (compact) {
                                journal.replaceJournal { out ->
                                    val counting = CountingOutputStream(out)
                                    CdbJournal.writeSnapshot(target, counting)
                                    lastSnapshotBytes = counting.count
                                    exportNanos = System.nanoTime() - t0
                                }
                                lastFullSaveAtNanos = System.nanoTime()
                            } else {
                                journal.appendJournal(CdbJournal.encodeDelta(target, changes))
                                exportNanos = System.nanoTime() - t0
                            }
                        }
                        PersistenceDiagnostics.recordSave(exportNanos, full = compact)
                    } catch (e: Throwable) {
                        target.changeLog.restore(changes)
                        throw e
                    }
                }
                true
            } catch (e: Throwable) {
                synchronized(dirtyLock) {
                    val now = System.nanoTime()
                    if (!dirty) {
                        dirty = true
                        firstDirtyAtNanos = now
                    }
                    lastDirtyAtNanos = now
                }
                throw e
            }
        }
        // 잠금 밖에서 낸다 — 구독자가 이 이벤트를 받고 save() 를 불러도 교착하지 않게.
        if (saved) _networkEvents.emit(value = NetworkEvent.NetworkUpdated)
    }

    /** 망 교체 전: 옛 망의 미뤄 둔 저장을 먼저 쓴다 (실패는 기록만 — 교체를 막지 않는다). */
    private suspend fun flushBeforeReplacing(reason: String) {
        runCatching { flushPendingSave() }.onFailure {
            if (it is kotlinx.coroutines.CancellationException) throw it
            logger?.w(category = LogCategory.FOUNDATION_MODEL) {
                "Pending network save before $reason failed: ${it::class.simpleName}: ${it.message}"
            }
        }
    }

    /**
     * Runs [block] holding the CDB mutation lock so that in-memory CDB writes
     * (config response handling) and read traversals (export/serialize) are serialized.
     *
     * simdo-fork (2026-06-05) — Fork-3 CDB mutation race.
     *
     * 앱(sdkbridge)이 provision 직후 `network.add(node)` / `save()` 같은 CDB write/traversal 을
     * lib 의 인바운드 RX 직렬화(AccessLayer.cdbMutex)와 같은 lock 위에서 수행하도록 노출한다.
     * networkManager 가 아직 없으면 lock 없이 block 을 그대로 실행한다(네트워크 미로드=경합 불가).
     */
    suspend fun <T> withCdbLock(block: suspend () -> T): T {
        val nm = networkManager ?: return block()
        return nm.cdbMutex.withLock { block() }
    }

    /**
     * Creates a Mesh Network with a given name and a UUID. If a UUID is not provided a random will
     * be generated.
     *
     * @param name Name of the mesh network.
     * @param uuid 128-bit Universally Unique Identifier (Uuid), which allows differentiation among
     *             multiple mesh networks.
     * @throws InvalidKeyLength if the key length is invalid.
     */
    @OptIn(ExperimentalUuidApi::class)
    @Throws(InvalidKeyLength::class)
    suspend fun create(
        name: String = "Mesh Network",
        uuid: Uuid = Uuid.random(),
        provisionerName: String = "Mesh Provisioner",
        networkKeys: List<ByteArray> = emptyList(),
    ) = create(
        name = name,
        uuid = uuid,
        provisioner = Provisioner(name = provisionerName),
        networkKeys = networkKeys
    )

    /**
     * Creates a Mesh Network with a given name and a UUID. If a UUID is not provided a random will
     * be generated.
     *
     * @param name Name of the mesh network.
     * @param uuid 128-bit UUID of the mesh network.
     * @param provisioner Provisioner to be added to the network.
     * @throws InvalidKeyLength if the key length is invalid.
     */
    @OptIn(ExperimentalTime::class, ExperimentalUuidApi::class)
    @Throws(InvalidKeyLength::class)
    suspend fun create(
        name: String = "Mesh Network",
        uuid: Uuid = Uuid.random(),
        provisioner: Provisioner,
        networkKeys: List<ByteArray> = emptyList(),
    ): MeshNetwork {
        // Check if the Network Key is of valid length
        networkKeys.forEach {
            if (it.size != 16) {
                logger?.e(category = LogCategory.FOUNDATION_MODEL) {
                    "Key length must be 16 bytes"
                }
                throw InvalidKeyLength()
            }
        }

        // simdo-fork (2026-10-15) — 망 교체 전 미뤄 둔 저장 flush (합침 저장).
        flushBeforeReplacing(reason = "create")
        return MeshNetwork(uuid = uuid, _name = name).also { network ->
            if (networkKeys.isNotEmpty()) {
                networkKeys.forEachIndexed { index, key ->
                    network.add(
                        name = if (index == 0) "Primary Network Key" else "Network Key $index",
                        key = key
                    )
                }
            } else {
                network.add(name = "Primary Network Key", index = 0u)
            }
            network.add(provisioner)
            this.network = network
            networkManager = NetworkManager(this)
            // Store the IvIndex of the newly created network.
            secureProperties.storeLocalProvisioner(
                uuid = uuid,
                localProvisionerUuid = network.provisioners.first().uuid
            )
            secureProperties.storeIvIndex(
                uuid = network.uuid,
                ivIndex = network.ivIndex
            )
            _networkEvents.emit(value = NetworkEvent.NetworkUpdated)
        }
    }

    /**
     * Forgets the currently loaded mesh network and saves the state.
     */
    suspend fun clear() {
        networkManager = null
        network = null
        // simdo-fork (2026-10-15) — 해제한 망의 미뤄 둔 저장은 버린다 (곧 빈 파일로 덮는다).
        synchronized(dirtyLock) { dirty = false }
        _networkEvents.emit(value = NetworkEvent.NetworkUpdated)
        // 진행 중인 옛 저장이 끝난 뒤 빈 파일을 쓴다 (persistLock) — 늦게 끝난 옛 저장이 해제를 되살리지 않게.
        persistLock.withLock {
            mutex.withLock {
                storage.save(network = byteArrayOf())
                // simdo-fork (2026-10-31) — 저널도 비운다 (빈 저널 = 망 없음 → 다음 load 는 종전 파일(빈) 을 본다).
                (storage as? JournalStorage)?.replaceJournal { }
            }
        }
    }

    /**
     * Imports a Mesh Network from a byte array containing a JSON defined by the Mesh Configuration
     * Database profile.
     *
     * @return a mesh network configuration decoded from the given byte array.
     * @throws ImportError if deserializing fails.
     */
    @OptIn(ExperimentalUuidApi::class)
    @Throws(ImportError::class)
    suspend fun import(array: ByteArray) = runCatching {
        // simdo-fork (2026-10-15) — 망 교체 전 옛 망의 미뤄 둔 저장 flush (합침 저장). 실패해도 import 는 진행.
        flushBeforeReplacing(reason = "import")
        deserialize(array)
            // simdo-fork (2026-08-11) — IV Index 복원. `MeshNetwork.ivIndex` 는 @Transient 이고
            // CDB Profile 스키마에도 최상위 ivIndex 가 없으므로 deserialize 결과는 항상 IvIndex(0).
            // `load()` 는 :320 에서 secure properties 로 복원하는데 `import()` 는 누락되어 있었다.
            // 그 상태로 송신하면 NetworkPdu 가 IV 0 으로 암호화되고, 망 IV Index 가 2 이상이면
            // 수신 노드가 iv_index / iv_index-1 로만 복호를 시도하므로 전량 무음 폐기된다
            // (obfuscation/MIC 실패). 첫 Secure Network Beacon 수신 시 NetworkLayer 가 교정하지만
            // 그 전까지 창이 열리고, 세션 중 import(서버 CDB 변경 수신 등)에서는 이미 SNB 를 받은
            // 뒤라 다음 SNB 까지 창이 유지된다.
            //
            // 조회 키는 **import 되는 CDB 자신의 uuid** — venue/프로젝트 전환 시 이전 망과 다른
            // uuid 일 수 있으므로 반드시 수신 객체 기준으로 조회해야 한다.
            // 사이드카에 row 가 없으면(SNB 를 한 번도 못 받은 신규 망) IvIndex(0) 이 반환되며,
            // 이는 기존 동작과 동일하고 첫 SNB 로 정렬되므로 무해하다.
            .apply { ivIndex = secureProperties.ivIndex(uuid = uuid) }
            .also { network ->
                this.network = network
                networkManager = NetworkManager(this)
                proxyFilter.onNewNetworkCreated()
                _networkEvents.emit(value = NetworkEvent.NetworkUpdated)
            }
    }.getOrElse {
        if (it is ImportError) throw it
        else throw ImportError(error = "Error while deserializing the mesh network", throwable = it)
    }

    /**
     * Exports a mesh network to a JSON defined by the Mesh Configuration Database Profile based
     * on the given configuration.
     *
     * simdo-fork (2026-10-26, #80) — Full 은 노드 잠금(nodesMonitor)을 노드 단위로 잡는다 (MeshNetworkSerializer.serialize).
     * CDB 잠금(cdbMutex)은 잡지 않는다 (suspend 아님) — 설정 응답 반영과 겹치면 노드 하나가 반영 중간 상태로 나갈 수 있다. 그 경우의
     * 리스트 변경 예외는 그 노드만 다시 인코딩한다. 일관된 노드 1대가 필요하면 [exportNode].
     *
     * @param configuration Specifies if the network should be fully exported or partially.
     * @return Bytearray containing the Mesh network configuration.
     */
    fun export(configuration: NetworkConfiguration = NetworkConfiguration.Full): ByteArray? {
        val target = network ?: return null
        return when (configuration) {
            // simdo-fork (2026-10-31) — Full 은 망 전체 트리·문자열 없이 바이트로 바로 (저장과 같은 스트리밍 직렬화, 출력 동일).
            is NetworkConfiguration.Full -> java.io.ByteArrayOutputStream(64 * 1024)
                .also { MeshNetworkSerializer.writeFull(target, it) }
                .toByteArray()

            else -> serialize(network = target, configuration = configuration).toString().toByteArray()
        }
    }

    /**
     * simdo-fork (2026-10-26, #80) — 노드 1대만 CDB 노드 JSON 으로 내보낸다. 결과는 [export] 의 `nodes[]` 에서 그 노드를 꺼낸 것과
     * 같다 (같은 직렬화기, MeshNetworkSerializerNodeSliceTest).
     *
     * 종전 앱은 노드 1대 JSON 이 필요할 때(서버 1008 노드 등록, WAL 노드 upsert) 망 전체를 export 해 그 노드를 골랐다 — 600노드면
     * 1회 0.3~3.8 s 동안 잠금을 쥐었다. 여기서는 그 노드를 인코딩하는 동안만 CDB 잠금(cdbMutex)을 잡는다: 설정 응답 반영과 겹치지
     * 않는 한 순간의 노드 상태다. cdbMutex 를 이미 쥔 채 부르지 말 것 (비재진입, 교착).
     *
     * @param uuid 노드 UUID.
     * @return 노드 JSON (compact). 망이 없거나 그 노드가 없으면 null.
     */
    @OptIn(ExperimentalUuidApi::class)
    suspend fun exportNode(uuid: Uuid): String? {
        val target = network ?: return null
        return target.cdbMutex.withLock {
            target.node(uuid = uuid)?.let { node ->
                val t0 = System.nanoTime()
                MeshNetworkSerializer.encodeNode(node).also {
                    val held = System.nanoTime() - t0
                    PersistenceDiagnostics.recordSerializationLock(held, held)
                }
            }
        }?.toString()
    }

    override fun publish(message: UnacknowledgedMeshMessage, model: Model) {
        networkManager?.let {
            model.let {
                requireNotNull(it.parentElement) {
                    logger?.e(category = LogCategory.MODEL) {
                        "Model does not belong to an Element"
                    }
                    throw IllegalStateException("Model does not belong to an Element")
                }
                it.publish?.let { publish ->
                    network?.applicationKey(index = publish.index)?.let {
                        networkManager?.let { networkManager ->
                            scope.launch {
                                networkManager.publish(message = message, from = model)
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * This method checks whether the proxy node knows the given Network Key.
     *
     * @param key The Network Key to check.
     */
    internal fun ensureNetworkKeyExists(key: NetworkKey) {
        proxyFilter.proxy?.let { node ->
            if (!node.knows(key = key)) {
                logger?.w(category = LogCategory.PROXY) {
                    "${node.name} cannot relay messages using ${key.name}, messages will be sent " +
                            "only to the local Node."
                }
            }
        } ?: run {
            logger?.w(category = LogCategory.PROXY) {
                "No GATT Proxy connected, message will be sent only to the local Node."
            }
        }
    }

    /**
     * Encrypts the message with the Application Key and the Network Key bound to it, and sends to
     * the given destination address.
     *
     * The method completes when the message has been sent or an error occurred.
     *
     * @param message              Message to be sent.
     * @param localElement         Source Element. If `nil`, the primary Element will be used. The
     *                             Element must belong to the local Provisioner's Node.
     * @param destination          Destination address.
     * @param initialTtl           Initial TTL (Time To Live) value of the message. If `null`, the
     *                             default Node TTL will be used.
     * @param applicationKey       Application Key to sign the message.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidSource if the Local Provisioner has no Unicast Address assigned.
     * @throws InvalidDestination if the Address is not a Unicast Address, an Unknown destination
     *                            Node, the Node does not have a Network Key,the Node's device key
     *                            is unknown or Cannot remove last Network Key.
     * @throws InvalidElement if the element does not belong to the local node.
     * @throws InvalidTtl if the TTL value is invalid.
     */
    @Throws(
        NoNetwork::class,
        InvalidDestination::class,
        InvalidSource::class,
        InvalidElement::class,
        InvalidTtl::class
    )
    suspend fun send(
        message: MeshMessage,
        localElement: Element?,
        destination: MeshAddress,
        initialTtl: UByte?,
        applicationKey: ApplicationKey,
    ) {
        val networkManager = requireNotNull(networkManager) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }
        val network = requireNotNull(network) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }
        val localNode = requireNotNull(network.localProvisioner?.node) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Local Provisioner has no Unicast Address assigned"
            }
            throw InvalidSource()
        }
        val source = localElement ?: localNode.elements.firstOrNull() ?: run {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Local Provisioner has no Unicast Address assigned"
            }
            throw InvalidSource()
        }
        require(source.parentNode == localNode) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "The given Element does not belong to the local Node"
            }
            throw InvalidElement()
        }
        require(initialTtl == null || initialTtl <= 127u) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "TTL value $initialTtl is invalid"
            }
            throw InvalidTtl()
        }
        ensureNetworkKeyExists(key = applicationKey.boundNetworkKey)
        networkManager.send(
            message = message,
            element = source,
            destination = destination,
            initialTtl = initialTtl,
            applicationKey = applicationKey
        )
    }

    /**
     * Clears the per-destination "busy" entry that [send] registers while a message is outstanding.
     *
     * (simdo-patch) [NetworkManager.send] only removes the outgoing-message entry on a normal
     * return (via `.also { outgoingMessages.remove(...) }`, no `finally`). If the caller cancels
     * the coroutine driving an acknowledged unicast send before the library's ack-timeout block
     * returns (e.g. a bounded commissioning probe that gives up early), that entry is leaked and the
     * next [send] to the same destination throws [no.nordicsemi.kotlin.mesh.core.exception.Busy].
     *
     * This explicitly removes the entry so callers can recover without waiting for the library's
     * ack-timeout. No-op if no network is initialized or the destination was not busy.
     *
     * @param destination Destination address whose busy entry should be cleared.
     */
    suspend fun clearOutgoingMessages(destination: MeshAddress) {
        networkManager?.clearOutgoingMessages(destination = destination)
    }

    /**
     * Encrypts the message with the Application Key and a Network Key bound to it, and sends to the
     * given [Group].
     *
     * The method completes when the message has been sent or an error occurred.
     *
     * @param message        Message to be sent.
     * @param localElement   Source Element. If null, the primary Element will be used. The Element
     *                       must belong to the local Provisioner's Node.
     * @param group          Target Group.
     * @param initialTtl     Initial TTL (Time To Live) value of the message. If `null`, the default
     *                       Node TTL will be used.
     * @param key            Application Key to sign the message.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidSource if the Local Provisioner has no Unicast Address assigned.
     * @throws InvalidDestination if the Address is not Unicast Address, an Unknown destination Node,
     *                            the Node does not have a Network Key,the Node's device key is
     *                            unknown or Cannot remove last Network Key.
     * @throws InvalidElement if the element does not belong to the local node.
     * @throws InvalidTtl if the TTL value is invalid.
     */
    @Throws(
        NoNetwork::class,
        InvalidDestination::class,
        InvalidSource::class,
        InvalidElement::class,
        InvalidTtl::class
    )
    suspend fun send(
        message: MeshMessage,
        localElement: Element? = null,
        group: Group,
        initialTtl: UByte? = null,
        key: ApplicationKey,
    ) {
        send(
            message = message,
            localElement = localElement,
            destination = group.address as MeshAddress,
            initialTtl = initialTtl,
            applicationKey = key
        )
    }

    /**
     * Encrypts the message with the first Application Key bound to the given [Model] and the
     * Network Key bound to it, and sends it to the Node to which the Model belongs to.
     *
     * The method completes when the message has been sent or an error occurred.
     *
     * @param message        Message to be sent.
     * @param localElement   Source Element. If `nil`, the primary Element will be used. The
     *                       Element must belong to the local Provisioner's Node.
     * @param model          Destination Model.
     * @param initialTtl     Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                       Node TTL will be used.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidDestination if the element does not belong to a node.
     * @throws ModelNotBoundToAppKey if the model is not bound to any application key.
     */
    @Throws(
        NoNetwork::class,
        InvalidDestination::class,
        ModelNotBoundToAppKey::class,
        InvalidSource::class,
        InvalidElement::class
    )
    suspend fun send(
        message: UnacknowledgedMeshMessage,
        localElement: Element? = null,
        model: Model,
        initialTtl: UByte? = null,
        applicationKey: ApplicationKey? = null,
    ) {
        if (network == null) throw NoNetwork()

        val node = model.parentElement?.parentNode ?: run {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Element does not belong to a Node"
            }
            throw InvalidDestination()
        }
        val destination = model.parentElement?.unicastAddress ?: run {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Element does not belong to a Node"
            }
            throw InvalidDestination()
        }

        // if the Application Key is given, check if it is bound to the Model.
        if (applicationKey != null) {
            if (!applicationKey.isBoundTo(model = model)) {
                logger?.e(LogCategory.FOUNDATION_MODEL) {
                    "Model is not bound to this Application Key"
                }
                throw ModelNotBoundToAppKey()
            }
        } else {
            // If not, make sure there are ay bound Application Keys.
            if (model.boundApplicationKeys.isEmpty()) {
                logger?.e(LogCategory.FOUNDATION_MODEL) {
                    "Error: Model is not bound to any Application Key."
                }
                throw NoAppKeysBoundToModel()
            }
        }
        // Check if the Application Key is known to the Proxy Node, or the message is sent to the
        // local Node.
        // simdo-fork (2026-06-07, P6 M=2) — registry-aware 가드. per-dest 1-hop bearer 등록 시
        // 목적지 노드는 자기 자신으로 직접 도달 → 노드 자신의 netkey 면 충분.
        val reachedDirectly =
            networkManager?.hasRegisteredBearer(destination = destination.address) == true
        val selectedAppKey = applicationKey ?: model.boundApplicationKeys
            .firstOrNull { key ->
                // Unless the message is sent locally, take only keys known to the Proxy Node.
                node.isLocalProvisioner ||
                        (reachedDirectly && node.knows(key = key.boundNetworkKey)) ||
                        proxyFilter.proxy?.knows(key = key.boundNetworkKey) == true
            }
        ?: run {
            logger?.e(LogCategory.PROXY) {
                "No GATT Proxy connected or no common Network Keys"
            }
            throw CannotRelay()
        }

        send(
            message = message,
            localElement = localElement,
            destination = destination,
            initialTtl = initialTtl,
            applicationKey = selectedAppKey
        )
    }

    /**
     * Encrypts the message with the common Application Key bound to both given [Model]s and the
     * Network Key bound to it, and sends it to the Node to which the target Model belongs to.
     *
     * The method completes when the message has been sent or an error occurred.
     *
     * @param message      Message to be sent.
     * @param localModel   Source Model who's primary Element will be used.
     * @param model        Destination Model.
     * @param initialTtl   Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                     Node TTL will be used.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidDestination if the element does not belong to a node.
     * @throws ModelNotBoundToAppKey if the model is not bound to any application key.
     * @throws InvalidSource if the source model does not belong to an Element or if the element
     *                       does not belong to the local node.
     */
    @Throws(
        NoNetwork::class,
        InvalidDestination::class,
        ModelNotBoundToAppKey::class,
        InvalidSource::class,
        InvalidElement::class
    )
    suspend fun send(
        message: UnacknowledgedMeshMessage,
        localModel: Model,
        model: Model,
        initialTtl: UByte? = null,
        applicationKey: ApplicationKey? = null,
    ) {
        localModel.parentElement?.let {
            send(
                message = message,
                localElement = it,
                model = model,
                initialTtl = initialTtl,
                applicationKey = applicationKey
            )
        } ?: {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Source Model does not belong to an Element"
            }
            throw InvalidSource()
        }
    }

    /**
     * Encrypts the message with the first Application Key bound to the given [Model] and a Network
     * Key bound to it, and sends it to the Node to which the Model belongs to and returns the
     * response.
     *
     * @param message        Message to be sent.
     * @param localElement   Source Element. If `nil`, the primary Element will be used. The Element
     *                       must belong to the local Provisioner's Node.
     * @param model          Destination Model.
     * @param initialTtl     Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                       Node TTL will be used.
     * @returns A Response with the expected [AcknowledgedMeshMessage.responseOpCode] received from
     *          the target Node.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidDestination if the Model does not belong to an Element.
     * @throws ModelNotBoundToAppKey if the model is not bound to any application key.
     * @throws InvalidSource if the Local Provisioner has no Unicast Address assigned.
     * @throws InvalidElement if the element does not belong to the local node.
     * @throws InvalidTtl if the TTL value is invalid.
     */
    @Throws(
        NoNetwork::class,
        InvalidDestination::class,
        ModelNotBoundToAppKey::class,
        InvalidSource::class,
        InvalidElement::class,
        InvalidTtl::class
    )
    suspend fun send(
        message: AcknowledgedMeshMessage,
        localElement: Element? = null,
        model: Model,
        initialTtl: UByte? = null,
        applicationKey: ApplicationKey? = null,
    ): MeshMessage? {
        val networkManager = requireNotNull(networkManager) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }
        val network = requireNotNull(network) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }
        val destination = requireNotNull(model.parentElement?.unicastAddress) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Model does not belong to an Element"
            }
            throw InvalidDestination()
        }
        val node = model.parentElement?.parentNode ?: run {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Element does not belong to a Node"
            }
            throw InvalidDestination()
        }

        if (applicationKey != null && !applicationKey.isBoundTo(model = model)) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Model is not bound to this Application Key"
            }
            throw ModelNotBoundToAppKey()
        }

        if (applicationKey == null && model.boundApplicationKeys.isEmpty()) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Model is not bound to any Application Key"
            }
            throw NoAppKeysBoundToModel()
        }

        // Check if the application Key is known to the Proxy Node, or the message is sent to the
        // local Node.
        // simdo-fork (2026-06-07, P6 M=2) — registry-aware 가드. per-dest 1-hop bearer 등록 시
        // 목적지 노드는 자기 자신으로 직접 도달 → 노드 자신의 netkey 면 충분.
        val reachedDirectly = networkManager.hasRegisteredBearer(destination = destination.address)
        val selectedAppKey = applicationKey ?: model.boundApplicationKeys
            .firstOrNull { key ->
                // Unless the message is sent locally, take only keys known to the Proxy Node.
                node.isLocalProvisioner ||
                        (reachedDirectly && node.knows(key = key.boundNetworkKey)) ||
                        proxyFilter.proxy?.knows(key = key.boundNetworkKey) == true
            }
        ?: run {
            logger?.e(LogCategory.PROXY) {
                "No GATT Proxy connected or no common Network Keys"
            }
            throw CannotRelay()
        }

        val localNode = requireNotNull(network.localProvisioner?.node) {
            logger?.e(LogCategory.PROXY) {
                "Local Provisioner has no Unicast Address assigned"
            }
            throw InvalidSource()
        }
        val source = requireNotNull(localElement ?: localNode.elements.firstOrNull()) {
            logger?.e(LogCategory.PROXY) {
                "Local Provisioner has no Unicast Address assigned"
            }
            throw InvalidSource()
        }

        require(source.parentNode == localNode) {
            logger?.e(LogCategory.PROXY) {
                "The Element does not belong to the local Node"
            }
            throw InvalidElement()
        }
        require(initialTtl == null || initialTtl <= 127u) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "TTL value $initialTtl is invalid"
            }
            throw InvalidTtl()
        }
        ensureNetworkKeyExists(key = selectedAppKey.boundNetworkKey)
        return networkManager.send(
            message = message,
            element = source,
            destination = destination,
            initialTtl = initialTtl, // ?: 1u, uncomment to emulate sending messages with TTL = 1 to local node
            applicationKey = selectedAppKey
        )
    }

    /**
     * Encrypts the message with the common Application Key bound to both given [Model]s and a
     * Network Key bound to it, and sends it to the Node to which the target Model belongs to.
     *
     * @param message         Message to be sent.
     * @param localModel      Source Model who's primary element will be used.
     * @param model           Destination Model.
     * @param initialTtl      Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                        Node TTL will be used.
     * @returns A response with the expected [AcknowledgedMeshMessage.responseOpCode] received from
     *          the target Node.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidDestination if the Model does not belong to an Element.
     * @throws ModelNotBoundToAppKey if the model is not bound to any application key.
     * @throws InvalidSource if the source model does not belong to an Element.
     * @throws InvalidElement if the element does not belong to the local node.
     * @throws InvalidTtl if the TTL value is invalid.
     */
    @Throws(
        NoNetwork::class,
        InvalidDestination::class,
        ModelNotBoundToAppKey::class,
        InvalidSource::class,
        InvalidElement::class,
        InvalidTtl::class
    )
    suspend fun send(
        message: AcknowledgedMeshMessage,
        localModel: Model,
        model: Model,
        initialTtl: UByte?,
    ): MeshMessage = localModel.parentElement
        ?.let {
            send(message = message, localElement = it, model = model, initialTtl = initialTtl)
        } ?: run {
        logger?.e(LogCategory.FOUNDATION_MODEL) {
            "Source Model does not belong to an Element"
        }
        throw InvalidSource()
    }

    /**
     * Sends a Configuration Message to the Node with given destination address and returns the
     * received response.
     *
     * The `destination` must be a Unicast Address, otherwise the method throws an
     * [InvalidDestination] error.
     *
     * @param message                Message to be sent.
     * @param destination            Destination Unicast Address.
     * @param initialTtl             Initial TTL (Time To Live) value of the message. If `nil`,
     *                               the default Node TTL will be used.
     * @throws NoNetwork             If the mesh network has not been created.
     * @throws InvalidSource         Local Node does not have configuration capabilities (no Unicast
     *                               Address assigned).
     * @throws InvalidDestination    Destination address is not a Unicast Address, or it belongs to
     *                               an unknown Node.
     * @throws CannotDelete          When trying to delete the last Network Key on the device.
     * @returns Message handle that can be used to cancel sending.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidSource if the Local Provisioner has no Unicast Address assigned.
     * @throws InvalidDestination if the Address is not Unicast Address, an Unknown destination
     *                            Node, the Node does not have a Network Key,the Node's device key
     *                            is unknown or Cannot remove last Network Key.
     * @throws InvalidTtl if the TTL value is invalid.
     */
    @OptIn(ExperimentalStdlibApi::class)
    @Throws(NoNetwork::class, InvalidSource::class, InvalidDestination::class, CannotDelete::class)
    suspend fun send(
        message: UnacknowledgedConfigMessage,
        destination: Address,
        initialTtl: UByte? = null,
        networkKey: NetworkKey? = null,
    ) {
        val networkManager = requireNotNull(networkManager) {
            logger?.e(LogCategory.PROXY) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }
        val network = requireNotNull(network) {
            logger?.e(LogCategory.PROXY) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }
        val element = requireNotNull(network.localProvisioner?.node?.primaryElement) {
            logger?.e(LogCategory.PROXY) {
                "Local Provisioner has no Unicast Address assigned"
            }
            throw InvalidSource()
        }
        val dst = MeshAddress.create(address = destination)
        require(dst is UnicastAddress) {
            logger?.e(LogCategory.PROXY) {
                "Address ${destination.toHexString()} is not a Unicast Address"
            }
            throw InvalidDestination()
        }
        val node = requireNotNull(value = network.node(address = dst)) {
            logger?.e(LogCategory.PROXY) {
                "Unknown destination Node"
            }
            throw InvalidDestination()
        }
        require(node.netKeys.isNotEmpty()) {
            logger?.e(LogCategory.PROXY) {
                "The target Node does not have a Network Key"
            }
            throw InvalidDestination()
        }

        if (networkKey != null && !node.knows(key = networkKey)) {
            logger?.e(LogCategory.PROXY) {
                "Node does not know the given Network Key"
            }
            throw InvalidKey()
        }

        // Check if the application Key is known to the Proxy Node, or the message is sent to the
        // local Node.
        // simdo-fork (2026-06-07, P6 M=2) — registry-aware 가드. per-dest 1-hop bearer 등록 시
        // 목적지 노드는 자기 자신으로 직접 도달 → 노드 자신의 netkey 면 충분. 자세한 설명은
        // AcknowledgedConfigMessage send 참조.
        val reachedDirectly = networkManager.hasRegisteredBearer(destination = destination)
        val selectedNetKey = networkKey ?: node.networkKeys
            .firstOrNull {
                // Unless the message is sent locally, take only keys known to the Proxy Node.
                node.isLocalProvisioner ||
                        (reachedDirectly && node.knows(key = it)) ||
                        proxyFilter.proxy?.knows(it) == true
            }
        ?: run {
            logger?.e(LogCategory.PROXY) {
                "No GATT Proxy connected or no common Network Keys"
            }
            throw CannotRelay()
        }

        requireNotNull(node.deviceKey) {
            logger?.e(LogCategory.PROXY) {
                "Node's device key is unknown"
            }
            throw InvalidDestination()
        }
        require(initialTtl == null || initialTtl <= 127u) {
            logger?.e(LogCategory.PROXY) {
                "TTL value $initialTtl is invalid"
            }
            throw InvalidTtl()
        }
        ensureNetworkKeyExists(key = selectedNetKey)
        networkManager.send(
            configMessage = message,
            element = element,
            destination = destination,
            initialTtl = initialTtl,
            networkKey = selectedNetKey
        )
    }

    /**
     * Sends a Configuration Message to the primary Element on the given [Node].
     *
     * @param message                Message to be sent.
     * @param node                   Destination Node.
     * @param initialTtl             Initial TTL (Time To Live) value of the message. If `nil`, the
     *                               default Node TTL will be used.
     * @throws CannotDelete when trying to delete the last Network Key on the device.
     * @returns Message handle that can be used to cancel sending.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidSource if the Local Provisioner has no Unicast Address assigned.
     * @throws InvalidDestination if the Address is not Unicast Address, an Unknown destination
     *                            Node, the Node does not have a Network Key,the Node's device key
     *                            is unknown or Cannot remove last Network Key.
     * @throws InvalidTtl if the TTL value is invalid.
     */
    @Throws(
        NoNetwork::class,
        InvalidDestination::class,
        InvalidSource::class,
        InvalidTtl::class
    )
    suspend fun send(message: UnacknowledgedConfigMessage, node: Node, initialTtl: UByte?) = send(
        message = message,
        destination = node.primaryUnicastAddress.address,
        initialTtl = initialTtl
    )

    /**
     * Sends Configuration Message to the Node with given destination Address.
     *
     * The [destination] must be a [UnicastAddress], otherwise the method throws an
     * [InvalidDestination] error.
     *
     * @param message         Message to be sent.
     * @param destination     Destination Unicast Address.
     * @param initialTtl      Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                        Node TTL will be used.
     * @throws CannotDelete   when trying to delete the last Network Key on the device.
     * @returns Response associated with the message.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidSource if the Local Provisioner has no Unicast Address assigned.
     * @throws InvalidDestination if the Address is not Unicast Address, an Unknown destination
     *                            Node, the Node does not have a Network Key,the Node's device key
     *                            is unknown or Cannot remove last Network Key.
     * @throws InvalidTtl if the TTL value is invalid.
     */
    @OptIn(ExperimentalStdlibApi::class)
    @Throws(
        NoNetwork::class,
        InvalidDestination::class,
        InvalidSource::class,
        InvalidTtl::class
    )
    suspend fun send(
        message: AcknowledgedConfigMessage,
        destination: Address,
        initialTtl: UByte? = null,
        networkKey: NetworkKey? = null,
    ): MeshMessage? {
        val networkManager = requireNotNull(networkManager) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }
        val network = requireNotNull(network) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }
        val element = requireNotNull(network.localProvisioner?.node?.primaryElement) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Local Provisioner has no Unicast Address assigned"
            }
            throw InvalidSource()
        }
        val dst = MeshAddress.create(address = destination)
        require(dst is UnicastAddress) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "${
                    destination.toHexString(
                        format = HexFormat {
                            number {
                                prefix = "0x"
                                minLength = 4
                                upperCase = true
                            }
                        }
                    )
                } is not a Unicast Address"
            }
            throw InvalidDestination()
        }
        val node = requireNotNull(value = network.node(address = dst)) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Unknown destination Node ${
                    destination.toHexString(
                        format = HexFormat {
                            number {
                                prefix = "0x"
                                minLength = 4
                                upperCase = true
                            }
                        }
                    )
                }"
            }
            throw InvalidDestination()
        }
        require(node.netKeys.isNotEmpty()) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "The target Node does not have a Network Key"
            }
            throw InvalidDestination()
        }

        if (networkKey != null && !node.knows(key = networkKey)) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Node does not know the given Network Key"
            }
            throw InvalidKey()
        }

        // simdo-fork (2026-06-07, P6 M=2) — 이 dst 로 per-dest 1-hop bearer 가 등록돼 있으면 목적지
        // 노드는 default proxy(proxyFilter.proxy) 가 아니라 **자기 자신**(직접 1-hop GATT)으로 도달한다.
        // 따라서 send-전 proxy-key 가드는 default proxy 의 키 인지가 아니라 **목적지 노드 자신**이 그 키를
        // 아는지(node.knows)를 봐야 한다. node.networkKeys 를 순회하므로 노드는 정의상 그 키를 알지만,
        // 명시적으로 node.knows 로 판정해 의도를 분명히 한다. (registry-aware 가드)
        val reachedDirectly = networkManager.hasRegisteredBearer(destination = destination)
        val selectedNetworkKey = networkKey ?: node.networkKeys
            .firstOrNull { key ->
                // A key that is being deleted cannot be used to send a message.
                (message as? ConfigNetKeyDelete)?.networkKeyIndex != key.index &&
                        // 1-hop 직접 도달(registry) 이면 목적지 노드 자신의 키이면 충분.
                        // 그 외엔 default proxy 가 아는 키만(원본 동작 보존).
                        (node.isLocalProvisioner ||
                                (reachedDirectly && node.knows(key = key)) ||
                                proxyFilter.proxy?.knows(key = key) == true)
            }
        ?: run {
            if (message as? ConfigNetKeyDelete != null) {
                logger?.e(LogCategory.FOUNDATION_MODEL) {
                    "Cannot delete the last Network Key or a key used to secure the message"
                }
                throw CannotDelete()
            }
            logger?.e(LogCategory.PROXY) {
                "No GATT Proxy connected or no common Network Keys"
            }
            throw CannotRelay()
        }
        requireNotNull(node.deviceKey) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Node's device key is unknown"
            }
            throw InvalidDestination()
        }
        if (message is ConfigNetKeyDelete) {
            require(node.netKeys.size > 1) {
                logger?.e(LogCategory.FOUNDATION_MODEL) {
                    "Cannot remove last Network Key"
                }
                throw InvalidDestination()
            }
        }
        require(initialTtl == null || initialTtl <= 127u) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "TTL value $initialTtl is invalid"
            }
            throw InvalidTtl()
        }
        return networkManager.send(
            configMessage = message,
            element = element,
            destination = destination,
            initialTtl = initialTtl,
            networkKey = selectedNetworkKey
        )
    }

    /**
     * Sends a Configuration Message to the primary Element on the given [Node] and returns the
     * received response.
     *
     * @param message     Message to be sent.
     * @param node        Destination Node.
     * @param initialTtl  Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                    Node TTL will be used.
     * @throws CannotDelete when trying to delete the last Network Key on the device.
     * @returns Response associated with the message.
     * @throws NoNetwork if the mesh network has not been created.
     * @throws InvalidSource if the Local Provisioner has no Unicast Address assigned.
     * @throws InvalidDestination if the Address is not Unicast Address, an Unknown destination
     *                            Node, the Node does not have a Network Key,the Node's device key
     *                            is unknown or Cannot remove last Network Key.
     * @throws InvalidTtl if the TTL value is invalid.
     */
    suspend fun send(message: AcknowledgedConfigMessage, node: Node, initialTtl: UByte?) = send(
        message = message,
        destination = node.primaryUnicastAddress.address,
        initialTtl = initialTtl
    )

    /**
     * Sends the Configuration Message to the primary Element of the local [Node] and returns the
     * received response.
     *
     * @param message The acknowledged configuration message to be sent.
     * @return The response associated with the message.
     * @throws NoNetwork when the mesh network has not been created
     * @throws InvalidSource when the local Node does not have configuration capabilities.
     */
    @Throws(NoNetwork::class, InvalidSource::class)
    suspend fun sendToLocalNode(message: AcknowledgedConfigMessage): MeshMessage? {
        val network = requireNotNull(network) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Mesh Network not created"
            }
            throw NoNetwork()
        }

        val destination = requireNotNull(network.localProvisioner?.node?.primaryUnicastAddress) {
            logger?.e(LogCategory.FOUNDATION_MODEL) {
                "Local Provisioner has no Unicast Address assigned"
            }
            throw InvalidSource()
        }
        return send(message = message, destination = destination.address, initialTtl = 1u)
    }

    /**
     * Sends the Proxy Configuration Message to the connected Proxy Node.
     *
     * This method will only work if the bearer uses is GATT Proxy. The message will be encrypted
     * and sent to the [Transmitter], which should deliver the PDU to the connected Node.
     *
     * @param message     Proxy Configuration message to be sent.
     * @throws NoNetwork When there is no mesh network created.
     * @throws IllegalStateException This method throws when the mesh network has not been created.
     */
    @Throws(NoNetwork::class, IllegalStateException::class)
    suspend fun send(message: ProxyConfigurationMessage): ProxyConfigurationMessage =
        networkManager?.send(message) ?: run {
            logger?.e(category = LogCategory.PROXY) { "Mesh Network not created" }
            throw NoNetwork()
        }

    /**
     * Observes network manager events.
     */
    @OptIn(ExperimentalUuidApi::class)
    private fun observeNetworkManagerEvents() {
        if (networkManagerEventObserver == null) {
            runCatching {
                networkManagerEventObserver = networkManager?.networkManagerEventFlow
                    ?.onEach {
                        when (it) {
                            // simdo-fork (2026-10-15) — NetworkManager 가 이 흐름 대신 markNetworkDirty() 를 직접 부른다. 남은 경로도 합침 저장으로.
                            NetworkManagerEvent.OnNetworkChanged -> markNetworkDirty()
                            NetworkManagerEvent.OnNetworkReset ->
                                network?.localProvisioner?.let { provisioner ->
                                    clear()
                                    // val localElements = this@MeshNetworkManager.localElements
                                    // provisioner.network = null
                                    // create(provisioner = provisioner)
                                    // this@MeshNetworkManager.localElements = localElements
                                }
                        }
                    }?.let { flow ->
                        // simdo-fork (2026-09-28) — 구독 등록 후 반환(교체 직후 첫 메시지 유실 방지).
                        scope.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) { flow.collect() }
                    }
            }.onFailure {
                logger?.w(category = LogCategory.FOUNDATION_MODEL) {
                    "Error while observing network manager events: ${it.message}"
                }
            }
        }
    }

    /**
     * Observes incoming mesh messages and emits [NetworkEvent.MeshMessageReceived] when a message
     * is received from the network.
     */
    private fun observeMeshMessages() {
        if (observeMeshMessages == null) {
            runCatching {
                observeMeshMessages = networkManager?.incomingMeshMessages
                    ?.onEach {
                        // simdo-patch: forward RX metadata captured at NetworkLayer.handle().
                        _networkEvents.emit(
                            value = NetworkEvent.MeshMessageReceived(
                                source = it.source.address,
                                destination = it.destination,
                                message = it.message as MeshMessage,
                                sequence = it.sequence,
                                ivIndex = it.ivIndex,
                                ttl = it.ttl,
                            )
                        )
                    }?.let { flow ->
                        // simdo-fork (2026-09-28) — 구독 등록 후 반환(교체 직후 첫 메시지 유실 방지).
                        scope.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) { flow.collect() }
                    }
            }.onFailure {
                logger?.w(category = LogCategory.FOUNDATION_MODEL) {
                    "Error while observing incoming mesh messages: ${it.message}"
                }
            }
        }
    }

    companion object {
        /**
         * simdo-fork (2026-10-28, simdo #99) — 망에 **아직 넣지 않은** 노드를 [exportNode] 와 같은 노드 JSON(CDB `nodes[]` 원소, 장치 키
         * 포함) 으로. 쓰임: 프로비저닝 Data 송신 직전 선기록 ([no.nordicsemi.kotlin.mesh.provisioning.ProvisioningManager.beforeProvisioningData]).
         * 같은 직렬화기라 그 노드가 망에 들어간 뒤 [exportNode] 출력과 같다 (BeforeProvisioningDataHookTest). 되살리기는 [decodeNode].
         * 잠금을 잡지 않는다 — 망 밖 노드 전용. 망 안 노드는 [exportNode].
         */
        fun encodeNode(node: Node): String = MeshNetworkSerializer.encodeNode(node).toString()

        /**
         * simdo-fork (2026-10-28) — [encodeNode]·[exportNode] 가 낸 노드 JSON 을 노드로. 모르는 키는 무시한다. 망에 넣는 것은 호출자
         * (`MeshNetwork.add(node)`).
         */
        fun decodeNode(json: String): Node = MeshNetworkSerializer.decodeNode(json)

        /** simdo-fork (2026-10-15) — 합침 저장 기본값: 마지막 변경 뒤 조용한 시간. */
        val DEFAULT_SAVE_DEBOUNCE: Duration = 2.seconds

        /** simdo-fork (2026-10-15) — 합침 저장 기본값: 첫 변경 뒤 상한. */
        val DEFAULT_SAVE_MAX_DELAY: Duration = 10.seconds
    }
}
