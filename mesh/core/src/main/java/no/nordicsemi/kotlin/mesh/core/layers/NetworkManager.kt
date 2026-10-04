@file:Suppress("unused")

package no.nordicsemi.kotlin.mesh.core.layers

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import no.nordicsemi.kotlin.mesh.bearer.AdvertisingBearer
import no.nordicsemi.kotlin.mesh.bearer.MeshBearer
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.Transmitter
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.ProxyFilterEventHandler
import no.nordicsemi.kotlin.mesh.core.layers.access.AccessLayer
import no.nordicsemi.kotlin.mesh.core.layers.access.Busy
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportLayer
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkLayer
import no.nordicsemi.kotlin.mesh.core.layers.uppertransport.UpperTransportLayer
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.BaseMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.HasOpCode
import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.MeshResponse
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedConfigMessage
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNetKeyDelete
import no.nordicsemi.kotlin.mesh.core.messages.proxy.ProxyConfigurationMessage
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.ApplicationKey
import no.nordicsemi.kotlin.mesh.core.model.Element
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Model
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.logger.LogCategory
import no.nordicsemi.kotlin.mesh.logger.Logger
import kotlin.concurrent.timer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit

/**
 * Network Manager contains the different layers of the mesh network architecture.
 *
 * @property manager Mesh network manager
 * @constructor Constructs the network manager.
 */
internal class NetworkManager internal constructor(
    private val manager: MeshNetworkManager,
) : NetworkManagerEventTransmitter {
    internal val scope: CoroutineScope = manager.scope
    internal var proxy: ProxyFilterEventHandler = manager.proxyFilter

    val logger: Logger?
        get() = manager.logger

    val securePropertiesStorage = manager.secureProperties
    internal var networkLayer = NetworkLayer(this)
        private set
    internal var lowerTransportLayer = LowerTransportLayer(this)
        private set
    internal var upperTransportLayer = UpperTransportLayer(this)
        private set
    internal var accessLayer = AccessLayer(this)
        private set

    // simdo-fork (2026-05-18) — bearer setter 의 이전 collector Job cancel 추가.
    // 원본 lib 가 setter 호출마다 awaitBearerPdus() 새 launchIn — 이전 Job cancel 없음.
    // multiple subscriber 자체는 정상 동작하지만, KotlinMeshActivator 의 race 가 함께 발생하면
    // orphan NetworkManager 의 collector 가 살아남아 SNB 받고 orphan ProxyFilter 에 set →
    // active ProxyFilter.proxy 영원히 null → CannotRelay throw.
    // 본 fix 로 같은 NetworkManager 안의 중복 collector 회피.
    private var bearerCollectorJob: kotlinx.coroutines.Job? = null

    // simdo-fork (2026-09-28) — 수신 채널 lifecycle 잠금.
    //
    // 1) setter 경합: 앱(KotlinMeshActivator)은 activate() 의 load 직후 재할당과 NetworkUpdated 핸들러의
    //    재할당을 **서로 다른 스레드에서 같은 ms 에** 부른다(실기기 2026-09-28 11:29:33.400). 종전 setter 는
    //    `cancel(이전) → 새 collector 대입` 이 원자적이지 않아, 두 호출이 같은 이전 Job 을 cancel 한 뒤 각자
    //    collector 를 대입하면 먼저 대입된 쪽이 참조를 잃고 영구히 살아남았다(같은 스택이 PDU 를 2번 처리 —
    //    로그상 Secure Network Beacon 3회 수신 + "PDU already handled").
    // 2) 같은 bearer 재할당 churn: 앱은 NetworkUpdated(Status 마다 save → 노드당 ~70회)마다 같은 bearer 를
    //    다시 넣는다. 종전 setter 는 매번 collector 를 끊고 다시 띄웠고, `bearer.pdus` 는 replay 없는
    //    SharedFlow 라 그 사이에 도착한 PDU 는 구독자 0 으로 버려졌다. 같은 bearer 이고 collector 가 살아
    //    있으면 no-op 으로 둔다. (새 NetworkManager 는 field 가 null 이라 이 분기에 걸리지 않는다 — E11/N-3
    //    hotfix 가 요구한 "새 인스턴스에는 반드시 bearer 를 물린다" 는 그대로 유지된다.)
    private val channelLock = Any()

    var bearer: MeshBearer? = null
        internal set(value) = synchronized(channelLock) {
            if (value != null && value === field && bearerCollectorJob?.isActive == true && !rxClosed) {
                return@synchronized
            }
            field = value
            bearerCollectorJob?.cancel()
            bearerCollectorJob = if (rxClosed) null else awaitBearerPdus(bearer = value, viaDefaultBearer = true)
        }

    // ─────────────────────────────────────────────────────────────────────────────
    // simdo-fork (2026-09-28) — NetworkManager 교체 시 옛 인스턴스 은퇴.
    //
    // MeshNetworkManager.load()/import()/create() 는 매번 새 NetworkManager 를 만든다. 종전에는 옛 인스턴스의
    // bearer collector 를 끊지 않아 두 스택(각자 NetworkLayer 캐시·SAR·ProxyFilter 트리거)이 같은 PDU 를
    // 처리했다 — 수신 이중 처리, SetFilterType/AddAddressesToFilter·SAR ACK 이중 송신, SEQ 낭비,
    // "Proxy Filter limit reached" 경고(실기기 2026-09-28).
    //
    // 옛 인스턴스를 즉시 끊지 못하는 경우가 하나 있다: 옛 인스턴스로 이미 나간 요청(acknowledged send /
    // Proxy Configuration)은 **그 인스턴스의** incoming flow 에서 응답을 기다린다. 끊으면 그 응답이 새 스택으로만
    // 가서 요청이 타임아웃난다(종전에는 옛 스택이 계속 살아 있어서 우연히 가려졌다 — Realtime CDB import 등
    // 설정 도중 import 는 실제로 일어난다). 그래서 진행 중 송신이 0 이 될 때까지만 수신을 유지하고(drain),
    // 0 이 되는 순간 수신을 닫는다. drain 은 [retireDrainTimeout] 상한이 있다.
    private val inFlight = java.util.concurrent.atomic.AtomicInteger(0)

    @Volatile
    private var retired = false

    @Volatile
    private var rxClosed = false

    private var retiredObserverJobs: List<kotlinx.coroutines.Job> = emptyList()

    /** 진행 중인 송신(응답 대기 포함) 수. 테스트·진단용. */
    internal val inFlightCount: Int
        get() = inFlight.get()

    /** 은퇴 후 수신까지 닫혔는가. 테스트·진단용. */
    internal val isRxClosed: Boolean
        get() = rxClosed

    /**
     * 송신 1건을 in-flight 로 표시한다. 은퇴해 수신이 이미 닫힌 인스턴스로 송신이 들어오면(교체 직전에
     * 인스턴스를 잡아 둔 호출자) 응답을 받을 수 있게 수신을 잠시 다시 연다 — 그 송신이 끝나면 다시 닫는다.
     */
    internal fun enterInFlight() {
        inFlight.incrementAndGet()
        if (retired) synchronized(channelLock) {
            if (rxClosed) {
                logger?.w(LogCategory.BEARER) {
                    "Retired network stack used for sending; reopening RX until the send completes"
                }
                rxClosed = false
                bearerCollectorJob?.cancel()
                bearerCollectorJob = awaitBearerPdus(bearer = bearer, viaDefaultBearer = true)
                _bearers.forEach { (destination, registered) ->
                    bearerCollectorJobs.remove(destination)?.cancel()
                    // 광고 베어러의 목적지별 등록은 송신 경로만 — 수신은 아래 수신 전용 연결이 맡는다.
                    if (registered is AdvertisingBearer) return@forEach
                    awaitBearerPdus(bearer = registered, viaDefaultBearer = false)?.let { bearerCollectorJobs[destination] = it }
                }
                receivers.keys.toList().forEach { receiver ->
                    receivers[receiver]?.cancel()
                    awaitBearerPdus(bearer = receiver, viaDefaultBearer = false)?.let { receivers[receiver] = it }
                }
            }
        }
    }

    internal fun exitInFlight() {
        if (inFlight.decrementAndGet() <= 0 && retired) closeRx()
    }

    private inline fun <T> trackInFlight(block: () -> T): T {
        enterInFlight()
        try {
            return block()
        } finally {
            exitInFlight()
        }
    }

    /**
     * 새 인스턴스가 옛 인스턴스의 수신 채널을 넘겨받는다. [defaultBearer] 를 물리고(구독이 끝난 뒤 반환 —
     * [awaitBearerPdus] 는 UNDISPATCHED), 옛 인스턴스의 per-destination 등록도 그대로 옮긴다. 이것이 끝난
     * 뒤에 옛 인스턴스를 은퇴시키므로 교체 사이에 구독자 0 인 순간이 없다.
     */
    internal fun adoptChannels(
        defaultBearer: MeshBearer?,
        from: NetworkManager?,
        advertisingReceivers: Collection<MeshBearer> = emptyList(),
    ) {
        if (defaultBearer != null) bearer = defaultBearer
        from?.bearers?.forEach { (destination, registered) ->
            registerBearer(destination = destination, meshBearer = registered, ttl = from.ttlOverrideFor(destination))
        }
        // simdo-fork (2026-10-05) — 수신 전용 연결(광고 베어러)도 새 인스턴스로 옮긴다. MeshNetworkManager 가 목록을 들고 있다
        // (네트워크가 아직 없을 때 붙인 것도 load/import 뒤 이 경로로 들어온다).
        advertisingReceivers.forEach { attachReceiver(it) }
    }

    /**
     * 이 인스턴스를 은퇴시킨다. 진행 중 송신이 없으면 즉시 수신을 닫고, 있으면 끝날 때까지(최대
     * [retireDrainTimeout]) 수신을 유지한다. [observerJobs] 는 이 인스턴스의 flow 를 구독하던
     * MeshNetworkManager 쪽 관찰자로, 수신을 닫을 때 같이 끊는다(drain 중 들어온 응답의 save/이벤트는 유지).
     */
    internal fun retire(observerJobs: List<kotlinx.coroutines.Job>) {
        synchronized(channelLock) {
            retired = true
            retiredObserverJobs = observerJobs
        }
        if (inFlight.get() <= 0) {
            closeRx()
            return
        }
        logger?.i(LogCategory.BEARER) {
            "Replaced network stack keeps RX until ${inFlight.get()} in-flight message(s) complete"
        }
        scope.launch {
            kotlinx.coroutines.withTimeoutOrNull(retireDrainTimeout) {
                while (inFlight.get() > 0) kotlinx.coroutines.delay(50)
            }
            if (!rxClosed) {
                if (inFlight.get() > 0) {
                    logger?.w(LogCategory.BEARER) {
                        "Replaced network stack still has ${inFlight.get()} in-flight message(s) after " +
                            "$retireDrainTimeout; closing RX"
                    }
                }
                closeRx(force = true)
            }
        }
    }

    private val retireDrainTimeout: Duration
        get() = manager.networkParameters.acknowledgementMessageTimeout + 5.seconds

    private fun closeRx(force: Boolean = false) {
        val observers: List<kotlinx.coroutines.Job>
        synchronized(channelLock) {
            if (!retired || rxClosed) return
            if (!force && inFlight.get() > 0) return
            rxClosed = true
            bearerCollectorJob?.cancel()
            bearerCollectorJob = null
            bearerCollectorJobs.values.forEach { it.cancel() }
            bearerCollectorJobs.clear()
            // 수신 전용 연결은 목록을 남기고 수집만 끊는다 (enterInFlight 가 다시 열 수 있게).
            receivers.keys.toList().forEach { receivers[it]?.cancel(); receivers[it] = null }
            observers = retiredObserverJobs
            retiredObserverJobs = emptyList()
        }
        observers.forEach { it.cancel() }
        // 은퇴한 스택의 주기 publication 타이머·acknowledgement 컨텍스트 정리(upstream 의 teardown).
        runCatching { accessLayer.close() }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // simdo-fork (2026-06-07, P6 M=2) — per-destination bearer registry.
    //
    // 배경: config 병렬화(P6)는 N 노드를 N 개의 1-hop GATT(Sub 풀 핸들)로 **동시** config 한다.
    //   기존 lib 는 [bearer] **단일 슬롯**뿐이라 한 시점에 한 채널로만 송수신 가능 → config 가
    //   직렬일 수밖에 없었다(app 이 매 노드 [bearer] 를 swap). 본 registry 가 유일 구조 blocker 다.
    //
    // 설계:
    //   - [bearer] 는 **default bearer** 로 유지(하위호환). group/proxy-config/미등록 dst 전부 default
    //     로 라우팅 → 평상시 측위 RX·group 제어는 회귀 0(아래 [bearerFor] 가 미등록이면 default 반환).
    //   - [bearers] 는 dst unicast → 그 노드 1-hop bearer. config 워커가 노드별로 register/unregister.
    //   - TX([NetworkLayer.send])는 `networkPdu.destination.address` 로 [bearerFor] lookup.
    //     (dst 는 PDU 헤더에 이미 있음. unicast 만 라우팅 — group/virtual 은 default.)
    //   - RX 는 dst 라우팅 불필요(SAR/ack 상관이 src/dest-keyed) → registered bearer 마다 collector
    //     Job 을 띄워 같은 [handle] 로 fan-in. [bearer] 와 동일 처리 경로(단순 병합).
    //
    // 동시성 안전(feasibility 코드 study 확정): ack 상관(AccessLayer.reliableMessageContexts=List,
    //   source/responseOpCode/destination 3-키 매칭)·SAR RX(incompleteSegments per-(src,seqZero))·
    //   TX seq(seqMutex atomic)·UpperTransport queue(per-dest)·CDB(cdbMutex) 전부 src/dest-keyed →
    //   N 동시 multi-destination config 트랜잭션이 cross-talk 0. registry 는 채널 라우팅만 추가한다.
    // simdo-fork (2026-09-28) — registry 와 default bearer 수신 채널이 같은 잠금을 쓴다(은퇴 시 일괄 정리).
    private val bearersLock: Any
        get() = channelLock

    @Volatile
    private var _bearers: Map<Address, MeshBearer> = emptyMap()

    /** dst unicast → 그 노드 1-hop bearer. 미등록 dst·group·proxy-config 는 [bearer](default) 로. */
    val bearers: Map<Address, MeshBearer>
        get() = _bearers

    private val bearerCollectorJobs = mutableMapOf<Address, kotlinx.coroutines.Job>()

    /**
     * 주어진 dst 로 송신할 bearer 를 반환한다. registry 에 등록된 1-hop bearer 가 있으면 그것,
     * 없으면 [bearer](default). 미등록 dst·group·proxy-config 는 자연히 default 로 떨어진다.
     */
    internal fun bearerFor(destination: Address): MeshBearer? =
        _bearers[destination] ?: bearer

    /**
     * simdo-fork (2026-06-07, P6 M=2) — [destination] 으로 가는 **per-dest 1-hop bearer 가 등록**돼
     * 있는지. 등록돼 있으면 그 노드는 default proxy([proxyFilter.proxy]) 가 아니라 **자기 자신**(직접
     * 1-hop GATT) 으로 도달한다 → MeshNetworkManager 의 send-전 proxy-key 가드는 default proxy 가
     * 아니라 **목적지 노드 자신**의 netkey 인지(node.knows)를 봐야 한다. 그 분기 판정에 쓴다.
     */
    internal fun hasRegisteredBearer(destination: Address): Boolean =
        _bearers.containsKey(destination)

    /**
     * config 워커(P6)가 노드 [destination] 의 1-hop config 진입 직전 호출 — [meshBearer] 를 그 노드
     * dst 로 등록하고 RX collector 를 띄운다. 멱등(같은 bearer 재등록 no-op). default [bearer] 는 건드리지
     * 않는다(측위 RX·group 제어 채널 보존). 등록 후 그 dst 로 가는 TX/RX 는 registered bearer 를 탄다.
     */
    fun registerBearer(destination: Address, meshBearer: MeshBearer, ttl: UByte? = null) = synchronized(bearersLock) {
        // simdo-fork (2026-10-05) — 목적지별 요청 TTL. 이 목적지로 TTL 을 정하지 않고(null) 보내는 메시지·이 목적지가 보낸 분할의
        // Segment Acknowledgment 가 이 값을 쓴다 ([ttlOverrideFor]). null = 종전(로컬 노드 Default TTL → 네트워크 파라미터).
        _ttlOverrides = if (ttl == null) _ttlOverrides - destination else _ttlOverrides + (destination to ttl)
        if (_bearers[destination] === meshBearer) return@synchronized
        // 같은 dst 에 이전 bearer 가 있었다면 그 collector 를 먼저 정리(누수 방지).
        bearerCollectorJobs.remove(destination)?.cancel()
        if (rxClosed || meshBearer is AdvertisingBearer) {
            // 은퇴해 수신이 닫힌 스택 — TX 라우팅만 기록하고 collector 는 띄우지 않는다.
            // simdo-fork (2026-10-05) — 광고 베어러도 송신 경로만 기록한다. 수신은 [attachReceiver] 한 번 (같은 공중 수신을
            // 엘리먼트 수만큼 처리하지 않게 — 5엘리먼트 조명 4대 동시 설정이면 20배였다).
            _bearers = _bearers + (destination to meshBearer)
            return@synchronized
        }
        // simdo-fork (2026-06-08, A-H1) — RX collector 를 **먼저** 띄우고 그게 살아 있을 때만 _bearers 에
        // 커밋한다. 종전엔 _bearers 커밋(TX 라우팅 활성)이 collector 생성 앞이라, awaitBearerPdus 가 null
        // (bearer.pdus 미가용)이면 반쪽 등록(TX 는 이 bearer 로 가나 RX 없음)이 됐다. 둘 다 bearersLock
        // 안이라 어차피 원자지만, 순서를 RX-then-TX 로 바로잡아 반쪽 등록 가능성을 구조적으로 제거한다.
        // (현 구현 awaitBearerPdus 는 bearer 가 non-null 이고 pdus 가 항상 있으면 non-null Job 반환.)
        val job = awaitBearerPdus(bearer = meshBearer, viaDefaultBearer = false) ?: return@synchronized
        bearerCollectorJobs[destination] = job
        _bearers = _bearers + (destination to meshBearer)
        logger?.i(LogCategory.BEARER) {
            "Registered per-dest bearer for 0x${destination.toHexString()}"
        }
    }

    /**
     * config 종료 후 호출 — 노드 [destination] 의 1-hop bearer 등록 해제 + RX collector 취소.
     * 이후 그 dst 로 가는 TX/RX 는 다시 default [bearer] 로 떨어진다. 멱등.
     */
    fun unregisterBearer(destination: Address) = synchronized(bearersLock) {
        _ttlOverrides = _ttlOverrides - destination
        if (_bearers[destination] == null) return@synchronized
        bearerCollectorJobs.remove(destination)?.cancel()
        _bearers = _bearers - destination
        logger?.i(LogCategory.BEARER) {
            "Unregistered per-dest bearer for 0x${destination.toHexString()}"
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // simdo-fork (2026-10-05, 커미셔닝 동글 "얇은 무선" 1단계) — 목적지별 TTL · 수신 전용 연결.

    @Volatile
    private var _ttlOverrides: Map<Address, UByte> = emptyMap()

    /**
     * [destination] 으로 TTL 을 정하지 않고 보내는 메시지의 TTL ([registerBearer] 의 `ttl`). 없으면 null — 종전대로 로컬 노드 Default TTL.
     * 광고 베어러로 직접 닿는 목적지에 요청을 작게 flood 하려고 쓴다 (설계 §9.1·9.2). 등록 해제하면 사라진다.
     */
    internal fun ttlOverrideFor(destination: Address): UByte? = _ttlOverrides[destination]

    /** 수신 전용 연결 → 그 수집기 (은퇴해 수신이 닫혔으면 null). 동일성 기준. [channelLock] 안에서만 바꾼다. */
    private val receivers = java.util.IdentityHashMap<MeshBearer, kotlinx.coroutines.Job?>()

    /** 테스트·진단용 — 지금 살아 있는 수신 수집기 수 (기본 베어러 + 목적지별 + 수신 전용). */
    internal val rxCollectorCount: Int
        get() = synchronized(channelLock) {
            listOfNotNull(bearerCollectorJob).count { it.isActive } +
                bearerCollectorJobs.values.count { it.isActive } +
                receivers.values.count { it?.isActive == true }
        }

    /**
     * [receiver] 의 `pdus` 를 이 스택에 한 번 붙인다 (송신 경로는 만들지 않는다). 광고 베어러용 — 목적지별 등록은 송신만 정하고,
     * 수신은 이 연결 하나로 받는다. 기본 베어러가 아니므로 Secure Network Beacon 은 IV 판정에만 쓰인다(프록시 필터를 건드리지 않음).
     * 멱등.
     */
    internal fun attachReceiver(receiver: MeshBearer) = synchronized(channelLock) {
        if (receivers.containsKey(receiver) && (rxClosed || receivers[receiver]?.isActive == true)) return@synchronized
        receivers[receiver]?.cancel()
        receivers[receiver] = if (rxClosed) null else awaitBearerPdus(bearer = receiver, viaDefaultBearer = false)
        logger?.i(LogCategory.BEARER) { "Attached receive-only bearer (${receiver::class.simpleName})" }
    }

    /** [attachReceiver] 를 되돌린다. 멱등. */
    internal fun detachReceiver(receiver: MeshBearer) = synchronized(channelLock) {
        if (!receivers.containsKey(receiver)) return@synchronized
        receivers.remove(receiver)?.cancel()
        logger?.i(LogCategory.BEARER) { "Detached receive-only bearer (${receiver::class.simpleName})" }
    }

    val meshNetwork: MeshNetwork
        get() = manager.network!!

    val networkParameters: NetworkParameters
        get() = manager.networkParameters
    private val mutex = Mutex()

    // simdo-fork (2026-06-05 Fork-3 / 2026-06-06 P4 hoist) — CDB mutation race.
    // 병렬 config (mode2) 에서 N 노드의 ConfigStatus 응답이 멀티스레드(Dispatchers.Default)로
    // 동시에 RX 되면, handleResponses 가 같은 MeshNetwork 의 _appKeys/_subscribe/_groups 등
    // mutable collection 에 동시 write → ConcurrentModificationException + lost update.
    // 동시에 export()/serialize 트래버설이 같은 collection 을 순회하면 CME.
    // cdbMutex 는 순수 in-memory CDB write/traversal 만 직렬화한다.
    // **주의**: send / RTT 대기 / reply (PDU 송신) 는 이 lock 밖에 둔다 → throughput(병렬 in-flight) 보존.
    // 기존 mutex(:97) 는 reliableMessageContexts/outgoingMessages 보호 전용이라 별개로 유지.
    //
    // P4 (2026-06-06): lock 을 mutated 대상인 MeshNetwork 로 hoist 했다. config-RX(여기) 와 병렬
    // provisioning add(ProvisioningManager, NetworkManager 를 모름)가 **같은 lock 인스턴스**를
    // 공유해야 race 가 없다. MeshNetwork.cdbMutex 로 위임 — 네트워크 미로드(null) 시엔 경합 불가라
    // 안정적 fallback lock 을 쓴다(이 경우 RX 도 불가능하므로 실질 무영향).
    private val fallbackCdbMutex = Mutex()
    internal val cdbMutex: Mutex
        get() = manager.network?.cdbMutex ?: fallbackCdbMutex

    private var outgoingMessages = mutableSetOf<MeshAddress>()

    private val _incomingProxyMessages = MutableSharedFlow<ReceivedMessage>()
    internal val incomingProxyMessages
        get() = _incomingProxyMessages.asSharedFlow()

    private val _incomingMeshMessages = MutableSharedFlow<ReceivedMessage>()
    internal val incomingMeshMessages
        get() = _incomingMeshMessages.asSharedFlow()

    /** 테스트 전용 — access 복호화 없이 수신 메시지 1건을 이 스택의 incoming flow 로 흘린다. */
    internal suspend fun emitIncomingMeshMessageForTest(message: ReceivedMessage) =
        _incomingMeshMessages.emit(value = message)

    private val _networkManagerEventFlow = MutableSharedFlow<NetworkManagerEvent>()
    override val networkManagerEventFlow
        get() = _networkManagerEventFlow.asSharedFlow()

    private val ioScope = CoroutineScope(context = SupervisorJob() + manager.ioDispatcher)

    /**
     * Awaits and returns the mesh pdu received by the given [bearer] and feeds it into the common
     * [handle] path. Used for both the default [bearer] and per-destination registered bearers
     * (P6) — RX is src/dest-keyed downstream, so multiple bearers simply fan-in.
     */
    private fun awaitBearerPdus(bearer: MeshBearer?, viaDefaultBearer: Boolean): kotlinx.coroutines.Job? {
        val pdus = bearer?.pdus ?: return null
        // simdo-fork (2026-10-05) — 광고 베어러로 온 PDU 는 네트워크 계층이 해독 직후 목적지를 거른다 ([AdvertisingBearer]).
        val viaAdvertisingBearer = bearer is AdvertisingBearer
        // simdo-fork (2026-09-28) — UNDISPATCHED: `collect` 가 SharedFlow 구독을 등록한 뒤에야 이 함수가
        // 반환된다. 교체·재구독 직후 곧바로 옛 collector 를 끊어도 구독자 0 인 순간이 생기지 않는다
        // (`pdus` 는 replay 없는 SharedFlow — 구독자 0 일 때 emit 된 PDU 는 사라진다).
        return scope.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
            pdus.collect {
                runCatching {
                    handle(
                        incomingPdu = it.data,
                        type = it.type,
                        viaDefaultBearer = viaDefaultBearer,
                        viaAdvertisingBearer = viaAdvertisingBearer,
                    )
                }
                    .onFailure { throwable ->
                        logger?.e(LogCategory.BEARER) { "Bearer error: $throwable" }
                    }
            }
        }
    }

    /**
     * Emits the network manager event.
     *
     * @param event Network manager event.
     */
    override suspend fun emitNetworkManagerEvent(event: NetworkManagerEvent) {
        _networkManagerEventFlow.emit(value = event)
    }

    /**
     * Handles the received PDU of a given type.
     *
     * This method parses incoming Proxy messages and mesh messages and emits them to their
     * respective SharedFlow which are observed by the respective subscriber.
     *
     * @param incomingPdu Incoming PDU.
     * @param type        PDU type.
     * @param viaDefaultBearer simdo-fork (2026-09-29) — 기본 베어러로 왔는가 ([NetworkLayer.handle] 참조). 목적지별 등록 베어러의
     *   수집기만 false 를 넘긴다. 기본 true = 종전.
     * @param viaAdvertisingBearer simdo-fork (2026-10-05) — [AdvertisingBearer] 로 왔는가 (목적지 거름). 기본 false = 종전.
     */
    fun handle(incomingPdu: ByteArray, type: PduType, viaDefaultBearer: Boolean = true, viaAdvertisingBearer: Boolean = false) {
        scope.launch {
            networkLayer.handle(
                incomingPdu = incomingPdu,
                type = type,
                viaDefaultBearer = viaDefaultBearer,
                viaAdvertisingBearer = viaAdvertisingBearer,
            )
                ?.let {
                    if (it.message is ProxyConfigurationMessage) {
                        _incomingProxyMessages.emit(value = it)
                    } else {
                        _incomingMeshMessages.emit(value = it)
                    }
                }
        }
    }

    /**
     * Await a response to the sent message.
     *
     * @param destination Destination address of the message.
     * @param timeout     Timeout duration.
     */
    suspend fun awaitMeshMessageResponse(
        destination: Address,
        responseOpcode: UInt,
        timeout: Duration,
        matches: (BaseMeshMessage) -> Boolean = { true },
    ) = awaitMeshMessageResponse(
        destination = MeshAddress.create(destination),
        responseOpcode = responseOpcode,
        timeout = timeout,
        matches = matches,
    )

    /**
     * Awaits for a response for a previously sent message.
     *
     * simdo-fork (2026-09-28) — 구독이 **이 함수가 처음 멈추기 전에** 등록되도록 `withTimeout { first {} }` 로 쓴다
     * (종전 `.timeout()` 연산자는 내부 produce 코루틴이 dispatch 된 뒤에야 구독했다). 호출자는 송신 **전에**
     * `async(start = UNDISPATCHED)` 로 이 함수를 시작해야 한다 — [incomingMeshMessages] 는 replay 없는 SharedFlow 라
     * 구독 전에 도착한 응답은 사라진다. 실기기 2026-09-28 14:46:22: 2분할 ConfigAppKeyAdd 의 Status 가 송신 함수
     * (마지막 분할 뒤 분할 간격만큼 기다림) 반환보다 몇 ms 먼저 도착해 수신·복호까지 됐는데도 15 s 타임아웃.
     *
     * simdo-fork (2026-09-29) — [matches]: 응답이 **그 요청의** 응답인지 파라미터까지 본다 (요청 파라미터를 되돌려 주는 Config Status
     * — [no.nordicsemi.kotlin.mesh.core.layers.access.responseMatchesRequest]). 종전에는 (보낸 주소, 응답 opcode) 만 비교해, 앞 요청의
     * 늦은 응답(앱이 포기한 뒤 lib 재전송이 받아 낸 것)이 다음 요청의 성공으로 기록됐다 (실기기 2026-09-28 17:57:11.259 — app0·0x1000
     * Bind Status 가 bind(app1, 0x1000) 의 응답이 됨).
     *
     * @param destination Destination address of the message.
     * @param timeout     Timeout duration.
     */
    suspend fun awaitMeshMessageResponse(
        destination: MeshAddress,
        responseOpcode: UInt,
        timeout: Duration,
        matches: (BaseMeshMessage) -> Boolean = { true },
    ): ReceivedMessage? = try {
        withTimeout(timeout) {
            incomingMeshMessages.first {
                destination == it.source && responseOpcode == (it.message as? HasOpCode)?.opCode &&
                    matches(it.message)
            }
        }
    } catch (e: TimeoutCancellationException) {
        logger?.w(LogCategory.BEARER) {
            "Timed out waiting for a response with response opCode 0x${
                responseOpcode.toHexString(
                    format = HexFormat.UpperCase
                )
            } from ${
                destination.address.toHexString(
                    format = HexFormat {
                        number.prefix = "0x"
                        upperCase = true
                    }
                )
            }: $e"
        }
        throw e
    }

    /**
     * Awaits for a response to a sent message.
     */
    suspend fun awaitProxyMessageResponse() = incomingProxyMessages.firstOrNull()

    /**
     * Clear outgoing messages for a given destination.
     *
     * @param destination destination address.
     */
    internal suspend fun clearOutgoingMessages(destination: MeshAddress) {
        mutex.withLock { outgoingMessages.remove(destination) }
    }

    /**
     * Publishes the given message using the Publish information from the given Model. If
     * publication is not set, this message does nothing.
     *
     * If publication retransmission is set, this method will retransmit the message specified
     * number of times, if applicable keeps the same TID value.
     *
     * @param message     Message to be published.
     * @param from        Source model from which the message is originating from.
     */
    suspend fun publish(message: MeshMessage, from: Model) {
        val publish = from.publish ?: return
        val localElement = from.parentElement ?: return
        val applicationKey = meshNetwork.applicationKey(index = publish.index) ?: return

        // calculate the TTL to be used
        val ttl = when (publish.ttl != 0xFF.toUByte()) {
            true -> publish.ttl
            false -> localElement.parentNode?.defaultTTL ?: networkParameters.defaultTtl
        }

        accessLayer.send(
            message = message,
            element = localElement,
            destination = publish.address as MeshAddress,
            ttl = ttl,
            applicationKey = applicationKey,
            retransmit = false
        )

        if (message is AcknowledgedMeshMessage) {
            var count = publish.retransmit.count.toInt()
            if (count > 0) {
                val interval: Duration = publish.retransmit.interval
                timer(
                    daemon = false,
                    period = interval.toLong(DurationUnit.MILLISECONDS)
                ) {
                    if (--count > 0) {
                        scope.launch {
                            accessLayer.send(
                                message = message,
                                element = localElement,
                                destination = publish.address as MeshAddress,
                                ttl = ttl,
                                applicationKey = applicationKey,
                                retransmit = true
                            )
                        }
                    } else {
                        cancel()
                    }
                }
            }
        }
    }

    /**
     * Ensures that the local node is not busy sending a message to the given destination address.
     *
     * @param destination Destination address.
     * @return `true` if the node is busy sending a message to the given destination address,
     * @throws Busy if the node is busy sending a message to the given destination address.
     */
    @Throws(Busy::class)
    private suspend fun ensureNotBusy(destination: MeshAddress) = mutex.withLock {
        require(!outgoingMessages.contains(destination)) { throw Busy() }
        outgoingMessages.add(destination)
        false
    }

    /**
     * Encrypts the message with the Application Key and a Network Key bound to it, and sends to the
     * given destination address.
     *
     * This method does not send nor return PDUs to be sent. Instead, for each created segment it
     * calls transmitter's [Transmitter.send] method, which should send the PDU over the air. This
     * is in order to support retransmission in case a packet was lost and needs to be sent again
     * after block acknowledgment was received.
     *
     * @param message          Message to be sent.
     * @param element          Source Element.
     * @param destination      Destination address.
     * @param initialTtl       Initial TTL (Time To Live) value of the message. If `nil`, the
     *                         default Node TTL will be used.
     * @param applicationKey   Application Key to sign the message.
     * @throws Busy if the node is busy sending a message to the given destination address.
     */
    @Throws(Busy::class)
    suspend fun send(
        message: MeshMessage,
        element: Element,
        destination: MeshAddress,
        initialTtl: UByte?,
        applicationKey: ApplicationKey,
    ): MeshMessage? = if (!ensureNotBusy(destination = destination)) {
        // simdo-fork (2026-06-08, C-F2) — busy-set 누수 봉인. 종전 `.also{}` 는 정상 return 시만 remove 라
        // accessLayer.send 가 timeout(awaitMeshMessageResponse re-throw)/cancel 로 throw 하면 dst 가
        // outgoingMessages 에 영구 잔류 → 같은 dst 재시도가 Busy. try/finally 로 throw/cancel/정상 모두 remove.
        // ensureNotBusy 가 add 성공(=false 반환) 한 뒤에만 이 블록에 진입하므로 finally remove 가 항상 짝맞음
        // (ensureNotBusy 가 Busy throw 하면 add 안 했고 이 블록 미진입 → 남의 엔트리 오삭제 없음).
        try {
            trackInFlight {
                accessLayer.send(
                    message = message,
                    element = element,
                    destination = destination,
                    ttl = initialTtl,
                    applicationKey = applicationKey,
                    retransmit = false
                )
            }
        } finally {
            // NonCancellable — outer cancel(예: caller withTimeout) 로 코루틴이 cancel 된 상태에선
            // finally 내 suspend(mutex.withLock)도 즉시 CancellationException 을 던져 remove 가 건너뛰어진다.
            // busy-set 정리는 cancel 경로에서도 반드시 일어나야 하므로 NonCancellable 로 보장한다.
            withContext(NonCancellable) { mutex.withLock { outgoingMessages.remove(destination) } }
        }
    } else null

    /**
     * Encrypts the message with the Application Key and a Network Key bound to it, and sends to the
     * given destination address.
     *
     * This method does not send nor return PDUs to be sent. Instead, for each created segment it
     * calls transmitter's [Transmitter.send] method, which should send the PDU over the air. This
     * is in order to support retransmission in case a packet was lost and needs to be sent again
     * after block acknowledgment was received.
     *
     * @param message         Message to be sent.
     * @param element         Source Element.
     * @param destination     Destination Unicast Address.
     * @param initialTtl      Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                        Node TTL will be used.
     * @param applicationKey  Application Key to sign the message.
     * @throws Busy if the node is busy sending a message to the given destination address.
     */
    @Throws(Busy::class)
    suspend fun send(
        message: AcknowledgedMeshMessage,
        element: Element,
        destination: UnicastAddress,
        initialTtl: UByte?,
        applicationKey: ApplicationKey,
    ): MeshMessage? {
        //val meshAddress = MeshAddress.create(address = destination)
        require(!ensureNotBusy(destination = destination)) { return null }

        // simdo-fork (2026-06-08, C-F2) — busy-set 누수 봉인. ensureNotBusy add 성공 후에만 try 진입 →
        // accessLayer.send 가 ack timeout 으로 throw 해도 finally 가 outgoingMessages 에서 dst 제거.
        return try {
            trackInFlight {
                accessLayer.send(
                    message = message,
                    element = element,
                    destination = destination,
                    ttl = initialTtl,
                    applicationKey = applicationKey,
                    retransmit = false
                )
            }
        } finally {
            // NonCancellable — cancel 경로에서도 busy-set 정리 보장(위 send 참조).
            withContext(NonCancellable) { mutex.withLock { outgoingMessages.remove(destination) } }
        }
    }

    /**
     * Encrypts the message with the Device Key and the first Network Key known to the target
     * device, and sends to the given destination address.
     *
     * This method does not send nor return PDUs to be sent. Instead, for each created segment it
     * calls transmitter's [Transmitter.send] method, which should send the PDU over the air. This
     * is in order to support retransmission in case a packet was lost and needs to be sent again
     * after block acknowledgment was received.
     *
     * @param configMessage  Message to be sent.
     * @param element        Source Element.
     * @param destination    Destination address.
     * @param initialTtl     Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                       Node TTL will be used.
     * @throws Busy if the node is busy sending a message to the given destination address.
     */
    @Throws(Busy::class)
    suspend fun send(
        configMessage: UnacknowledgedConfigMessage,
        element: Element,
        destination: Address,
        initialTtl: UByte?,
        networkKey: NetworkKey,
    ) {
        val meshAddress = MeshAddress.create(address = destination)
        require(!ensureNotBusy(destination = meshAddress)) { throw Busy() }
        // simdo-fork (2026-06-08, C-F2) — busy-set 누수 봉인. ensureNotBusy add 성공 후에만 try 진입 →
        // accessLayer.send throw/cancel 시에도 finally 가 outgoingMessages 에서 meshAddress 제거.
        try {
            trackInFlight {
                accessLayer.send(
                    message = configMessage,
                    localElement = element,
                    destination = destination,
                    initialTtl = initialTtl,
                    networkKey = networkKey
                )
            }
        } finally {
            // Added to clear the outgoing message list
            // NonCancellable — cancel 경로에서도 busy-set 정리 보장(위 send 참조).
            withContext(NonCancellable) { mutex.withLock { outgoingMessages.remove(meshAddress) } }
        }
    }

    /**
     * Encrypts the message with the Device Key and the first Network Key known to the target device,
     * and sends to the given destination address.
     *
     * The [ConfigNetKeyDelete] will be signed with a different Network Key that is removing.
     *
     * This method does not send nor return PDUs to be sent. Instead, for each created segment it
     * calls transmitter's [Transmitter.send] method, which should send the PDU over the air. This
     * is in order to support retransmission in case a packet was lost and needs to be sent again
     * after block acknowledgment was received.
     *
     * @param configMessage   Message to be sent.
     * @param element         Source Element.
     * @param destination     Destination address.
     * @param initialTtl      Initial TTL (Time To Live) value of the message. If `nil`, the default
     *                        Node TTL will be used.
     * @throws Busy if the node is busy sending a message to the given destination address.
     */
    @Throws(Busy::class)
    suspend fun send(
        configMessage: AcknowledgedConfigMessage,
        element: Element,
        destination: Address,
        initialTtl: UByte?,
        networkKey: NetworkKey,
    ): MeshMessage? {
        val meshAddress = MeshAddress.create(address = destination)
        require(!ensureNotBusy(destination = meshAddress)) { return null }
        // simdo-fork (2026-06-08, C-F2) — busy-set 누수 봉인. ensureNotBusy add 성공 후에만 try 진입 →
        // config ack timeout(awaitMeshMessageResponse re-throw)/cancel 에도 finally 가 meshAddress 제거.
        // P6: outgoingMessages 는 dst-keyed(registry/default 무관) — config 실패가 같은 dst 의 측위/group
        // 제어 채널까지 Busy 오염시키던 누수를 봉인한다.
        return try {
            trackInFlight {
                accessLayer.send(
                    message = configMessage,
                    localElement = element,
                    destination = destination,
                    initialTtl = initialTtl,
                    networkKey = networkKey
                )
            }
        } finally {
            // Added to clear the outgoing message list
            // NonCancellable — cancel 경로에서도 busy-set 정리 보장(위 send 참조).
            withContext(NonCancellable) { mutex.withLock { outgoingMessages.remove(meshAddress) } }
        }
    }

    /**
     * Sends the Proxy Configuration message to the connected Proxy node.
     *
     * @param message Proxy Configuration message to be sent.
     */
    suspend fun send(message: ProxyConfigurationMessage): ProxyConfigurationMessage? =
        trackInFlight { networkLayer.send(message = message) }

    /**
     * Replies to the received message, which was sent with the given key set, with the given
     * message.
     *
     * @param origin      Destination address of the message that the reply is for.
     * @param message     Response message to be sent.
     * @param element     Source Element.
     */
    suspend fun reply(
        origin: Address,
        message: MeshResponse,
        element: Element,
        destination: Address,
        keySet: KeySet,
    ) {
        accessLayer.reply(
            origin = origin,
            message = message,
            element = element,
            destination = destination,
            keySet = keySet
        )
    }

    /**
     * Cancels sending the message.
     *
     * @param handle Message handle.
     */
    suspend fun cancel(handle: MessageHandle) {
        accessLayer.cancel(handle)
    }
}

/**
 * Data class representing a received message, containing the source and destination addresses, and
 * the message itself.
 *
 * @property source      Source address from which the message was received.
 * @property destination Destination address to which the message is intended.
 * @property message     Message that was received.
 * @property sequence    simdo-patch: 24-bit sequence number from the Network PDU.
 * @property ivIndex     simdo-patch: 32-bit IV Index used to decrypt the Network PDU.
 * @property ttl         simdo-patch: TTL value read from the Network PDU header.
 */
internal data class ReceivedMessage(
    val source: MeshAddress,
    val destination: MeshAddress,
    val message: BaseMeshMessage,
    // simdo-patch: RX metadata captured at NetworkLayer.handle() emit site (single source of truth).
    val sequence: UInt,
    val ivIndex: UInt,
    val ttl: UByte,
)