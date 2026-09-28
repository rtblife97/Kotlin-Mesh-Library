package no.nordicsemi.kotlin.mesh.core.layers

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import no.nordicsemi.kotlin.mesh.bearer.BearerError
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.MeshBearer
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.access.AccessPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.AccessMessage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPduType
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkPduDecoder
import no.nordicsemi.kotlin.mesh.core.layers.uppertransport.UpperTransportPdu
import no.nordicsemi.kotlin.mesh.core.messages.ConfigMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppBind
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppStatus
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.SigModelId
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.crypto.Crypto
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-09-29) — 폰 동시 GATT 설정 1단계의 라이브러리 불변 (설계 docs/feature/ANDROID_COMMISSIONING_CONCURRENT_GATT_DESIGN.md §9).
 *
 *  - 불변 2: 목적지별 등록이 **비어 있으면** 송수신·프록시 필터 동작이 종전과 같다 (기본 링크 Beacon → 필터 초기화, 기본 링크 Closed →
 *    필터 키 삭제). 등록 링크의 Beacon·Closed 는 기본 링크 필터 상태를 건드리지 않는다 (D3).
 *  - 불변 3: 목적지 X 로 가는 PDU 는 X 의 등록 링크 또는 기본 베어러로만 나간다.
 *  - 불변 6 (통합): 실제 NetKey·DevKey 로 암호화한 두 노드의 Status 를 가짜 링크 두 개로 동시에 주입하면(서로의 응답이 릴레이로 다른
 *    링크에도 옴) 각 요청은 자기 노드의 응답으로만 끝난다.
 *
 * 가짜 링크는 lib 가 실제로 내보내는 PDU 를 보고(복호화해 목적지를 확인), 응답은 lib 수신 경로(링크의 pdus → NetworkLayer 복호화 →
 * LowerTransport → AccessLayer)로 흘린다. 무선·안드로이드 GATT 동작은 여기서 확인할 수 없다 (실기기 목록).
 */
@OptIn(ExperimentalUuidApi::class)
class ConcurrentLinkIsolationTest {

    private val provisionerAddress = 0x0001
    private val nodeX = 0x0007
    private val nodeY = 0x0105

    // ── 불변 2 ────────────────────────────────────────────────────────────────

    @Test
    fun `empty registry - Secure Network Beacon on the default link initialises the proxy filter as before`() = runBlocking {
        val mgr = manager()
        val nm = mgr.networkManager!!
        val default = LinkFake("default")
        mgr.meshBearer = default
        assertNull(nm.networkLayer.proxyNetworkKey)

        default.inject(beacon(mgr.network!!))
        waitUntil(2_000) { default.sentOfType(PduType.PROXY_CONFIGURATION) > 0 }

        assertNotNull("기본 링크 Beacon → 필터 키", nm.networkLayer.proxyNetworkKey)
        assertTrue("필터 설정은 기본 링크로 나간다", default.sentOfType(PduType.PROXY_CONFIGURATION) > 0)
    }

    @Test
    fun `empty registry - Closed on the default link clears the proxy key as before`() = runBlocking {
        val mgr = manager()
        val nm = mgr.networkManager!!
        val default = LinkFake("default", closed = true)
        mgr.meshBearer = default
        nm.networkLayer.proxyNetworkKey = mgr.network!!.networkKeys.first()

        val thrown = runCatching { nm.networkLayer.send(heartbeat(mgr.network!!, nodeX), PduType.NETWORK_PDU, 5u) }.exceptionOrNull()

        assertTrue("기본 링크가 닫혀 있으면 Closed", thrown is BearerError.Closed)
        assertNull("기본 링크 Closed → 필터 키 삭제 (종전 동작)", nm.networkLayer.proxyNetworkKey)
    }

    @Test
    fun `registered link Beacon does not initialise the default link filter`() = runBlocking {
        val mgr = manager()
        val nm = mgr.networkManager!!
        val default = LinkFake("default")
        val linkX = LinkFake("X")
        mgr.meshBearer = default
        mgr.registerBearer(nodeX.toUShort(), linkX)

        linkX.inject(beacon(mgr.network!!))
        delay(500)

        assertNull("등록 링크 Beacon 은 기본 링크 필터 키를 세우지 않는다", nm.networkLayer.proxyNetworkKey)
        assertEquals("필터 설정이 어디로도 나가지 않는다", 0, default.sentOfType(PduType.PROXY_CONFIGURATION) + linkX.sentOfType(PduType.PROXY_CONFIGURATION))
    }

    @Test
    fun `Closed on a registered link keeps the default link proxy key`() = runBlocking {
        val mgr = manager()
        val nm = mgr.networkManager!!
        mgr.meshBearer = LinkFake("default")
        mgr.registerBearer(nodeX.toUShort(), LinkFake("X", closed = true))
        val key = mgr.network!!.networkKeys.first()
        nm.networkLayer.proxyNetworkKey = key

        val thrown = runCatching { nm.networkLayer.send(heartbeat(mgr.network!!, nodeX), PduType.NETWORK_PDU, 5u) }.exceptionOrNull()

        assertTrue("등록 링크가 닫혀 있으면 그 송신만 Closed", thrown is BearerError.Closed)
        assertTrue("기본 링크 필터 키는 그대로", nm.networkLayer.proxyNetworkKey === key)
    }

    // ── 불변 3 ────────────────────────────────────────────────────────────────

    @Test
    fun `a PDU to X leaves only on X's link or the default link, never on another slot's link`() = runBlocking {
        val mgr = manager()
        val nm = mgr.networkManager!!
        val default = LinkFake("default")
        val linkX = LinkFake("X")
        val linkY = LinkFake("Y")
        mgr.meshBearer = default
        mgr.registerBearer(nodeX.toUShort(), linkX)
        mgr.registerBearer(nodeY.toUShort(), linkY)
        val network = mgr.network!!

        nm.networkLayer.send(heartbeat(network, nodeX), PduType.NETWORK_PDU, 5u)
        nm.networkLayer.send(heartbeat(network, nodeY), PduType.NETWORK_PDU, 5u)
        nm.networkLayer.send(heartbeat(network, 0x0002), PduType.NETWORK_PDU, 5u)

        assertEquals(listOf(nodeX), linkX.destinations(network))
        assertEquals(listOf(nodeY), linkY.destinations(network))
        assertEquals("미등록 목적지는 기본 링크", listOf(0x0002), default.destinations(network))
    }

    // ── 불변 6 (통합) ─────────────────────────────────────────────────────────

    @Test
    fun `two nodes answered concurrently on two links - each request ends only with its own node's response`() = runBlocking {
        val mgr = manager()
        val network = mgr.network!!
        // 앱은 설정 송신마다 로컬 노드 Config Client 처리기를 붙인다(ConfigurationClient.ensureLocalElementHandlers). 없으면 Status 가 UnknownMessage.
        assertTrue(mgr.rehydrateLocalNodeHandlers())
        mgr.meshBearer = LinkFake("default")
        val onOff = SigModelId(0x1000u)
        // 두 노드가 같은 opcode(Model App Bind)에 같은 AppKey·모델로 응답한다 — 요청 파라미터 중 다른 것은 엘리먼트 주소뿐.
        val statusX = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, 0u, UnicastAddress(nodeX.toUShort()), onOff)
        val statusY = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, 0u, UnicastAddress(nodeY.toUShort()), onOff)
        val wireX = response(network, nodeX, statusX, sequence = 0x10u)
        val wireY = response(network, nodeY, statusY, sequence = 0x20u)
        lateinit var linkX: LinkFake
        lateinit var linkY: LinkFake
        // 각 노드는 자기 링크로 응답하고, 이웃이 릴레이해 상대 링크에도 같은 응답이 들어온다(프록시 필터에 프로비저너 주소).
        linkX = LinkFake("X") {
            delay(40); linkX.inject(wireX); delay(15); linkY.inject(wireX)
        }
        linkY = LinkFake("Y") {
            linkX.inject(wireY); delay(10); linkY.inject(wireY)
        }
        mgr.registerBearer(nodeX.toUShort(), linkX)
        mgr.registerBearer(nodeY.toUShort(), linkY)
        val x = network.node(nodeX.toUShort())!!
        val y = network.node(nodeY.toUShort())!!

        val (rx, ry) = listOf(
            async(Dispatchers.Default) {
                withTimeoutOrNull(5_000) { mgr.send(ConfigModelAppBind(0u, UnicastAddress(nodeX.toUShort()), onOff), x, initialTtl = null) }
            },
            async(Dispatchers.Default) {
                withTimeoutOrNull(5_000) { mgr.send(ConfigModelAppBind(0u, UnicastAddress(nodeY.toUShort()), onOff), y, initialTtl = null) }
            },
        ).awaitAll()

        assertTrue("X 요청은 X 의 Status 로 끝난다: $rx", rx is ConfigModelAppStatus && rx.elementAddress.address.toInt() == nodeX)
        assertTrue("Y 요청은 Y 의 Status 로 끝난다: $ry", ry is ConfigModelAppStatus && ry.elementAddress.address.toInt() == nodeY)
        // 요청도 자기 링크로만 나갔다 (불변 3 을 실제 송신 경로에서).
        assertTrue("X 링크엔 X 로 가는 PDU 만: ${linkX.destinations(network)}", linkX.destinations(network).isNotEmpty() && linkX.destinations(network).all { it == nodeX })
        assertTrue("Y 링크엔 Y 로 가는 PDU 만: ${linkY.destinations(network)}", linkY.destinations(network).isNotEmpty() && linkY.destinations(network).all { it == nodeY })
        linkX.close(); linkY.close()
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    /** 보낸 PDU 를 기록하고, 첫 송신 때 [reply] 를 다른 코루틴에서 돌리는 링크. [closed] 면 송신마다 [BearerError.Closed]. */
    private class LinkFake(
        val tag: String,
        private val closed: Boolean = false,
        private val reply: (suspend () -> Unit)? = null,
    ) : MeshBearer {
        val sent = CopyOnWriteArrayList<Pair<ByteArray, PduType>>()
        private val flow = MutableSharedFlow<Pdu>(extraBufferCapacity = 64)
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        override val pdus: Flow<Pdu> = flow
        override val state: StateFlow<BearerEvent> get() = throw UnsupportedOperationException()
        override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.NetworkPdu, PduTypes.MeshBeacon, PduTypes.ProxyConfiguration)
        override val isOpen: Boolean get() = !closed
        override suspend fun open() = Unit
        override suspend fun close() = scope.cancel()
        override suspend fun send(pdu: ByteArray, type: PduType) {
            if (closed) throw BearerError.Closed()
            val first = sent.isEmpty()
            sent += pdu to type
            if (first && reply != null) scope.launch { reply.invoke() }
        }

        suspend fun inject(bytes: ByteArray, type: PduType = PduType.NETWORK_PDU) = flow.emit(Pdu(bytes, type))
        suspend fun inject(beacon: Pair<ByteArray, PduType>) = inject(beacon.first, beacon.second)

        fun sentOfType(type: PduType) = sent.count { it.second == type }

        fun destinations(network: MeshNetwork): List<Int> = sent.filter { it.second == PduType.NETWORK_PDU }
            .mapNotNull { NetworkPduDecoder.decode(it.first, PduType.NETWORK_PDU, network)?.destination?.address?.toInt() }
    }

    /** 노드 → 프로비저너, 노드 DevKey·NetKey 로 암호화한 비분할 Access 메시지의 wire 바이트. */
    private fun response(network: MeshNetwork, from: Int, status: ConfigModelAppStatus, sequence: UInt): ByteArray {
        val node = network.node(from.toUShort())!!
        val netKey = network.networkKeys.first { it.index.toInt() == 0 }
        val access = AccessPdu.init(
            message = status,
            source = from.toUShort(),
            destination = MeshAddress.create(provisionerAddress),
            userInitiated = false,
        )
        val keySet = no.nordicsemi.kotlin.mesh.core.layers.DeviceKeySet.init(netKey, node)!!
        val upper = UpperTransportPdu.init(access, keySet, sequence, network.ivIndex)
        return NetworkPduDecoder.encode(
            lowerTransportPdu = AccessMessage(upper, netKey),
            pduType = PduType.NETWORK_PDU,
            sequence = sequence,
            ttl = 5u,
        ).pdu
    }

    /** 프로비저너 → [dst] Heartbeat 제어 메시지 (App/DevKey 없이 NetKey 만). */
    private fun heartbeat(network: MeshNetwork, dst: Int): LowerTransportPdu = object : LowerTransportPdu {
        override val networkKey: NetworkKey = network.networkKeys.first { it.index.toInt() == 0 }
        override val ivIndex: UInt = network.ivIndex.transmitIvIndex
        override val source: MeshAddress = MeshAddress.create(provisionerAddress)
        override val destination: MeshAddress = MeshAddress.create(dst)
        override val type = LowerTransportPduType.CONTROL_MESSAGE
        override val upperTransportPdu: ByteArray = byteArrayOf(0x05, 0x00, 0x00)
        override val transportPdu: ByteArray = byteArrayOf(0x0A) + upperTransportPdu
    }

    /** 이 망 기본 NetKey 로 인증한 Secure Network Beacon (IV Index = 망의 현재 값). */
    private fun beacon(network: MeshNetwork): Pair<ByteArray, PduType> {
        val key = network.networkKeys.first { it.index.toInt() == 0 }
        val iv = network.ivIndex.index
        val body = byteArrayOf(0x00) + key.networkId +
            byteArrayOf((iv shr 24).toByte(), (iv shr 16).toByte(), (iv shr 8).toByte(), iv.toByte())
        val cmac = Crypto::class.java.getDeclaredMethod("calculateCmac", ByteArray::class.java, ByteArray::class.java)
            .apply { isAccessible = true }
            .invoke(Crypto, body, key.derivatives.beaconKey) as ByteArray
        return (byteArrayOf(0x01) + body + cmac.copyOfRange(0, 8)) to PduType.MESH_BEACON
    }

    private suspend fun waitUntil(ms: Long, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + ms
        while (!condition() && System.currentTimeMillis() < deadline) delay(20)
    }

    private class StubStorage : Storage {
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = Unit
    }

    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) = Unit
        private val seq = java.util.concurrent.atomic.AtomicInteger(0)
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = seq.getAndIncrement().toUInt()
        override suspend fun storeNextSequenceNumber(uuid: Uuid, address: UnicastAddress, sequenceNumber: UInt) = Unit
        override suspend fun resetSequenceNumber(uuid: Uuid, address: UnicastAddress) = Unit
        override suspend fun lastSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(uuid: Uuid, source: UnicastAddress, lastSeqAuth: ULong) = Unit
        override suspend fun previousSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(uuid: Uuid, source: UnicastAddress, seqAuth: ULong) = Unit
        override suspend fun storeLocalProvisioner(uuid: Uuid, localProvisionerUuid: Uuid) = Unit
        override suspend fun localProvisioner(uuid: Uuid): String? = null
    }

    private val cdb: ByteArray = javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()

    private fun manager(): MeshNetworkManager = MeshNetworkManager(
        storage = StubStorage(),
        secureProperties = StubSecureProperties(),
        ioDispatcher = Dispatchers.Default,
    ).also { runBlocking { it.import(cdb) } }
}
