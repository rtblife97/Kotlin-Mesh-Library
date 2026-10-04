package no.nordicsemi.kotlin.mesh.core.layers

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import no.nordicsemi.kotlin.mesh.bearer.AdvertisingBearer
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.access.AccessPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.AccessMessage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.SegmentedAccessMessage
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkPduDecoder
import no.nordicsemi.kotlin.mesh.core.layers.uppertransport.UpperTransportPdu
import no.nordicsemi.kotlin.mesh.core.messages.ConfigMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppBind
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppStatus
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.SigModelId
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-05, 커미셔닝 동글 "얇은 무선" 1단계) — 광고 베어러([AdvertisingBearer]).
 *
 * 광고 베어러에는 프록시 필터가 없어 망의 모든 메시지가 들어온다. 실제 NetKey·AppKey 로 암호화한 **남의 목적지** PDU 를 광고 베어러 수신으로
 * 주입해 두 결함이 재현되지 않음을 본다 (수정 전에는 둘 다 실패한다):
 *  - 결함 1: 남의 목적지 메시지가 해독되어 앱 수신 흐름(`incomingMeshMessages`)으로 나간다.
 *  - 결함 2: 남의 목적지 분할 메시지가 재조립 버퍼(`incompleteSegments`)에 들어가 지워지지 않는다 (Discard 타이머는 우리 목적지만).
 * 같은 바이트를 우리 주소로 보낸 대조군은 그대로 들어온다 (시험이 공허하지 않게).
 *
 * 불변 (등록·해제): 광고 베어러를 노드 엘리먼트 주소마다 목적지별로 등록해도 수신 수집기는 늘지 않는다(수신은 attach 한 번), 그 목적지 송신은
 * 등록한 TTL 로 나가며, 해제하면 경로·TTL 이 사라지고 수신 연결은 남는다.
 */
@OptIn(ExperimentalUuidApi::class)
class AdvertisingBearerTest {

    private val provisionerAddress = 0x0001
    private val nodeX = 0x0007
    private val nodeY = 0x0105

    @Test
    fun `defect 1 - a message to another node heard on the advertising bearer is not delivered to the app`() = runBlocking {
        val mgr = manager()
        val nm = mgr.networkManager!!
        val network = mgr.network!!
        val adv = AdvFake()
        mgr.attachAdvertisingReceiver(adv)
        val received = CopyOnWriteArrayList<Int>()
        val watcher = launch(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) {
            nm.incomingMeshMessages.collect { received += it.destination.address.toInt() }
        }

        adv.inject(appKeyStatus(network, from = nodeX, to = nodeY, sequence = 0x30u))
        adv.inject(appKeyStatus(network, from = nodeX, to = provisionerAddress, sequence = 0x31u))
        waitUntil(2_000) { received.contains(provisionerAddress) }
        delay(200)
        watcher.cancel()

        assertTrue("대조군(우리 주소) 은 들어온다: $received", received.contains(provisionerAddress))
        assertTrue("남의 목적지(0x%04X) 는 앱으로 나가지 않는다: $received".format(nodeY), nodeY !in received)
    }

    @Test
    fun `defect 2 - a partial segmented message to another node does not stay in the reassembly buffer`() = runBlocking {
        val mgr = manager()
        val nm = mgr.networkManager!!
        val network = mgr.network!!
        val adv = AdvFake()
        mgr.attachAdvertisingReceiver(adv)

        adv.inject(firstSegment(network, from = nodeX, to = nodeY, sequence = 0x40u))
        delay(500)
        assertEquals("남의 목적지 분할은 재조립 버퍼에 들어가지 않는다", 0, nm.lowerTransportLayer.incompleteSegmentCount)

        adv.inject(firstSegment(network, from = nodeY, to = provisionerAddress, sequence = 0x50u))
        waitUntil(2_000) { nm.lowerTransportLayer.incompleteSegmentCount == 1 }
        assertEquals("대조군(우리 주소) 분할은 재조립을 기다린다", 1, nm.lowerTransportLayer.incompleteSegmentCount)
    }

    @Test
    fun `per-destination registration of an advertising bearer routes TX with its TTL and adds no RX collector`() = runBlocking {
        val mgr = manager()
        val nm = mgr.networkManager!!
        val network = mgr.network!!
        val adv = AdvFake()
        mgr.attachAdvertisingReceiver(adv)
        val before = nm.rxCollectorCount
        val elements = (nodeX until nodeX + 5).map { it.toUShort() }

        elements.forEach { assertTrue(mgr.registerBearer(it, adv, ttl = 3u)) }

        assertEquals("엘리먼트 5개를 등록해도 수신 수집기는 그대로 (수신 = attach 한 번)", before, nm.rxCollectorCount)
        assertTrue(elements.all { nm.hasRegisteredBearer(it) && nm.ttlOverrideFor(it) == 3.toUByte() })
        // TTL 을 정하지 않은 요청이 등록 TTL 로, 그 목적지 등록 베어러로 나간다.
        withTimeoutOrNull(300) {
            mgr.send(ConfigModelAppBind(0u, UnicastAddress(nodeX.toUShort()), SigModelId(0x1000u)), network.node(nodeX.toUShort())!!, initialTtl = null)
        }
        val sent = adv.sent.mapNotNull { NetworkPduDecoder.decode(it, PduType.NETWORK_PDU, network) }
        assertTrue("동글로 나간 PDU: ${sent.size}", sent.isNotEmpty())
        assertTrue(sent.all { it.destination.address.toInt() == nodeX && it.ttl == 3.toUByte() })

        elements.forEach { mgr.unregisterBearer(it) }
        assertTrue("해제하면 경로·TTL 이 사라진다", elements.none { nm.hasRegisteredBearer(it) || nm.ttlOverrideFor(it) != null })
        assertEquals("수신 연결은 남는다", before, nm.rxCollectorCount)
        mgr.detachAdvertisingReceiver(adv)
        assertEquals(before - 1, nm.rxCollectorCount)
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    /** 광고 베어러 가짜 — 보낸 PDU 를 기록하고, [inject] 로 공중 수신을 흉내 낸다. */
    private class AdvFake : AdvertisingBearer {
        val sent = CopyOnWriteArrayList<ByteArray>()
        private val flow = MutableSharedFlow<Pdu>(extraBufferCapacity = 64)
        override val pdus: Flow<Pdu> = flow
        override val state: StateFlow<BearerEvent> = MutableStateFlow(BearerEvent.Opened)
        override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.NetworkPdu, PduTypes.MeshBeacon)
        override val isOpen: Boolean = true
        override suspend fun open() = Unit
        override suspend fun close() = Unit
        override suspend fun send(pdu: ByteArray, type: PduType) {
            sent += pdu
        }

        suspend fun inject(bytes: ByteArray) = flow.emit(Pdu(bytes, PduType.NETWORK_PDU))
    }

    /** [from] → [to] Config Model App Status 를 **앱 키**로 암호화한 비분할 Access 메시지 (앱 키 메시지는 목적지와 무관하게 해독된다). */
    private fun appKeyStatus(network: MeshNetwork, from: Int, to: Int, sequence: UInt): ByteArray {
        val appKey = network.applicationKeys.first()
        val status = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, 0u, UnicastAddress(from.toUShort()), SigModelId(0x1000u))
        val access = AccessPdu.init(message = status, source = from.toUShort(), destination = MeshAddress.create(to), userInitiated = false)
        val upper = UpperTransportPdu.init(access, AccessKeySet(appKey), sequence, network.ivIndex)
        return NetworkPduDecoder.encode(
            lowerTransportPdu = AccessMessage(upper, appKey.boundNetworkKey),
            pduType = PduType.NETWORK_PDU,
            sequence = sequence,
            ttl = 4u,
        ).pdu
    }

    /** 같은 메시지를 분할(2조각)로 만들고 첫 조각만 — 나머지를 못 들은 경우. */
    private fun firstSegment(network: MeshNetwork, from: Int, to: Int, sequence: UInt): ByteArray {
        val appKey = network.applicationKeys.first()
        val status = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, 0u, UnicastAddress(from.toUShort()), SigModelId(0x1000u))
        val access = AccessPdu.init(message = status, source = from.toUShort(), destination = MeshAddress.create(to), userInitiated = false)
        val upper = UpperTransportPdu.init(access, AccessKeySet(appKey), sequence, network.ivIndex)
        val segment = SegmentedAccessMessage.init(pdu = upper, networkKey = appKey.boundNetworkKey, offset = 0u)
        assertTrue("두 조각 이상이어야 한다", segment.lastSegmentNumber >= 1u)
        return NetworkPduDecoder.encode(lowerTransportPdu = segment, pduType = PduType.NETWORK_PDU, sequence = sequence, ttl = 4u).pdu
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
