package no.nordicsemi.kotlin.mesh.core.layers

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.MeshBearer
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.core.JournalStorage
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.PersistenceDiagnostics
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.access.AccessPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.AccessMessage
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
import no.nordicsemi.kotlin.mesh.core.model.serialization.EmblazeCdbFixture
import no.nordicsemi.kotlin.mesh.core.model.serialization.MeshNetworkSerializer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-31) — 노드 단위 증분 저장(저널)의 불변.
 *
 *  1. 저장 파일(저널)에서 다시 올린 망 = 저장 순간의 메모리 망 (export 바이트 단위). 노드 이름·구독·제외 표시·추가·삭제·지웠다 다시
 *     넣기·망 이름(껍데기) 이 섞여도.
 *  2. 그 저장들은 증분이다 — 망 전체를 다시 쓰지 않는다 (전체 저장 수 그대로, 덧붙인 양이 스냅숏보다 훨씬 작다).
 *  3. 실제 설정 응답(노드 DevKey·NetKey 로 암호화한 Config Status 를 수신 경로로) 이 바꾼 노드가 증분 저장에 들어간다.
 *  4. 덧붙이는 도중 끊긴 묶음은 버리고 앞 묶음까지 올린다.
 *  5. 저널이 스냅숏보다 커지면 전체 스냅숏으로 압축한다. 기록이 놓친 변경도 [MeshNetworkManager.fullSaveMaxInterval] 의 전체 저장이 덮는다.
 */
@OptIn(ExperimentalUuidApi::class)
class NodeJournalSaveTest {

    @Test
    fun `증분 저장을 섞어도 다시 올린 망 = 메모리 망`() = runBlocking {
        val storage = MemoryJournalStorage()
        // 압축 기준을 스냅숏 크기만으로 (작은 망에서도 "스냅숏 직후 매 저장 압축" 같은 결함이 드러나게).
        val mgr = manager(EmblazeCdbFixture.network(nodeCount = 60), storage).apply { journalMinCompactionBytes = 0 }
        val network = mgr.network!!
        mgr.save() // 들여온 망 → 전체 스냅숏
        val snapshotBytes = storage.size()
        assertReloadEquals(mgr, storage)

        val mark = PersistenceDiagnostics.mark()
        val nodes = network.nodes
        val steps: List<Pair<String, () -> Unit>> = listOf(
            "노드 이름" to { nodes[12].name = "조명 \"12\" 이름 바꿈 \\ 끝" },
            "모델 구독 지우기" to { nodes[20].elements[0].models[3].unsubscribeFromAll() },
            "제외 표시" to { nodes[30].excluded = true },
            "노드 추가" to { network.add(cloneOf(nodes[5], address = 0x7000)) },
            "노드 삭제" to { network.remove(nodes[40].uuid) },
            // 같은 UUID 를 지웠다 다시 넣으면 끝으로 간다 (옛 주소는 제외 목록에 있어 새 주소로).
            "지웠다 다시 넣기" to { val n = nodes[7]; network.remove(n.uuid); network.add(cloneOf(n, address = 0x7020, keepUuid = true)) },
            "망 이름(껍데기)" to { network.name = "신동 3층 망" },
            "같은 묶음에 여럿" to {
                nodes[1].name = "한 묶음 1"
                network.add(cloneOf(nodes[2], address = 0x7010))
                nodes[3].name = "한 묶음 3"
            },
        )
        for ((label, mutate) in steps) {
            val before = String(mgr.export()!!)
            mutate()
            assertNotEquals("$label: 메모리 망이 바뀌어야 시험이 된다", before, String(mgr.export()!!))
            mgr.save()
            assertReloadEquals(mgr, storage, label)
        }
        val window = PersistenceDiagnostics.since(mark)
        assertEquals("모두 증분 저장", 0L, window.fullSaves)
        assertEquals(steps.size.toLong(), window.saves)
        val appended = storage.size() - snapshotBytes
        assertTrue("덧붙인 양 $appended B 는 스냅숏 $snapshotBytes B 보다 훨씬 작다", appended < snapshotBytes / 4)
    }

    @Test
    fun `실제 설정 응답이 바꾼 노드가 증분 저장에 들어간다`() = runBlocking {
        val storage = MemoryJournalStorage()
        val mgr = manager(fixture, storage)
        val network = mgr.network!!
        mgr.save()
        assertTrue(mgr.rehydrateLocalNodeHandlers())
        mgr.meshBearer = LinkFake()
        val x = network.node(nodeX.toUShort())!!
        val onOff = SigModelId(0x1000u)
        val before = String(mgr.export()!!)
        val mark = PersistenceDiagnostics.mark()

        // AppKey 3 바인딩 응답 — 시험용 CDB 의 이 모델은 아직 3 에 묶여 있지 않다 (lib 처리기가 노드에 바인딩을 더한다).
        val status = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, 3u, UnicastAddress(nodeX.toUShort()), onOff)
        val wire = response(network, nodeX, status, sequence = 0x200u)
        lateinit var link: LinkFake
        link = LinkFake { link.inject(wire) }
        mgr.registerBearer(nodeX.toUShort(), link)
        val r = withTimeoutOrNull(5_000) { mgr.send(ConfigModelAppBind(3u, UnicastAddress(nodeX.toUShort()), onOff), x, initialTtl = null) }
        mgr.unregisterBearer(nodeX.toUShort())
        link.close()
        assertTrue("응답을 받아야 한다: $r", r is ConfigModelAppStatus)
        assertNotEquals("응답이 노드를 바꿨어야 한다", before, String(mgr.export()!!))

        mgr.save()
        assertEquals("증분 저장", 0L, PersistenceDiagnostics.since(mark).fullSaves)
        assertReloadEquals(mgr, storage)
    }

    @Test
    fun `덧붙이다 끊긴 묶음은 버리고 앞 묶음까지 올린다`() = runBlocking {
        val storage = MemoryJournalStorage()
        val mgr = manager(EmblazeCdbFixture.network(nodeCount = 20), storage)
        val network = mgr.network!!
        mgr.save()
        network.nodes[3].name = "끊기기 전"
        mgr.save()
        val committed = String(mgr.export()!!)
        val good = storage.size()

        network.nodes[4].name = "끊긴 묶음"
        mgr.save()
        // 마지막 묶음의 앞 절반만 쓰인 채 프로세스가 죽었다.
        storage.truncate(good + (storage.size() - good) / 2)
        val reloaded = manager(null, storage).apply { assertTrue(load()) }
        assertEquals(committed, String(reloaded.export()!!))
    }

    @Test
    fun `저널이 스냅숏보다 커지면 전체 스냅숏으로 압축`() = runBlocking {
        val storage = MemoryJournalStorage()
        val mgr = manager(EmblazeCdbFixture.network(nodeCount = 10), storage).apply { journalMinCompactionBytes = 0 }
        val network = mgr.network!!
        mgr.save()
        val snapshot = storage.size()
        val mark = PersistenceDiagnostics.mark()
        var i = 0
        while (PersistenceDiagnostics.since(mark).fullSaves == 0L && i < 200) {
            network.nodes[i % 10].name = "압축 $i"
            mgr.save()
            i++
        }
        assertEquals("저널이 스냅숏 크기에 이르면 한 번 압축", 1L, PersistenceDiagnostics.since(mark).fullSaves)
        assertTrue("압축 뒤 저널 ≈ 스냅숏 크기", storage.size() < snapshot * 2)
        assertReloadEquals(mgr, storage)
    }

    @Test
    fun `기록이 놓친 노드 변경도 전체 저장 주기가 덮는다`() = runBlocking {
        val storage = MemoryJournalStorage()
        val mgr = manager(EmblazeCdbFixture.network(nodeCount = 10), storage)
        val network = mgr.network!!
        mgr.save()
        // 바뀐 노드 기록을 거치지 않는 변경 (내부 필드 직접) — 증분 저장은 이것을 모른다.
        network.nodes[2].features._proxy = no.nordicsemi.kotlin.mesh.core.model.Proxy(
            state = no.nordicsemi.kotlin.mesh.core.model.FeatureState.Disabled,
        )
        network.nodes[5].name = "기록된 변경"
        mgr.save()
        val reloadedMissed = manager(null, storage).apply { assertTrue(load()) }
        assertNotEquals("놓친 변경은 증분에 없다 (안전망이 필요한 이유)", String(mgr.export()!!), String(reloadedMissed.export()!!))

        mgr.fullSaveMaxInterval = Duration.ZERO
        mgr.save()
        assertReloadEquals(mgr, storage)
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    private val provisionerAddress = 0x0001
    private val nodeX = 0x0007
    private val fixture: ByteArray = javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()

    private suspend fun assertReloadEquals(mgr: MeshNetworkManager, storage: MemoryJournalStorage, label: String = "") {
        val reloaded = manager(null, storage)
        assertTrue("$label: 저널에서 올려야 한다", reloaded.load())
        assertEquals("$label: 다시 올린 망 = 메모리 망", String(mgr.export()!!), String(reloaded.export()!!))
    }

    /** [source] 노드를 새 UUID·주소로 복제 (같은 직렬화기로 왕복). */
    private fun cloneOf(source: no.nordicsemi.kotlin.mesh.core.model.Node, address: Int, keepUuid: Boolean = false) =
        MeshNetworkSerializer.encodeNodeToString(source)
            .let { if (keepUuid) it else it.replace(source.uuid.toString().uppercase(), Uuid.random().toString().uppercase()) }
            .replace("\"unicastAddress\":\"%04X\"".format(source.primaryUnicastAddress.address.toInt()), "\"unicastAddress\":\"%04X\"".format(address))
            .let { MeshNetworkSerializer.decodeNode(it) }

    private fun manager(cdb: ByteArray?, storage: Storage): MeshNetworkManager = MeshNetworkManager(
        storage = storage,
        secureProperties = StubSecureProperties(),
        ioDispatcher = Dispatchers.Default,
    ).also { m -> if (cdb != null) runBlocking { m.import(cdb) } }

    /** 메모리 저널 저장소 (앱 MeshNetworkStorage 의 파일 저널과 같은 계약). */
    private class MemoryJournalStorage : Storage, JournalStorage {
        @Volatile private var journal: ByteArray? = null
        fun size() = journal?.size?.toLong() ?: 0L
        fun truncate(to: Long) { journal = journal!!.copyOf(to.toInt()) }
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = Unit
        override suspend fun openJournal(): InputStream? = journal?.let { ByteArrayInputStream(it) }
        override suspend fun replaceJournal(writer: suspend (OutputStream) -> Unit) {
            val out = ByteArrayOutputStream()
            writer(out)
            journal = out.toByteArray()
        }
        override suspend fun appendJournal(batch: ByteArray) {
            journal = (journal ?: ByteArray(0)) + batch
        }
        override suspend fun journalSize(): Long = size()
    }

    /** 첫 송신 때 [reply] 를 다른 코루틴에서 돌리는 링크 (lib 수신 경로로 응답을 흘린다). */
    private class LinkFake(private val reply: (suspend () -> Unit)? = null) : MeshBearer {
        private val flow = MutableSharedFlow<Pdu>(extraBufferCapacity = 64)
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        private val sent = AtomicInteger()
        override val pdus: Flow<Pdu> = flow
        override val state: StateFlow<BearerEvent> get() = throw UnsupportedOperationException()
        override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.NetworkPdu, PduTypes.MeshBeacon, PduTypes.ProxyConfiguration)
        override val isOpen: Boolean = true
        override suspend fun open() = Unit
        override suspend fun close() = scope.cancel()
        override suspend fun send(pdu: ByteArray, type: PduType) {
            if (sent.getAndIncrement() == 0 && reply != null) scope.launch { reply.invoke() }
        }

        suspend fun inject(bytes: ByteArray) = flow.emit(Pdu(bytes, PduType.NETWORK_PDU))
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
        val keySet = DeviceKeySet.init(netKey, node)!!
        val upper = UpperTransportPdu.init(access, keySet, sequence, network.ivIndex)
        return NetworkPduDecoder.encode(
            lowerTransportPdu = AccessMessage(upper, netKey),
            pduType = PduType.NETWORK_PDU,
            sequence = sequence,
            ttl = 5u,
        ).pdu
    }

    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) = Unit
        private val seq = AtomicInteger(0)
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
}
