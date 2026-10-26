package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.yield
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.PersistenceDiagnostics
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.NetworkConfiguration
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-26, #80) — 망 직렬화를 노드 단위로 쪼갠 뒤에도 지켜야 할 것.
 *
 * 1. 출력은 쪼개기 전(망 전체를 한 번에 인코딩)과 바이트 단위로 같다 — 저장 파일·서버 업로드·앱 후처리가 이 문자열을 읽는다.
 * 2. 노드 1대 export 는 전체 export 의 `nodes[]` 에서 그 노드를 꺼낸 것과 같다 — 서버 1008·WAL 노드 upsert 의 payload 형식.
 * 3. export → import → export 가 같다 (저장 파일로 되살린 망이 같은 파일을 낸다).
 * 4. 저장이 도는 동안에도 CDB 잠금을 기다리는 쪽(설정 응답 반영)이 노드 사이에 들어온다 — 종전엔 저장 1회 내내 막혔다
 *    (2026-10-26 S21U 화면 끔 600노드: 응답 틈 4.0 s 가 합침 저장 주기마다).
 */
@OptIn(ExperimentalUuidApi::class)
class MeshNetworkSerializerNodeSliceTest {

    /** MeshNetworkSerializer 와 같은 설정 — 쪼개기 전 방식(망 통째 인코딩)을 재현해 비교한다. */
    private val legacyJson = Json {
        encodeDefaults = true
        explicitNulls = false
        ignoreUnknownKeys = true
    }

    private fun legacySerialize(network: MeshNetwork): String = network.withNodesLock {
        val header = mapOf<String, JsonElement>(
            "\$schema" to JsonPrimitive("http://json-schema.org/draft-04/schema#"),
            "id" to JsonPrimitive("http://www.bluetooth.com/specifications/assigned-numbers/meshprofile/cdb-schema.json#"),
            "version" to JsonPrimitive("1.0.0"),
        )
        JsonObject(header + legacyJson.encodeToJsonElement(network).jsonObject).toString()
    }

    @Test
    fun `노드 단위 직렬화 출력 = 망 통째 인코딩 출력 (두 경로 모두)`() = runBlocking {
        val network = manager(cdb = fixture).network!!
        val legacy = legacySerialize(network)

        assertEquals(legacy, MeshNetworkSerializer.serialize(network, NetworkConfiguration.Full).toString())
        assertEquals(legacy, MeshNetworkSerializer.serializeWithCdbLock(network).toString())
        // 노드가 많은 망에서도 같다 (복제 노드 = 모델·구독·바인딩 있는 노드).
        val big = manager(cdb = synthesized(nodeCount = 120)).network!!
        assertEquals(legacySerialize(big), MeshNetworkSerializer.serializeWithCdbLock(big).toString())
    }

    @Test
    fun `노드 1대 export = 전체 export 의 그 노드`() = runBlocking {
        val mgr = manager(cdb = fixture)
        val full = Json.parseToJsonElement(String(mgr.export()!!)).jsonObject["nodes"]!!.jsonArray
        assertTrue(full.size >= 10)
        for (element in full) {
            val uuid = element.jsonObject["UUID"]!!.jsonPrimitive.content
            assertEquals(element.toString(), mgr.exportNode(Uuid.parse(uuid)))
        }
        assertNull(mgr.exportNode(Uuid.random()))
    }

    @Test
    fun `export - import - export 가 같다`() = runBlocking {
        val first = manager(cdb = synthesized(nodeCount = 60)).export()!!
        val second = manager(cdb = first).export()!!
        assertEquals(String(first), String(second))
    }

    @Test
    fun `저장 중에도 CDB 잠금 대기자가 노드 사이에 들어온다`() = runBlocking {
        val exporting = AtomicBoolean(false)
        val storage = object : Storage {
            override suspend fun load(): ByteArray = ByteArray(0)
            // 쓰기가 불리면 export 는 끝났다.
            override suspend fun save(network: ByteArray) = exporting.set(false)
        }
        val mgr = manager(cdb = synthesized(nodeCount = 300), storage = storage)
        val network = mgr.network!!
        val acquiredDuringExport = AtomicInteger(0)
        // 잠금을 먼저 쥐고 저장을 띄워, 저장이 잠금 앞에 줄을 선 뒤에 놓는다 (그 전의 획득을 세지 않게).
        network.cdbMutex.lock()
        val saving = launch(Dispatchers.Default) { mgr.save() }
        delay(100)
        exporting.set(true)
        network.cdbMutex.unlock()
        // 설정 응답 반영(AccessLayer) 흉내 — 저장이 끝날 때까지 CDB 잠금을 짧게 잡았다 놓기를 되풀이.
        val probe = launch(Dispatchers.Default) {
            while (isActive && exporting.get()) {
                network.cdbMutex.withLock { if (exporting.get()) acquiredDuringExport.incrementAndGet() }
                yield()
            }
        }
        saving.join()
        probe.cancelAndJoin()
        // 종전(`withCdbLock { export() }`)이면 0~2 (저장이 export 를 끝낼 때까지 못 들어온다). 노드 단위면 노드 수에 가깝다.
        assertTrue("저장 중 잠금 획득 ${acquiredDuringExport.get()}회", acquiredDuringExport.get() >= 30)
    }

    /**
     * 벤치 (단언 없음 — 숫자를 남긴다, 평소엔 건너뜀). 2000노드 망에서 종전 방식이 잠금을 쥔 시간(망 통째 인코딩 1회) 과 지금 저장이
     * 한 번에 쥔 가장 긴 시간. `MESH_SERIALIZER_BENCH=1 ./gradlew :Kotlin-Mesh-Library:mesh:core:test --tests '*NodeSliceTest*' -i | grep BENCH`.
     */
    @Test
    fun `bench 2000노드 잠금 보유 시간`() = runBlocking {
        assumeTrue(System.getenv("MESH_SERIALIZER_BENCH") != null)
        val mgr = manager(cdb = synthesized(nodeCount = 2000))
        val network = mgr.network!!
        repeat(2) { legacySerialize(network); mgr.save() } // 예열
        val legacyMs = (0 until 3).minOf {
            val t0 = System.nanoTime(); legacySerialize(network); (System.nanoTime() - t0) / 1e6
        }
        val mark = PersistenceDiagnostics.mark()
        repeat(3) { mgr.save() }
        val window = PersistenceDiagnostics.since(mark)
        println(
            "BENCH nodes=${network.nodes.size} bytes=${mgr.export()!!.size} 종전_잠금_ms=${"%.1f".format(legacyMs)} " +
                "지금_한번최장_ms=${"%.2f".format(window.lockMaxMs.min())} 지금_누적_ms=${"%.1f".format(window.lockMs.min())} " +
                "export_ms=${"%.1f".format(window.exportMs.min())}"
        )
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    private val fixture: ByteArray =
        javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()

    /** 시험용 CDB 의 노드(모델·구독·바인딩 있는 것)를 복제해 [nodeCount] 개를 더한 CDB. 주소는 0x1000 부터 4칸씩. */
    private fun synthesized(nodeCount: Int): ByteArray {
        val root = Json.parseToJsonElement(String(fixture)).jsonObject
        val templates = root["nodes"]!!.jsonArray.map { it.jsonObject }
            .filter { node -> node["elements"]!!.jsonArray.sumOf { it.jsonObject["models"]!!.jsonArray.size } >= 10 }
        val clones = (0 until nodeCount).map { i ->
            val template = templates[i % templates.size]
            JsonObject(template.toMutableMap().apply {
                put("UUID", JsonPrimitive(Uuid.random().toString().uppercase()))
                put("name", JsonPrimitive("Clone $i"))
                put("unicastAddress", JsonPrimitive("%04X".format(0x1000 + i * 4)))
            })
        }
        val nodes = JsonArray(root["nodes"]!!.jsonArray + clones)
        return JsonObject(root.toMutableMap().apply { put("nodes", nodes) }).toString().toByteArray()
    }

    private fun manager(cdb: ByteArray, storage: Storage = NullStorage()): MeshNetworkManager =
        MeshNetworkManager(
            storage = storage,
            secureProperties = StubSecureProperties(),
            ioDispatcher = Dispatchers.Default,
        ).also { runBlocking { it.import(cdb) } }

    private class NullStorage : Storage {
        val last = AtomicReference<ByteArray?>(null)
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = last.set(network)
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
