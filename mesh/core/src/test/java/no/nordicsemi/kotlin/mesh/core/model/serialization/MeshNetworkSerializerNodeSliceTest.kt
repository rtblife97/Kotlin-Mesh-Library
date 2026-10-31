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

    /**
     * 2026-10-31 — 저장·export 를 스트리밍(노드마다 바로 쓰기)으로 바꾼 뒤에도 파일 바이트가 종전과 같다. 저장 파일·서버 업로드·앱 후처리가
     * 이 바이트를 읽으므로 형식이 한 글자도 바뀌면 안 된다. 시험용 CDB(노드 모양 여러 가지) + emblaze 조명 모양 + 한글 이름.
     */
    @Test
    fun `스트리밍 저장·export 바이트 = 종전 바이트`() = runBlocking {
        val shapes = listOf(fixture, synthesized(nodeCount = 40), EmblazeCdbFixture.network(nodeCount = 60), withKoreanNames(fixture))
        for (cdb in shapes) {
            val storage = NullStorage()
            val mgr = manager(cdb = cdb, storage = storage)
            val network = mgr.network!!
            val legacy = legacySerialize(network).toByteArray()

            val streamed = java.io.ByteArrayOutputStream().also { MeshNetworkSerializer.writeWithCdbLock(network, it) }.toByteArray()
            assertEquals(String(legacy), String(streamed))
            assertTrue(legacy.contentEquals(streamed))
            assertTrue(legacy.contentEquals(mgr.export()!!))
            // 저장 경로: 기본 saveStreaming(바이트로 모아 save) 으로 저장소에 들어간 바이트.
            mgr.save()
            assertTrue(legacy.contentEquals(storage.last.get()!!))
            // 노드 하나 인코딩 = 트리 인코딩의 toString.
            for (node in network.nodes) {
                assertEquals(MeshNetworkSerializer.encodeNode(node).toString(), MeshNetworkSerializer.encodeNodeToString(node))
            }
        }
    }

    /**
     * 2026-10-31 — 트리 없는 디코드(올리기·들여오기) = 종전 디코드(트리 경유). 같은 망이 나와야 한다 (다시 내보낸 바이트로 비교).
     * 벤더 서버 형식 CDB 도 보려면 `MESH_VENDOR_CDB=<파일>` (저장소 밖 파일도 됨, 시험은 내용을 출력하지 않는다).
     */
    @Test
    fun `트리 없는 디코드 = 종전 디코드`() {
        val vendor = System.getenv("MESH_VENDOR_CDB")?.let { java.io.File(it).readBytes() }
        val shapes = listOfNotNull(fixture, synthesized(nodeCount = 40), EmblazeCdbFixture.network(nodeCount = 60), withKoreanNames(fixture), vendor)
        for (cdb in shapes) {
            val streamed = MeshNetworkSerializer.deserialize(cdb)
            val viaTree = MeshNetworkSerializer.deserializeViaTree(cdb)
            assertEquals(legacySerialize(viaTree), legacySerialize(streamed))
        }
        // 검사도 같다: 스키마·판이 틀리면 둘 다 거부.
        val badSchema = String(fixture).replaceFirst("http://json-schema.org/draft-04/schema#", "x").toByteArray()
        val badVersion = String(fixture).replaceFirst("\"version\":\"1.0.0\"", "\"version\":\"1.0\"").replaceFirst("\"version\": \"1.0.0\"", "\"version\": \"1.0\"").toByteArray()
        for (bad in listOf(badSchema, badVersion)) {
            assertTrue(runCatching { MeshNetworkSerializer.deserialize(bad) }.isFailure)
            assertTrue(runCatching { MeshNetworkSerializer.deserializeViaTree(bad) }.isFailure)
        }
    }

    /**
     * 2026-10-31 — 저장 1회의 순간 메모리가 출력 크기의 작은 배수로 묶인다. 종전(망 전체 트리 → 문자열 → 바이트)은 출력의 약 75배를 할당했고
     * (1,400노드 243 MB) 폰 힙 한도 가까이에서 OOM 이 났다 (2026-10-30 Flip7). 할당 바이트는 HotSpot 스레드 계측값.
     */
    @Test
    fun `저장 1회 할당이 출력의 수십 배를 넘지 않는다`() = runBlocking {
        val threadMx = java.lang.management.ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
        val storage = object : Storage {
            var size = 0L
            override suspend fun load(): ByteArray = ByteArray(0)
            override suspend fun save(network: ByteArray) = Unit
            override suspend fun saveStreaming(writer: suspend (java.io.OutputStream) -> Unit) {
                val counting = object : java.io.OutputStream() {
                    override fun write(b: Int) { size++ }
                    override fun write(b: ByteArray, off: Int, len: Int) { size += len }
                }
                writer(counting)
            }
        }
        val mgr = manager(cdb = EmblazeCdbFixture.network(nodeCount = 600), storage = storage)
        mgr.save() // 예열 (클래스 적재)
        storage.size = 0
        val tid = Thread.currentThread().id
        val before = threadMx.getThreadAllocatedBytes(tid)
        mgr.save()
        val allocated = threadMx.getThreadAllocatedBytes(tid) - before
        val ratio = allocated.toDouble() / storage.size
        assertTrue("저장 1회 할당 ${allocated / 1_000_000} MB / 출력 ${storage.size / 1000} KB = ${"%.1f".format(ratio)}배", ratio < 30)
    }

    /** 2026-10-31 — 쓰는 도중 저장소가 실패하면 저장은 실패로 끝나고 미저장 표시가 남는다 (다음 저장이 다시 쓴다). */
    @Test
    fun `스트리밍 저장 도중 실패 - 미저장 표시가 남는다`() = runBlocking {
        val storage = object : Storage {
            var fail = true
            override suspend fun load(): ByteArray = ByteArray(0)
            override suspend fun save(network: ByteArray) = Unit
            override suspend fun saveStreaming(writer: suspend (java.io.OutputStream) -> Unit) {
                val out = object : java.io.OutputStream() {
                    var written = 0
                    override fun write(b: Int) { write(byteArrayOf(b.toByte()), 0, 1) }
                    override fun write(b: ByteArray, off: Int, len: Int) {
                        written += len
                        if (fail && written > 4096) throw java.io.IOException("disk full (시험)")
                    }
                }
                writer(out)
            }
        }
        val mgr = manager(cdb = EmblazeCdbFixture.network(nodeCount = 50), storage = storage)
        val failed = runCatching { mgr.save() }.exceptionOrNull()
        assertTrue("저장 실패가 올라와야 한다", failed is java.io.IOException)
        assertTrue("실패한 저장 뒤에도 미저장 표시", mgr.hasUnsavedChanges)
        storage.fail = false
        assertTrue(mgr.flushPendingSave())
        assertTrue(!mgr.hasUnsavedChanges)
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

    /** 망·그룹·노드 이름을 한글로 (UTF-8 다바이트·따옴표·역슬래시 이스케이프 포함). */
    private fun withKoreanNames(cdb: ByteArray): ByteArray {
        val root = Json.parseToJsonElement(String(cdb)).jsonObject
        fun renamed(list: JsonElement, prefix: String) = JsonArray(list.jsonArray.mapIndexed { i, e ->
            JsonObject(e.jsonObject.toMutableMap().apply { put("name", JsonPrimitive("$prefix $i \"따옴표\" \\ 끝")) })
        })
        return JsonObject(root.toMutableMap().apply {
            put("meshName", JsonPrimitive("신동 광산 3층 현장"))
            put("groups", renamed(root["groups"]!!, "구역"))
            put("nodes", renamed(root["nodes"]!!, "조명"))
        }).toString().toByteArray()
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
