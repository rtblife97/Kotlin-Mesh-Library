package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.NodeIdentityDiagnostics
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.util.nodeIdentity
import no.nordicsemi.kotlin.mesh.crypto.Crypto
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test
import kotlin.random.Random
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-26, #80 ⑤) — Node Identity 해석([MeshNetwork.node] NodeIdentity 판).
 *
 * 1. 같은 광고에 같은 노드가 맞춰진다 — 광고는 종전 경로(JCA ECB, [Crypto.calculateECB])로 만든다.
 * 2. 노드 잠금은 노드 목록 스냅샷에만 쓴다: 광고 여러 건 해석 동안 잠금 시간 합이 해시 대조 시간 합의 작은 일부다
 *    (종전엔 대조 전체가 잠금 안 — 597노드 S21U 에서 1건 130~450 ms 동안 수신·송신의 노드 찾기가 섰다).
 */
@OptIn(ExperimentalUuidApi::class)
class NodeIdentityResolveTest {

    /** 종전 해시: e(IdentityKey, data) 의 하위 8바이트, JCA ECB 로. */
    private fun legacyHash(data: ByteArray, identityKey: ByteArray): ByteArray =
        Crypto.calculateECB(data, identityKey).copyOfRange(8, 16)

    /** [node] 가 낼 Node Identity 광고 서비스 데이터 (type 0x01 + hash 8 + random 8). */
    private fun publicAdvert(node: Node, random: ByteArray): ByteArray {
        val key = node.networkKeys.first().derivatives.identityKey
        val data = ByteArray(6) + random + node.primaryUnicastAddress.address.toByteArray()
        return byteArrayOf(0x01) + legacyHash(data, key) + random
    }

    /** Private Node Identity (type 0x03): Padding(5) || 0x03 || Random || Address — Mesh Protocol 1.1 §7.2.2.2.4. */
    private fun privateAdvert(node: Node, random: ByteArray): ByteArray {
        val key = node.networkKeys.first().derivatives.identityKey
        val data = ByteArray(5) + byteArrayOf(0x03) + random + node.primaryUnicastAddress.address.toByteArray()
        return byteArrayOf(0x03) + legacyHash(data, key) + random
    }

    @Test
    fun `같은 광고에 같은 노드가 맞춰진다 - 망 전체와 노드 1대 판 모두`() {
        val network = manager(synthesized(nodeCount = 40)).network!!
        val nodes = network.nodes
        for (node in nodes) {
            val random = Random.nextBytes(8)
            val identity = publicAdvert(node, random).nodeIdentity()!!
            assertSame(node, network.node(identity))
            assertTrue(identity.matches(node))
            // 다른 노드에는 안 맞는다.
            assertTrue(nodes.filter { it !== node }.none { identity.matches(it) })
            // random 이 다르면 (= 해시가 그 random 의 것이 아니면) 어느 노드에도 안 맞는다.
            val forged = (publicAdvert(node, random).copyOfRange(0, 9) + Random.nextBytes(8)).nodeIdentity()!!
            assertNull(network.node(forged))

            val private = privateAdvert(node, random).nodeIdentity()!!
            assertSame(node, network.node(private))
        }
    }

    @Test
    fun `해석 동안 노드 잠금은 스냅샷에만 쓴다`() {
        val network = manager(synthesized(nodeCount = 2000)).network!!
        val miss = (byteArrayOf(0x01) + Random.nextBytes(16)).nodeIdentity()!!
        repeat(5) { network.node(miss) } // 예열
        val mark = NodeIdentityDiagnostics.mark()
        repeat(50) { assertNull(network.node(miss)) }
        val window = NodeIdentityDiagnostics.since(mark)
        assertEquals(50L, window.resolves)
        // 종전 구현이면 잠금 시간 = 대조 시간 전체. 지금은 2000개 참조 복사뿐이다.
        assertTrue("잠금 ${window.lockMs} ms / 대조 ${window.matchMs} ms", window.lockMs < window.matchMs * 0.25)
    }

    /**
     * 벤치 (단언 없음, 평소엔 건너뜀). 2000노드에서 못 맞추는 광고 1건(= 모든 노드 대조) 해석 시간: 종전(노드 잠금 안, 노드마다
     * JCA Cipher) 대 지금(잠금 밖, 키 스케줄 재사용). `MESH_SERIALIZER_BENCH=1 ... --tests '*NodeIdentityResolveTest*' -i | grep BENCH`.
     */
    @Test
    fun `bench 2000노드 Node Identity 해석`() {
        assumeTrue(System.getenv("MESH_SERIALIZER_BENCH") != null)
        val network = manager(synthesized(nodeCount = 2000)).network!!
        val random = Random.nextBytes(8)
        val hash = Random.nextBytes(8)
        val miss = (byteArrayOf(0x01) + hash + random).nodeIdentity()!!
        fun legacy(): Node? = network.withNodesLock {
            network.nodes.find { node ->
                val data = ByteArray(6) + random + node.primaryUnicastAddress.address.toByteArray()
                node.networkKeys.any { legacyHash(data, it.derivatives.identityKey).contentEquals(hash) }
            }
        }
        repeat(5) { legacy(); network.node(miss) }
        fun timeMs(block: () -> Unit): Double = (0 until 10).minOf { val t0 = System.nanoTime(); block(); (System.nanoTime() - t0) / 1e6 }
        val legacyMs = timeMs { legacy() }
        val nowMs = timeMs { network.node(miss) }
        val mark = NodeIdentityDiagnostics.mark()
        network.node(miss)
        val w = NodeIdentityDiagnostics.since(mark)
        println(
            "BENCH node_identity nodes=${network.nodes.size} 종전_잠금안_ms=${"%.2f".format(legacyMs)} " +
                "지금_ms=${"%.2f".format(nowMs)} 지금_잠금_ms=${"%.3f".format(w.lockMs)}"
        )
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    private val fixture: ByteArray =
        javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()

    /** 시험용 CDB 의 노드를 복제해 [nodeCount] 개를 더한 CDB. 주소는 0x1000 부터 4칸씩. */
    private fun synthesized(nodeCount: Int): ByteArray {
        val root = Json.parseToJsonElement(String(fixture)).jsonObject
        val templates = root["nodes"]!!.jsonArray.map { it.jsonObject }
        val clones = (0 until nodeCount).map { i ->
            JsonObject(templates[i % templates.size].toMutableMap().apply {
                put("UUID", JsonPrimitive(Uuid.random().toString().uppercase()))
                put("unicastAddress", JsonPrimitive("%04X".format(0x1000 + i * 4)))
            })
        }
        val nodes = JsonArray(root["nodes"]!!.jsonArray + clones)
        return JsonObject(root.toMutableMap().apply { put("nodes", nodes) }).toString().encodeToByteArray()
    }

    private fun manager(cdb: ByteArray): MeshNetworkManager = MeshNetworkManager(
        storage = object : Storage {
            override suspend fun load(): ByteArray = ByteArray(0)
            override suspend fun save(network: ByteArray) = Unit
        },
        secureProperties = StubSecureProperties(),
        ioDispatcher = Dispatchers.Default,
    ).also { runBlocking { it.import(cdb) } }

    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) = Unit
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = 0u
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
