@file:OptIn(ExperimentalUuidApi::class)

package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork 회귀 가드 — `import()` 의 IV Index 복원 (2026-08-11).
 *
 * ## 결함
 *
 * `MeshNetwork.ivIndex` 는 `@Transient` 이고 CDB Profile 스키마에도 최상위 `ivIndex` 가 없다
 * (본 테스트가 쓰는 `cdb_json.json` 도 마찬가지). 따라서 `deserialize()` 결과는 **항상**
 * `IvIndex(index = 0u)` 다. `load()` 는 secure properties 사이드카에서 복원하는데 `import()` 는
 * 누락되어 있었다.
 *
 * ## 왜 중요한가
 *
 * 송신 경로(`NetworkPdu`)는 in-memory `meshNetwork.ivIndex` 를 쓴다. 망 IV Index 가 N 일 때
 * 앱이 IV 0 으로 암호화해 보내면, 수신 노드는 `iv_index` 또는 `iv_index-1` 로만 복호를 시도하므로
 * **N >= 2 부터 전량 무음 폐기**된다(obfuscation/MIC 실패). N=1 은 old_iv 경로로 우연히 통과한다.
 *
 * simdo 앱의 실제 진입 경로는 대부분 `import()` 다 — 부트(서버 pull/WAL warm pull/부트스트랩),
 * venue 전환, provisioner 등록, 서버 CDB 변경 Realtime 수신, 클라우드 복원. 즉 `load()` 만
 * 고쳐진 상태로는 보호되지 않는다.
 */
class ImportIvIndexRestoreTest {

    private companion object {
        /** `cdb_json.json` 의 meshUUID. */
        const val CDB_UUID = "72C6BE40-444D-2081-BEAA-DDAD4E3CC21C"
        const val STORED_IV: UInt = 7u
    }

    /**
     * uuid 별 IV Index 사이드카 stub. 실제 구현(simdo `MeshSecurePropertiesStorage`)과 동일하게
     * row 가 없으면 `IvIndex()`(index 0) 를 반환한다.
     */
    private class StubProperties(
        private val ivByUuid: Map<Uuid, IvIndex> = emptyMap(),
    ) : SecurePropertiesStorage {
        /** 조회에 사용된 uuid 기록 — "수신 객체 기준 조회" 검증용. */
        val ivIndexQueries = mutableListOf<Uuid>()

        override suspend fun ivIndex(uuid: Uuid): IvIndex {
            ivIndexQueries += uuid
            return ivByUuid[uuid] ?: IvIndex()
        }

        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) {}
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = 0u
        override suspend fun storeNextSequenceNumber(
            uuid: Uuid,
            address: UnicastAddress,
            sequenceNumber: UInt,
        ) {}
        override suspend fun resetSequenceNumber(uuid: Uuid, address: UnicastAddress) {}
        override suspend fun lastSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(uuid: Uuid, source: UnicastAddress, lastSeqAuth: ULong) {}
        override suspend fun previousSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(uuid: Uuid, source: UnicastAddress, seqAuth: ULong) {}
        override suspend fun storeLocalProvisioner(uuid: Uuid, localProvisionerUuid: Uuid) {}
        override suspend fun localProvisioner(uuid: Uuid): String? = null
    }

    /** `load()` 경로용 — 주어진 바이트를 그대로 돌려주는 storage. */
    private class BytesStorage(private val bytes: ByteArray) : Storage {
        override suspend fun load(): ByteArray = bytes
        override suspend fun save(network: ByteArray) {}
    }

    private fun cdbBytes(): ByteArray =
        javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()

    private fun newManager(
        properties: SecurePropertiesStorage,
        storage: Storage = TestStorage(),
    ) = MeshNetworkManager(
        storage = storage,
        secureProperties = properties,
        ioDispatcher = Dispatchers.Unconfined,
    )

    // ── 핵심 회귀: import 가 사이드카에서 IV 를 복원해야 한다 ─────────────────────

    @Test
    fun `import restores iv index from secure properties`() = runBlocking {
        val uuid = Uuid.parse(CDB_UUID)
        val props = StubProperties(mapOf(uuid to IvIndex(index = STORED_IV)))
        val manager = newManager(props)

        manager.import(cdbBytes())

        val network = assertNotNull(manager.network, "import 후 network 가 있어야 한다")
        assertEquals(
            STORED_IV,
            network.ivIndex.index,
            "import 는 secure properties 의 IV Index 를 복원해야 한다 " +
                "(0 이면 망 IV>=2 에서 송신 전량 폐기)",
        )
    }

    // ── load 와의 대칭성 ────────────────────────────────────────────────────────

    @Test
    fun `load and import restore the same iv index`() = runBlocking {
        val uuid = Uuid.parse(CDB_UUID)
        val bytes = cdbBytes()

        val loadProps = StubProperties(mapOf(uuid to IvIndex(index = STORED_IV)))
        val loaded = newManager(loadProps, BytesStorage(bytes))
        assertTrue(loaded.load(), "load() 가 성공해야 한다")

        val importProps = StubProperties(mapOf(uuid to IvIndex(index = STORED_IV)))
        val imported = newManager(importProps)
        imported.import(bytes)

        assertEquals(
            assertNotNull(loaded.network).ivIndex.index,
            assertNotNull(imported.network).ivIndex.index,
            "load() 와 import() 의 IV Index 복원 결과가 같아야 한다",
        )
    }

    // ── 조회 키는 import 되는 CDB 자신의 uuid ───────────────────────────────────

    @Test
    fun `import queries secure properties with the imported network uuid`() = runBlocking {
        val props = StubProperties()
        val manager = newManager(props)

        manager.import(cdbBytes())

        assertEquals(
            listOf(Uuid.parse(CDB_UUID)),
            props.ivIndexQueries,
            "venue 전환 시 이전 망이 아니라 **import 되는 망**의 uuid 로 조회해야 한다",
        )
    }

    @Test
    fun `import of a different network does not inherit another networks iv index`() = runBlocking {
        // 사이드카에는 '다른 망'의 IV 만 들어 있다 → import 대상 망은 0 이어야 한다.
        val otherUuid = Uuid.parse("11111111-2222-3333-4444-555555555555")
        val props = StubProperties(mapOf(otherUuid to IvIndex(index = 99u)))
        val manager = newManager(props)

        manager.import(cdbBytes())

        assertEquals(
            0u,
            assertNotNull(manager.network).ivIndex.index,
            "다른 망의 IV Index 를 상속하면 안 된다",
        )
    }

    // ── fallback: 사이드카 미존재(신규 망) 는 0 유지 ──────────────────────────────

    @Test
    fun `import falls back to zero when sidecar has no row`() = runBlocking {
        val props = StubProperties()
        val manager = newManager(props)

        manager.import(cdbBytes())

        assertEquals(
            0u,
            assertNotNull(manager.network).ivIndex.index,
            "SNB 를 한 번도 못 받은 신규 망은 0 시작 — 첫 SNB 로 정렬되므로 무해",
        )
    }
}
