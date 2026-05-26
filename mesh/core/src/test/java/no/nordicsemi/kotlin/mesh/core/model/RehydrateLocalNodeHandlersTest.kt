@file:OptIn(ExperimentalUuidApi::class)

package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * Locks in the [MeshNetworkManager.rehydrateLocalNodeHandlers] behavior.
 *
 * 배경 (simdo, 2026-05-26 device 진단): [Model.eventHandler] 는 `@Transient` 라 import/load 후
 * 로컬 Provisioner node 의 foundation Model handler 가 전부 null 로 남는다. 이 상태에서
 * `AccessLayer.handle` 의 Device-Key 분기가 모든 Model 을 skip → Config 응답이 UnknownMessage 로
 * 거부됨. rehydrate 가 **AccessLayer 가 읽는 node** 에 handler 를 재attach 하는지 검증.
 *
 * 참고: upstream `TestPropertiesStorage` 는 build.gradle.kts 에서 dependency drift 로 test
 * sourceSet 에서 제외돼 있어 (suspend modifier mismatch) 사용 불가. 본 테스트는 최소 stub 을
 * 자체 정의한다.
 */
class RehydrateLocalNodeHandlersTest {

    /** 현재 [SecurePropertiesStorage] 시그니처에 맞춘 최소 no-op stub. */
    private class StubProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid) = IvIndex()
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

    private fun newManager() = MeshNetworkManager(
        storage = TestStorage(),
        secureProperties = StubProperties(),
        ioDispatcher = Dispatchers.Unconfined,
    )

    private fun importCdb(manager: MeshNetworkManager) = runBlocking {
        val bytes = this@RehydrateLocalNodeHandlersTest.javaClass.classLoader
            .getResourceAsStream("cdb_json.json")!!.readAllBytes()
        manager.import(bytes)
    }

    @Test
    fun `import 직후 로컬 node 의 Config Server handler 는 null (버그 재현)`() {
        val manager = newManager()
        importCdb(manager)

        val node = manager.network!!.localProvisioner!!.node!!
        val configServer = node.elements.first().models
            .first { it.modelId.id == Model.CONFIGURATION_SERVER_MODEL_ID.toUInt() }

        // @Transient 라 deserialize 후 handler 유실 — 이게 UnknownMessage 의 근본 원인.
        assertNull(configServer.eventHandler, "import 직후엔 handler 가 null 이어야 (버그 전제)")
    }

    @Test
    fun `rehydrate 는 AccessLayer 가 읽는 node 에 foundation handler 를 재attach 한다`() {
        val manager = newManager()
        importCdb(manager)

        val attached = manager.rehydrateLocalNodeHandlers()
        assertTrue(attached, "network/provisioner/node 가 있으면 true 를 반환해야 함")

        // AccessLayer.handle 이 iterate 하는 바로 그 컬렉션.
        val node = manager.network!!.localProvisioner!!.node!!
        val primaryModels = node.elements.first().models

        val configServer = primaryModels
            .first { it.modelId.id == Model.CONFIGURATION_SERVER_MODEL_ID.toUInt() }
        val configClient = primaryModels
            .first { it.modelId.id == Model.CONFIGURATION_CLIENT_MODEL_ID.toUInt() }

        assertNotNull(configServer.eventHandler, "Config Server handler 가 attach 돼야 함")
        assertNotNull(configClient.eventHandler, "Config Client handler 가 attach 돼야 함")

        // ConfigurationClientHandler 는 opCode 0x02 (Config Composition Data Status) 를 알아야 함.
        // 0x02 가 매핑돼야 AccessLayer 가 UnknownMessage 대신 정상 디코드한다.
        val clientHandler = configClient.eventHandler!!
        assertTrue(
            clientHandler.messageTypes.containsKey(0x02u),
            "Config Client handler 가 opCode 0x02 를 등록해야 (Composition Data Status 디코드)",
        )
    }

    @Test
    fun `rehydrate 는 idempotent — 두 번째 호출은 기존 handler 를 유지`() {
        val manager = newManager()
        importCdb(manager)

        manager.rehydrateLocalNodeHandlers()
        val node = manager.network!!.localProvisioner!!.node!!
        val firstHandler = node.elements.first().models
            .first { it.modelId.id == Model.CONFIGURATION_SERVER_MODEL_ID.toUInt() }
            .eventHandler
        assertNotNull(firstHandler)

        // 두 번째 호출은 fast-path no-op (이미 non-null) — 같은 handler 인스턴스 유지.
        val attached = manager.rehydrateLocalNodeHandlers()
        assertTrue(attached)
        val secondHandler = node.elements.first().models
            .first { it.modelId.id == Model.CONFIGURATION_SERVER_MODEL_ID.toUInt() }
            .eventHandler
        assertTrue(firstHandler === secondHandler, "두 번째 호출은 handler 를 재생성하지 않아야 함")
    }

    @Test
    fun `network 가 없으면 false 를 반환하고 throw 하지 않는다`() {
        val manager = newManager()
        assertFalse(manager.rehydrateLocalNodeHandlers(), "network null 이면 false")
    }
}
