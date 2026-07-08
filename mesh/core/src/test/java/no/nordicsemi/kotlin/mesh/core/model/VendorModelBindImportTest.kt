@file:OptIn(ExperimentalUuidApi::class)

package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * Vendor model 식별/bind 회귀 (simdo 2026-07-08 실기기: Cylinder Sensor cid 0x0136 가 vendor model
 * 14개 보유 → 리셋·재커미셔닝해도 "Not configured", Nordic DUT(vendor 0개)는 정상).
 *
 * 근원 = [VendorModelId.equals] copy-paste 버그(`if (other !is SigModelId) return false`) → 동일
 * VendorModelId 두 개가 절대 같지 않다고 판정 → `==`/HashSet/HashMap/distinctBy/Element.contains 가
 * 전부 vendor model 에서 오작동. 본 테스트가 그 identity 계약을 못박는다.
 */
class VendorModelBindImportTest {

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
        val bytes = this@VendorModelBindImportTest.javaClass.classLoader
            .getResourceAsStream("cdb_json.json")!!.readAllBytes()
        manager.import(bytes)
    }

    // ── VendorModelId identity (근원 버그 직격) ────────────────────────────────

    @Test
    fun `identical vendor model ids are equal`() {
        val a = VendorModelId(modelIdentifier = 0x0001u, companyIdentifier = 0x0136u)
        val b = VendorModelId(modelIdentifier = 0x0001u, companyIdentifier = 0x0136u)
        assertEquals(a, b, "동일 (company, model) 은 equal 이어야 한다")
        assertEquals(a.hashCode(), b.hashCode(), "equal 이면 hashCode 도 동일")
    }

    @Test
    fun `different vendor model ids are not equal`() {
        val a = VendorModelId(modelIdentifier = 0x0001u, companyIdentifier = 0x0136u)
        val b = VendorModelId(modelIdentifier = 0x0002u, companyIdentifier = 0x0136u)
        val c = VendorModelId(modelIdentifier = 0x0001u, companyIdentifier = 0x0059u)
        assertTrue(a != b, "model id 다르면 not equal")
        assertTrue(a != c, "company id 다르면 not equal")
    }

    @Test
    fun `vendor model id works as HashSet HashMap key`() {
        val v = VendorModelId(modelIdentifier = 0x0001u, companyIdentifier = 0x0136u)
        assertTrue(hashSetOf<ModelId>(v).contains(v), "HashSet 이 vendor 를 찾아야 한다")
        val map = hashMapOf<ModelId, Int>(v to 7)
        assertEquals(7, map[VendorModelId(0x0001u, 0x0136u)])
    }

    @Test
    fun `vendor id not equal to sig id with same lower 16 bits`() {
        val sig = SigModelId(modelIdentifier = 0x0001u)
        val vendor = VendorModelId(modelIdentifier = 0x0001u, companyIdentifier = 0x0136u)
        assertTrue(sig != vendor && vendor != sig, "SIG 와 vendor 는 lower16 같아도 not equal")
    }

    // ── Element.contains(vendorModelId) (원 버그의 대표 소비 API) ───────────────

    @Test
    fun `Element contains recognizes a vendor model`() {
        val vendor = VendorModelId(modelIdentifier = 0x0001u, companyIdentifier = 0x0136u)
        val element = Element(_name = "e", location = Location.UNKNOWN)
            .apply { add(model = Model(modelId = vendor, _bind = mutableListOf(), _subscribe = mutableListOf())) }
        assertTrue(element.contains(vendorModelId = vendor), "Element.contains 가 vendor 를 찾아야 한다")
    }

    // ── CDB import round-trip 이 vendor bind 를 보존하는지 (실측 증상 경로) ──────

    @Test
    fun `CDB import preserves vendor model bind`() {
        val manager = newManager()
        importCdb(manager)

        // fixture: node 0x0109 el0 vendor model 003F002A bind=[3]
        val node = manager.network!!.nodes.first {
            it.primaryUnicastAddress.address == 0x0109u.toUShort()
        }
        val vendorModel = node.elements.first().models.firstOrNull { it.modelId.id == 0x003F002Au }
        assertNotNull(vendorModel, "vendor model 003F002A 이 import 후 존재해야 한다")
        assertEquals(
            listOf<KeyIndex>(3u),
            vendorModel.bind,
            "vendor model 의 AppKey bind([3]) 가 import round-trip 후 보존돼야 한다",
        )
    }

    @Test
    fun `handler-style lookup finds vendor model and records bind`() {
        // ConfigModelAppStatus 핸들러가 하는 것과 동일: fresh VendorModelId(=response.modelId)로 조회 후
        // bind 기록. Element.model()은 `.id` 비교라 equals 버그와 무관하게 vendor 를 찾는다 —
        // 이 경로가 bind 기록의 실패 지점이 **아님**을 못박는다.
        val manager = newManager()
        importCdb(manager)
        val node = manager.network!!.nodes.first {
            it.primaryUnicastAddress.address == 0x0109u.toUShort()
        }
        val element = node.elements.first()
        val fresh = VendorModelId(modelIdentifier = 0x002Au, companyIdentifier = 0x003Fu)
        val model = element.model(modelId = fresh)
        assertNotNull(model, "handler 의 element.model(response.modelId) 조회가 vendor 를 찾아야 한다")
        model!!.bind(index = 9u)
        assertTrue(model.bind.contains(9u), "vendor model bind 기록이 반영돼야 한다")
    }
}
