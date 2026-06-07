package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.MeshBearer
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.ExperimentalUuidApi

/**
 * P6 M=2 회귀 가드 (simdo-fork, 2026-06-07 — config 병렬화 per-dest bearer registry).
 *
 * ## 배경
 *
 * config 병렬화(P6)는 N 노드를 N 개 1-hop GATT 로 **동시** config 한다. 종전 lib 는 `NetworkManager`
 * 에 [NetworkManager.bearer] **단일 슬롯**뿐이라 한 시점에 한 채널만 가능했다(=직렬 config). P6 는
 * dst unicast → 그 노드 bearer 의 **registry**([NetworkManager.registerBearer])를 도입한다:
 *  - TX([NetworkLayer.send])는 `networkPdu.destination.address` → [NetworkManager.bearerFor] 로 라우팅.
 *  - 미등록 dst(group/proxy/평상시 측위)는 default [NetworkManager.bearer] 로 떨어진다(회귀 0).
 *  - RX 는 src/dest-keyed 라 dst 라우팅 불필요 → registered bearer 마다 collector 를 fan-in.
 *
 * 본 테스트는 실제 [MeshNetworkManager] 를 import 해 살아있는 `NetworkManager` 위에서 registry
 * 라우팅 불변을 가드한다(stub MeshBearer 들을 등록/해제하고 [NetworkManager.bearerFor] 결과를 검증).
 */
@OptIn(ExperimentalUuidApi::class)
class PerDestBearerRegistryTest {

    /** pdus flow + send 캡처만 하는 최소 MeshBearer stub. */
    private class StubBearer(val tag: String) : MeshBearer {
        val pduFlow = MutableSharedFlow<Pdu>(extraBufferCapacity = 64)
        override val pdus: Flow<Pdu> = pduFlow
        override val state: StateFlow<BearerEvent>
            get() = throw UnsupportedOperationException()
        override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.NetworkPdu)
        override val isOpen: Boolean = true
        override suspend fun open() = Unit
        override suspend fun close() = Unit
        override suspend fun send(pdu: ByteArray, type: PduType) = Unit
    }

    // 본 테스트 자족(self-contained) storage stub — 공유 [TestPropertiesStorage] 는 현재 interface 와
    // 시그니처가 어긋나(suspend mismatch) 컴파일되지 않으므로 여기 최신 시그니처로 직접 둔다.
    private class StubStorage : Storage {
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = Unit
    }

    @OptIn(kotlin.uuid.ExperimentalUuidApi::class)
    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: kotlin.uuid.Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: kotlin.uuid.Uuid, ivIndex: IvIndex) = Unit
        private var seq: UInt = 0u
        override suspend fun nextSequenceNumber(uuid: kotlin.uuid.Uuid, address: UnicastAddress): UInt = seq++
        override suspend fun storeNextSequenceNumber(uuid: kotlin.uuid.Uuid, address: UnicastAddress, sequenceNumber: UInt) = Unit
        override suspend fun resetSequenceNumber(uuid: kotlin.uuid.Uuid, address: UnicastAddress) = Unit
        override suspend fun lastSeqAuthValue(uuid: kotlin.uuid.Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(uuid: kotlin.uuid.Uuid, source: UnicastAddress, lastSeqAuth: ULong) = Unit
        override suspend fun previousSeqAuthValue(uuid: kotlin.uuid.Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(uuid: kotlin.uuid.Uuid, source: UnicastAddress, seqAuth: ULong) = Unit
        override suspend fun storeLocalProvisioner(uuid: kotlin.uuid.Uuid, localProvisionerUuid: kotlin.uuid.Uuid) = Unit
        override suspend fun localProvisioner(uuid: kotlin.uuid.Uuid): String? = null
    }

    private fun freshNetworkManager(): MeshNetworkManager {
        val mgr = MeshNetworkManager(
            storage = StubStorage(),
            secureProperties = StubSecureProperties(),
            ioDispatcher = Dispatchers.Default,
        )
        val jsonBytes =
            this.javaClass.classLoader.getResourceAsStream("cdb_json.json")!!.readAllBytes()
        runBlocking { mgr.import(jsonBytes) }
        return mgr
    }

    private val nodeA: UShort = 0x0002u
    private val nodeB: UShort = 0x0003u
    private val groupAddr: UShort = 0xC000u

    /**
     * 미등록 dst·group 은 default bearer 로, 등록된 dst 는 각자 자기 bearer 로 라우팅된다.
     * (= [NetworkManager.bearerFor] 의 `_bearers[dst] ?: bearer` 불변 — TX 채널 분리의 핵심.)
     */
    @Test
    fun `bearerFor 는 등록 dst 는 그 bearer, 미등록은 default 로 라우팅한다`() {
        val mgr = freshNetworkManager()
        val nm = mgr.networkManager!!
        val default = StubBearer("default")
        val bearerForA = StubBearer("A")
        val bearerForB = StubBearer("B")

        mgr.meshBearer = default // default 슬롯

        // 등록 전: 모든 dst 가 default.
        assertSame("등록 전 nodeA 는 default", default, nm.bearerFor(nodeA))
        assertSame("등록 전 nodeB 는 default", default, nm.bearerFor(nodeB))

        // nodeA, nodeB 각각 등록.
        assertTrue(mgr.registerBearer(nodeA, bearerForA))
        assertTrue(mgr.registerBearer(nodeB, bearerForB))

        // 등록된 dst 는 각자 자기 bearer, 미등록(group)·default 슬롯은 default.
        assertSame("nodeA → A", bearerForA, nm.bearerFor(nodeA))
        assertSame("nodeB → B", bearerForB, nm.bearerFor(nodeB))
        assertSame("group(미등록) → default", default, nm.bearerFor(groupAddr))

        // nodeA 해제 후: nodeA 는 default 로 복귀, nodeB 는 유지(독립성).
        mgr.unregisterBearer(nodeA)
        assertSame("해제 후 nodeA → default", default, nm.bearerFor(nodeA))
        assertSame("nodeB 는 유지", bearerForB, nm.bearerFor(nodeB))
    }

    /**
     * registry 가 비어 있으면 default [NetworkManager.bearer] 만 쓰인다(평상시 측위/group 제어 회귀 0).
     * default 가 null 이면 bearerFor 도 null(종전 단일 슬롯 동작 보존).
     */
    @Test
    fun `registry 비면 default 슬롯과 동일 동작 - 비-config 경로 회귀 0`() {
        val mgr = freshNetworkManager()
        val nm = mgr.networkManager!!

        // default null → 모든 dst null (종전 단일 슬롯 null 과 동일).
        assertNull("default null 이면 bearerFor 도 null", nm.bearerFor(nodeA))

        val default = StubBearer("default")
        mgr.meshBearer = default
        assertSame("registry 비면 모든 dst 가 default", default, nm.bearerFor(nodeA))
        assertSame(default, nm.bearerFor(groupAddr))
        assertTrue("registry 비어있음", nm.bearers.isEmpty())
    }

    /**
     * 등록/해제가 [NetworkManager.bearers] 맵을 정확히 반영하고 멱등하다(누수 0).
     */
    @Test
    fun `register unregister 는 bearers 맵을 정확히 반영하고 멱등하다`() {
        val mgr = freshNetworkManager()
        val nm = mgr.networkManager!!
        val a = StubBearer("A")
        val b = StubBearer("B")

        mgr.registerBearer(nodeA, a)
        mgr.registerBearer(nodeB, b)
        assertEquals(2, nm.bearers.size)
        assertSame(a, nm.bearers[nodeA])

        // 멱등: 같은 dst 재등록(다른 bearer) → 교체, 크기 유지.
        val a2 = StubBearer("A2")
        mgr.registerBearer(nodeA, a2)
        assertEquals("같은 dst 재등록은 교체(크기 불변)", 2, nm.bearers.size)
        assertSame(a2, nm.bearers[nodeA])

        // 해제 후 크기 감소, 재해제 멱등(no-op).
        mgr.unregisterBearer(nodeA)
        assertEquals(1, nm.bearers.size)
        mgr.unregisterBearer(nodeA)
        assertEquals("재해제 멱등", 1, nm.bearers.size)
        assertFalse(nm.bearers.containsKey(nodeA))
    }

    /**
     * networkManager 미초기화(network 미로드) 시 registerBearer 는 no-op(false) — 안전 실패.
     */
    @Test
    fun `network 미로드면 registerBearer 는 false`() {
        val mgr = MeshNetworkManager(
            storage = StubStorage(),
            secureProperties = StubSecureProperties(),
            ioDispatcher = Dispatchers.Default,
        )
        // import 안 함 → networkManager null.
        assertNull(mgr.networkManager)
        assertFalse("network 미로드면 false", mgr.registerBearer(nodeA, StubBearer("A")))
    }
}
