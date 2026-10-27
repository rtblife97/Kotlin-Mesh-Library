package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.provisioning.ProvisioningBearer
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.Provisioner
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.model.UnicastRange
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-28, simdo #99) — Provisioning Data 송신 직전 선기록 훅 ([ProvisioningManager.beforeProvisioningData]).
 *
 * 지키는 것 (고아 방지의 전제):
 *  1. 훅은 Data PDU 가 베어러로 나가기 **전에** 끝난다 — 그 노드의 장치 키는 기기가 도출할 키와 같다.
 *  2. 훅이 넘긴 노드의 JSON 은 기기가 망에 들어간 뒤의 노드 JSON(앱이 서버 1008 로 보내는 형식) 과 같고, 그 JSON 으로 되살린 망이
 *     같은 JSON 을 낸다 (앱 재시작 뒤 원장 선기록으로 복원하는 경로).
 *  3. 훅이 던지면 Data 를 보내지 않고 `Failed(WriteAheadFailed)` 로 끝난다 (기기는 미할당으로 남아 다시 가입할 수 있다).
 *  4. Data 송신 자체가 던지면 [ProvisioningManager.deliveredNode] 는 비어 있다 — 키를 못 받았을 수 있는 기기를 살리기로 망에 넣지 않는다.
 *
 * 기기 쪽은 [FakeProvisioneeBearer] (실제 ECDH·확인값·복호).
 */
@OptIn(ExperimentalUuidApi::class)
class BeforeProvisioningDataHookTest {

    private val meshManager = MeshNetworkManager(
        storage = InMemoryStorage(),
        secureProperties = InMemorySecureProperties(),
        ioDispatcher = Dispatchers.Unconfined,
    )

    private val network: MeshNetwork = runBlocking {
        meshManager.create(
            name = "WriteAhead",
            provisioner = Provisioner(uuid = Uuid.random(), name = "Local").apply {
                allocate(range = UnicastRange(lowAddress = UnicastAddress(0x0001), highAddress = UnicastAddress(0x00FF)))
            },
        )
    }

    /** 베어러로 나간 PDU 종류와 훅 호출을 한 줄로 기록한다. [failData] 면 Data(0x07) 송신에서 던진다(기기에 안 감). */
    private class Recording(val inner: FakeProvisioneeBearer, val events: MutableList<String>, val failData: Boolean = false) :
        ProvisioningBearer by inner {
        override suspend fun send(pdu: ByteArray, type: PduType) {
            val kind = pdu[0].toInt()
            if (kind == 0x07 && failData) {
                events += "send-fail 0x07"
                throw IllegalStateException("GATT write failed")
            }
            events += "send 0x%02X".format(kind)
            inner.send(pdu, type)
        }
    }

    private fun manager(bearer: ProvisioningBearer, uuid: Uuid) = ProvisioningManager(
        unprovisionedDevice = UnprovisionedDevice(name = "emblazeL-7A60-F1", uuid = uuid),
        meshNetwork = network,
        bearer = bearer,
        ioDispatcher = Dispatchers.Unconfined,
    )

    private fun ProvisioningManager.runToEnd(): List<ProvisioningState> = runBlocking {
        val states = mutableListOf<ProvisioningState>()
        provision(attentionTimer = 0u).collect { state ->
            states += state
            if (state is ProvisioningState.CapabilitiesReceived) state.start(state.defaultParameters)
        }
        states
    }

    @Test
    fun `hook finishes before the Data PDU leaves and carries the key the device derives`() {
        val events = mutableListOf<String>()
        val fake = FakeProvisioneeBearer(numberOfElements = 3)
        runBlocking { fake.open() }
        val uuid = Uuid.random()
        val manager = manager(Recording(fake, events), uuid)
        var seen: Node? = null
        var json: String? = null
        var deliveredDuringHook: Node? = null
        manager.beforeProvisioningData = { node ->
            events += "hook"
            seen = node
            json = MeshNetworkManager.encodeNode(node)
            deliveredDuringHook = manager.deliveredNode
        }

        val states = manager.runToEnd()

        assertTrue(states.last() is ProvisioningState.Complete, "가입이 끝까지 가야 함: $states")
        val hookAt = events.indexOf("hook")
        val dataAt = events.indexOf("send 0x07")
        assertTrue(hookAt in 0 until dataAt, "훅이 Data 송신 전에 끝나야 함: $events")
        assertNull(deliveredDuringHook, "훅 동안에는 아직 '전달됨' 노드가 없다")
        val node = assertNotNull(seen)
        assertContentEquals(assertNotNull(fake.derivedDeviceKey), node.deviceKey, "선기록 장치 키 = 기기가 도출한 키")
        assertEquals(fake.receivedUnicastAddress, node.primaryUnicastAddress.address)
        assertEquals(3, node.elementsCount)
        assertNull(node.network, "선기록 노드는 아직 망 밖")
        assertNotNull(manager.deliveredNode, "send 정상 반환 뒤에는 전달됨 노드가 있다")

        // 선기록 JSON = 망에 들어간 뒤 export (서버 1008 형식).
        val prewritten = assertNotNull(json)
        val exported = runBlocking { meshManager.exportNode(uuid) }
        assertEquals(exported, prewritten)

        // 재시작 뒤 복원: 그 노드가 없는 망(앱이 Complete 처리 전에 죽어 저장 파일에 없음) 에 선기록 JSON 으로 되살리면 같은 JSON 을 낸다.
        // (같은 망에서 remove 뒤 add 는 안 된다 — lib 가 뺀 주소를 IV Index 가 오를 때까지 exclusion 으로 막는다. 앱은 그 경우 새 주소로 간다.)
        val restartManager = MeshNetworkManager(
            storage = InMemoryStorage(),
            secureProperties = InMemorySecureProperties(),
            ioDispatcher = Dispatchers.Unconfined,
        )
        val restarted = runBlocking {
            restartManager.create(
                name = "AfterRestart",
                provisioner = Provisioner(uuid = Uuid.random(), name = "Local").apply {
                    allocate(range = UnicastRange(lowAddress = UnicastAddress(0x0001), highAddress = UnicastAddress(0x00FF)))
                },
            )
        }
        assertNull(restarted.node(uuid = uuid))
        val restored = MeshNetworkManager.decodeNode(prewritten)
        runBlocking { restarted.withCdbLock { restarted.add(node = restored) } }
        assertEquals(prewritten, runBlocking { restartManager.exportNode(uuid) })
        assertContentEquals(fake.derivedDeviceKey, restarted.node(uuid = uuid)?.deviceKey)
    }

    @Test
    fun `a failing hook stops before Data - nothing sent, Failed with WriteAheadFailed`() {
        val events = mutableListOf<String>()
        val fake = FakeProvisioneeBearer(numberOfElements = 1)
        runBlocking { fake.open() }
        val uuid = Uuid.random()
        val manager = manager(Recording(fake, events), uuid)
        manager.beforeProvisioningData = { throw IllegalStateException("disk full") }

        val states = manager.runToEnd()

        val failed = states.last() as? ProvisioningState.Failed
        assertNotNull(failed, "선기록 실패 = 가입 실패: $states")
        val error = assertIs<WriteAheadFailed>(failed.error)
        assertEquals("disk full", error.cause.message)
        assertEquals(0, events.count { it == "send 0x07" }, "Data 는 나가지 않는다: $events")
        assertNull(fake.derivedDeviceKey, "기기는 장치 키를 받지 않았다")
        assertNull(manager.deliveredNode)
        assertNull(network.node(uuid = uuid))
    }

    @Test
    fun `when the Data send throws the node is pre-written but not marked delivered`() {
        val events = mutableListOf<String>()
        val fake = FakeProvisioneeBearer(numberOfElements = 1)
        runBlocking { fake.open() }
        val uuid = Uuid.random()
        val manager = manager(Recording(fake, events, failData = true), uuid)
        var hooked = 0
        manager.beforeProvisioningData = { hooked++ }

        assertFailsWith<IllegalStateException> { manager.runToEnd() }

        assertEquals(1, hooked, "선기록은 했다 (송신 전에)")
        assertTrue(events.indexOf("send-fail 0x07") > 0)
        assertNull(manager.deliveredNode, "송신이 던지면 '전달됨' 아님 — 살리기 대상이 아니다")
        assertNull(runBlocking { manager.commitDeliveredNode() })
        assertNull(network.node(uuid = uuid))
    }
}
