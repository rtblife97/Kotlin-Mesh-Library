package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure.DEVICE_KEY_REFRESH
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure.NODE_ADDRESS_REFRESH
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure.NODE_COMPOSITION_REFRESH
import no.nordicsemi.kotlin.mesh.core.model.Insecure
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.Provisioner
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.model.UnicastRange
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-patch (2026-08-26) — Node Provisioning Protocol Interface (MshPRT 1.1 §3.11.8) 를
 * `ProvisioningManager` 로 **끝까지 돌리는** 테스트.
 *
 * [FakeProvisioneeBearer] 가 디바이스 역할을 실제 crypto 로 연기한다 — ECDH · confirmation ·
 * session key · Provisioning Data 복호가 전부 진짜다. 따라서 여기서 초록이면
 *
 * - 프로비저닝 상태머신이 NPPI 모드에서도 처음부터 끝까지 돌고,
 * - 프로비저너와 디바이스가 **같은 Device Key** 를 도출하며,
 * - Provisioning Data 의 주소·NetKey·IV Index 가 노드가 기대하는 값으로 나가고,
 * - 종결 시 CDB 가 절차별 규칙대로 갱신된다
 *
 * 는 뜻이다. 종전에는 provisioning 상태머신을 끝까지 태우는 테스트가 **하나도 없었다**.
 */
@OptIn(ExperimentalUuidApi::class)
class ProvisioningManagerNppiTest {

    private companion object {
        const val TARGET_ADDRESS = 0x0100
        const val TARGET_ELEMENTS = 2
        val OLD_DEVICE_KEY = ByteArray(16) { 0xAA.toByte() }
    }

    private class Fixture(
        ivIndex: UInt = 0u,
        targetElements: Int = TARGET_ELEMENTS,
    ) {
        val manager = MeshNetworkManager(
            storage = InMemoryStorage(),
            secureProperties = InMemorySecureProperties(),
            ioDispatcher = Dispatchers.Unconfined,
        )

        val network: MeshNetwork = runBlocking {
            manager.create(
                name = "NPPI Flow Network",
                provisioner = Provisioner(uuid = Uuid.random(), name = "Local").apply {
                    allocate(range = UnicastRange(lowAddress = UnicastAddress(address = 0x0001),
                        highAddress = UnicastAddress(address = 0x00FF)))
                    allocate(range = UnicastRange(lowAddress = UnicastAddress(address = 0x0300),
                        highAddress = UnicastAddress(address = 0x03FF)))
                },
            )
        }.apply { if (ivIndex > 0u) setIvIndex(index = ivIndex) }

        val netKey = network.networkKeys.first()

        /**
         * 대상 노드는 provisioner 의 할당 범위 **밖**(0x0100)에 둔다. 다른 provisioner 가
         * 배정했을 수도 있는 상황을 재현하기 위해서다 — 0x00/0x02 는 그런 노드에도 되어야 한다.
         */
        val target: Node = Node(
            name = "Target",
            uuid = Uuid.random(),
            deviceKey = OLD_DEVICE_KEY,
            unicastAddress = UnicastAddress(address = TARGET_ADDRESS),
            elementCount = targetElements,
            assignedNetworkKey = netKey,
            security = Insecure,
        ).also { network.add(node = it) }

        // NOTE: composition data 의 보존/무효화 전이는 `mesh:core` 의
        // NodeProvisioningProtocolInterfaceTest 가 봉인한다 — Node 의 composition 필드는
        // setter 가 internal 이라 이 모듈에서는 심을 수 없다. 여기서는 관측 가능한
        // 대체 지표(Element 개수, isCompositionDataReceived)만 확인한다.
    }

    /**
     * `provision()` 을 끝까지 수집한다. Capabilities 를 받으면 기본 파라미터로 즉시 진행한다
     * (앱의 "자동 진행" 경로와 동일).
     */
    private fun ProvisioningManager.runToCompletion(): List<ProvisioningState> = runBlocking {
        provision(attentionTimer = 0u)
            .also { }
            .let { flow ->
                val states = mutableListOf<ProvisioningState>()
                flow.collect { state ->
                    states += state
                    if (state is ProvisioningState.CapabilitiesReceived) {
                        state.start(state.defaultParameters)
                    }
                }
                states
            }
    }

    private fun nppiManager(
        f: Fixture,
        procedure: NodeProvisioningProtocolInterfaceProcedure,
        bearer: FakeProvisioneeBearer,
    ) = ProvisioningManager(
        node = f.target,
        procedure = procedure,
        meshNetwork = f.network,
        bearer = bearer,
        ioDispatcher = Dispatchers.Unconfined,
    )

    // ----------------------------------------------------------------------------------------
    // 회귀: 신규 장치 프로비저닝 경로는 그대로여야 한다
    // ----------------------------------------------------------------------------------------

    @Test
    fun `provisioning a brand new device still adds a node the classic way`() {
        val f = Fixture()
        val bearer = FakeProvisioneeBearer(numberOfElements = 3)
        runBlocking { bearer.open() }
        val device = UnprovisionedDevice(name = "New", uuid = Uuid.random())
        val manager = ProvisioningManager(
            unprovisionedDevice = device,
            meshNetwork = f.network,
            bearer = bearer,
            ioDispatcher = Dispatchers.Unconfined,
        )

        val states = manager.runToCompletion()

        assertTrue(states.last() is ProvisioningState.Complete, "Complete 로 끝나야 한다")
        assertNull(
            manager.nodeProvisioningProtocolInterfaceProcedure,
            "일반 프로비저닝은 NPPI 모드가 아니다",
        )
        val added = f.network.node(uuid = device.uuid)
        assertNotNull(added, "새 노드가 망에 추가되어야 한다")
        assertContentEquals(
            bearer.derivedDeviceKey, added.deviceKey,
            "프로비저너와 디바이스가 같은 Device Key 를 도출해야 한다",
        )
        assertEquals(3, added.elementsCount, "Capabilities 의 Element 개수 반영")
        assertTrue(
            f.network.localProvisioner!!.hasAllocatedRange(range = added.unicastRange),
            "신규 장치 주소는 local Provisioner 의 할당 범위에서 나온다",
        )
    }

    // ----------------------------------------------------------------------------------------
    // 0x00 — Device Key Refresh
    // ----------------------------------------------------------------------------------------

    @Test
    fun `device key refresh runs end to end and only swaps the device key`() {
        val f = Fixture()
        val bearer = FakeProvisioneeBearer(
            numberOfElements = TARGET_ELEMENTS,
            expectedNetworkKey = f.netKey.key,
            expectedIvIndex = f.network.ivIndex.index,
            expectedAddress = TARGET_ADDRESS.toUShort(),
        )
        runBlocking { bearer.open() }

        val states = nppiManager(f, DEVICE_KEY_REFRESH, bearer).runToCompletion()

        assertNull(bearer.rejection, "디바이스가 Provisioning Data 를 받아들여야 한다")
        assertTrue(states.last() is ProvisioningState.Complete)
        assertContentEquals(
            bearer.derivedDeviceKey, f.target.deviceKey,
            "CDB 의 Device Key 가 디바이스가 도출한 새 키로 교체되어야 한다",
        )
        assertFalse(
            OLD_DEVICE_KEY.contentEquals(f.target.deviceKey),
            "옛 Device Key 는 남아 있으면 안 된다",
        )
        assertEquals(
            UnicastAddress(address = TARGET_ADDRESS), f.target.primaryUnicastAddress,
            "주소 유지",
        )
        assertEquals(1, f.network.nodes.count { it.uuid == f.target.uuid }, "노드 중복 없음")
        assertSame(f.target, f.network.node(uuid = f.target.uuid), "같은 인스턴스 유지")
        assertEquals(
            TARGET_ELEMENTS, f.target.elementsCount,
            "Element 개수 유지 — composition 은 바뀌지 않는다",
        )
    }

    @Test
    fun `device key refresh keeps the node's own address usable (self-exclusion)`() {
        val f = Fixture()
        // blocker ①: self-exclusion 이 없으면 여기서 InvalidAddress 로 즉사했다.
        assertTrue(
            nppiManager(f, DEVICE_KEY_REFRESH, FakeProvisioneeBearer())
                .isUnicastAddressValid(
                    unicastAddress = f.target.primaryUnicastAddress,
                    numberOfElements = TARGET_ELEMENTS,
                ),
            "0x00 은 노드의 현재 주소를 그대로 써야 하고, 그것이 유효해야 한다",
        )
    }

    @Test
    fun `device key refresh rejects any address other than the node's own`() {
        val f = Fixture()
        val manager = nppiManager(f, DEVICE_KEY_REFRESH, FakeProvisioneeBearer())

        assertFalse(
            manager.isUnicastAddressValid(
                unicastAddress = UnicastAddress(address = 0x0310),
                numberOfElements = TARGET_ELEMENTS,
            ),
            "0x00 에서 주소를 바꾸면 노드가 refresh_is_valid() 에서 거부한다",
        )
    }

    // ----------------------------------------------------------------------------------------
    // 0x01 — Node Address Refresh
    // ----------------------------------------------------------------------------------------

    @Test
    fun `address refresh runs end to end, moves the node and excludes the old range`() {
        val f = Fixture()
        val previousAddresses = f.target.addresses
        val bearer = FakeProvisioneeBearer(
            numberOfElements = TARGET_ELEMENTS,
            expectedNetworkKey = f.netKey.key,
            expectedIvIndex = f.network.ivIndex.index,
            forbiddenRange = TARGET_ADDRESS until TARGET_ADDRESS + TARGET_ELEMENTS,
        )
        runBlocking { bearer.open() }

        val states = nppiManager(f, NODE_ADDRESS_REFRESH, bearer).runToCompletion()

        assertNull(bearer.rejection, "디바이스가 새 주소를 받아들여야 한다")
        assertTrue(states.last() is ProvisioningState.Complete)
        assertContentEquals(bearer.derivedDeviceKey, f.target.deviceKey, "Device Key 도 교체")
        assertFalse(
            f.target.primaryUnicastAddress == UnicastAddress(address = TARGET_ADDRESS),
            "주소가 반드시 바뀌어야 한다",
        )
        assertTrue(
            f.network.localProvisioner!!.hasAllocatedRange(range = f.target.unicastRange),
            "새 주소는 local Provisioner 의 할당 범위에서 나온다",
        )
        assertEquals(
            f.target.primaryUnicastAddress.address,
            bearer.receivedUnicastAddress,
            "디바이스가 받은 주소와 CDB 의 주소가 같아야 한다",
        )
        previousAddresses.forEach {
            assertFalse(
                f.network.isAddressAvailable(address = it, elementCount = 1),
                "옛 주소 $it 는 exclusion list 에 들어가야 한다 (노드가 seq 를 0 으로 리셋한다)",
            )
        }
        assertEquals(1, f.network.nodes.count { it.uuid == f.target.uuid }, "노드 중복 없음")
    }

    @Test
    fun `address refresh proposes an address outside the node's current range`() {
        val f = Fixture()
        val manager = nppiManager(f, NODE_ADDRESS_REFRESH, FakeProvisioneeBearer())

        assertFalse(
            manager.isUnicastAddressValid(
                unicastAddress = f.target.primaryUnicastAddress,
                numberOfElements = TARGET_ELEMENTS,
            ),
            "0x01 은 같은 주소를 허용하지 않는다",
        )
        assertTrue(
            manager.isUnicastAddressValid(
                unicastAddress = UnicastAddress(address = 0x0310),
                numberOfElements = TARGET_ELEMENTS,
            ),
            "할당 범위 안의 빈 주소는 유효",
        )
        assertFalse(
            manager.isUnicastAddressValid(
                unicastAddress = UnicastAddress(address = TARGET_ADDRESS - 1),
                numberOfElements = TARGET_ELEMENTS,
            ),
            "옛 범위와 겹치는 주소는 거부",
        )
    }

    // ----------------------------------------------------------------------------------------
    // 0x02 — Node Composition Refresh
    // ----------------------------------------------------------------------------------------

    @Test
    fun `composition refresh runs end to end, keeps the address and adopts the new element count`() {
        val f = Fixture()
        val bearer = FakeProvisioneeBearer(
            // 펌웨어 업데이트로 element 가 2 → 4 로 늘어난 노드를 재현한다.
            numberOfElements = 4,
            expectedNetworkKey = f.netKey.key,
            expectedIvIndex = f.network.ivIndex.index,
            expectedAddress = TARGET_ADDRESS.toUShort(),
        )
        runBlocking { bearer.open() }

        val states = nppiManager(f, NODE_COMPOSITION_REFRESH, bearer).runToCompletion()

        assertNull(bearer.rejection)
        assertTrue(states.last() is ProvisioningState.Complete)
        assertContentEquals(bearer.derivedDeviceKey, f.target.deviceKey, "Device Key 교체")
        assertEquals(
            UnicastAddress(address = TARGET_ADDRESS), f.target.primaryUnicastAddress,
            "0x02 는 주소를 유지한다",
        )
        assertEquals(4, f.target.elementsCount, "새 Element 개수를 즉시 반영")
        assertFalse(
            f.target.isCompositionDataReceived,
            "Page 128 이 Page 0 이 됐으므로 CDP0 재독해가 필요하다",
        )
    }

    // ----------------------------------------------------------------------------------------
    // 사전조건 / 가드
    // ----------------------------------------------------------------------------------------

    @Test
    fun `nppi requires a network key the node already knows`() {
        val f = Fixture()
        // 노드가 모르는 두 번째 NetKey 를 망에 추가하고, 그것을 강제로 고르게 한다.
        val foreignKey = f.network.add(name = "Foreign NetKey")
        val bearer = FakeProvisioneeBearer(numberOfElements = TARGET_ELEMENTS)
        runBlocking { bearer.open() }
        val manager = nppiManager(f, DEVICE_KEY_REFRESH, bearer)

        assertFailsWith<InvalidNodeProvisioningProtocolInterfaceState>(
            message = "노드는 Provisioning Data 의 NetKey 를 자기 subnet 의 송신 키와 " +
                "바이트 비교한다 (Zephyr refresh_is_valid()). 모르는 키를 보내면 조용히 실패한다",
        ) {
            runBlocking {
                manager.provision(attentionTimer = 0u).collect { state ->
                    if (state is ProvisioningState.CapabilitiesReceived) {
                        state.start(state.defaultParameters.copy(networkKey = foreignKey))
                    }
                }
            }
        }
    }

    @Test
    fun `device key refresh aborts before sending data when the element count changed`() {
        val f = Fixture()
        // 노드가 2 element 로 등록돼 있는데 3 을 보고한다 = composition 이 바뀐 것.
        val bearer = FakeProvisioneeBearer(numberOfElements = 3)
        runBlocking { bearer.open() }

        assertFailsWith<InvalidNodeProvisioningProtocolInterfaceState> {
            nppiManager(f, DEVICE_KEY_REFRESH, bearer).runToCompletion()
        }

        assertNull(
            bearer.receivedProvisioningData,
            "★ Provisioning Data 를 보내기 **전에** 중단해야 한다 — 보낸 뒤 실패하면 노드는 " +
                "새 Device Key 를 갖고 CDB 는 옛 키를 갖는 어긋난 상태가 된다",
        )
        assertContentEquals(OLD_DEVICE_KEY, f.target.deviceKey, "CDB 는 그대로")
        assertEquals(TARGET_ELEMENTS, f.target.elementsCount, "Element 개수도 그대로")
    }

    @Test
    fun `nppi refuses a node that does not belong to the network`() {
        val f = Fixture()
        val other = Fixture()

        assertFailsWith<InvalidNodeProvisioningProtocolInterfaceState> {
            ProvisioningManager(
                node = other.target,
                procedure = DEVICE_KEY_REFRESH,
                meshNetwork = f.network,
                bearer = FakeProvisioneeBearer(),
                ioDispatcher = Dispatchers.Unconfined,
            )
        }
    }

    @Test
    fun `nppi refuses the local provisioner's own node`() {
        val f = Fixture()
        val provisionerNode = assertNotNull(
            f.network.localProvisioner?.node,
            "local Provisioner 노드가 있어야 한다",
        )

        assertFailsWith<InvalidNodeProvisioningProtocolInterfaceState>(
            message = "로컬 Provisioner 의 Device Key 는 무선으로 갱신하는 물건이 아니다",
        ) {
            ProvisioningManager(
                node = provisionerNode,
                procedure = DEVICE_KEY_REFRESH,
                meshNetwork = f.network,
                bearer = FakeProvisioneeBearer(),
                ioDispatcher = Dispatchers.Unconfined,
            )
        }
    }

    @Test
    fun `the manager and the bearer must agree on the procedure`() {
        // PB-Remote 베어러는 절차 옥텟을 Link Open 에 실어 보낸다. 둘이 어긋나면 프로비저너와
        // 노드가 "방금 무슨 일이 있었나" 에 대해 다른 결론을 내린다.
        val f = Fixture()
        val bearer = FakeProvisioneeBearer()

        // FakeProvisioneeBearer 는 PBRemoteBearer 가 아니므로 검사 대상이 아니다 — 통과해야 한다.
        ProvisioningManager(
            node = f.target,
            procedure = NODE_COMPOSITION_REFRESH,
            meshNetwork = f.network,
            bearer = bearer,
            ioDispatcher = Dispatchers.Unconfined,
        ).let {
            assertEquals(NODE_COMPOSITION_REFRESH, it.nodeProvisioningProtocolInterfaceProcedure)
        }
    }

    @Test
    fun `nppi survives a non-zero iv index`() {
        // MshPRT 1.1: Provisioning Data 는 NetworkKey(16) | NetKeyIndex(2) | Flags(1) |
        // IVIndex(4) | UnicastAddress(2) 순서다. 노드는 IV Index 가 자기 것과 다르면
        // NPPI 를 거부한다(Zephyr refresh_is_valid(): `if (iv_index != bt_mesh.iv_index)`).
        // IV Index 0 인 망에서는 Flags/IVIndex 순서가 뒤바뀌어도 전부 0 이라 증상이 없으므로,
        // 반드시 0 이 아닌 IV Index 로 확인해야 한다.
        val f = Fixture(ivIndex = 0x0000_1234u)
        val bearer = FakeProvisioneeBearer(
            numberOfElements = TARGET_ELEMENTS,
            expectedNetworkKey = f.netKey.key,
            expectedIvIndex = 0x0000_1234u,
            expectedAddress = TARGET_ADDRESS.toUShort(),
        )
        runBlocking { bearer.open() }

        val states = nppiManager(f, DEVICE_KEY_REFRESH, bearer).runToCompletion()

        val data = assertNotNull(bearer.receivedProvisioningData, "디바이스가 Data PDU 를 받았어야 한다")
        assertEquals(25, data.size, "Provisioning Data 는 25 옥텟")
        assertContentEquals(
            expected = f.netKey.key,
            actual = data.sliceArray(indices = 0 until 16),
            message = "[0..15] Network Key",
        )
        assertEquals(0, data[16].toInt() and 0xFF, "[16..17] NetKey Index (big endian)")
        assertEquals(0, data[17].toInt() and 0xFF, "[16..17] NetKey Index (big endian)")
        assertEquals(
            0x00, data[18].toInt() and 0xFF,
            "[18] Flags — Key Refresh / IV Update 둘 다 비활성",
        )
        assertContentEquals(
            expected = byteArrayOf(0x00, 0x00, 0x12, 0x34),
            actual = data.sliceArray(indices = 19 until 23),
            message = "[19..22] IV Index (big endian). ★ 종전 라이브러리는 Flags 와 IV Index 의 " +
                "순서를 바꿔 넣었다. IV Index 0 인 망에서는 두 필드가 모두 0 이라 증상이 없고, " +
                "0 이 아닌 순간 노드가 엉뚱한 IV Index 를 받는다(NPPI 는 IV Index 불일치를 " +
                "거부하므로 곧바로 실패)",
        )
        assertEquals(
            TARGET_ADDRESS, ((data[23].toInt() and 0xFF) shl 8) or (data[24].toInt() and 0xFF),
            "[23..24] Unicast Address (big endian)",
        )
        assertNull(
            bearer.rejection,
            "Provisioning Data 의 IV Index 가 노드의 것과 일치해야 한다",
        )
        assertTrue(states.last() is ProvisioningState.Complete)
        assertContentEquals(bearer.derivedDeviceKey, f.target.deviceKey, "Device Key 교체")
    }

    @Test
    fun `provisioning data carries the iv update flag in the flags octet`() {
        // Flags 옥텟은 IV Index **앞**에 있다. IV Update 가 진행 중이면 bit 1 이 서고, 그 비트가
        // IV Index 자리에 실리면 노드가 IV Index 를 통째로 잘못 읽는다.
        val f = Fixture()
        f.network.setIvIndex(index = 0x0000_00ABu, isIvUpdateActive = true)
        val bearer = FakeProvisioneeBearer(
            numberOfElements = TARGET_ELEMENTS,
            expectedNetworkKey = f.netKey.key,
            expectedAddress = TARGET_ADDRESS.toUShort(),
        )
        runBlocking { bearer.open() }

        nppiManager(f, DEVICE_KEY_REFRESH, bearer).runToCompletion()

        val data = assertNotNull(bearer.receivedProvisioningData)
        assertEquals(
            0x02, data[18].toInt() and 0xFF,
            "[18] Flags — IV Update Active = bit 1 (MshPRT 1.1 §5.4.2.5)",
        )
        assertContentEquals(
            expected = byteArrayOf(0x00, 0x00, 0x00, 0xAB.toByte()),
            actual = data.sliceArray(indices = 19 until 23),
            message = "[19..22] IV Index 는 Flags 뒤에 온다",
        )
    }
}
