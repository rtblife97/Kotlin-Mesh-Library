package no.nordicsemi.kotlin.mesh.core.model

import no.nordicsemi.kotlin.mesh.core.exception.AddressAlreadyInUse
import no.nordicsemi.kotlin.mesh.core.exception.DoesNotBelongToNetwork
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure.DEVICE_KEY_REFRESH
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure.NODE_ADDRESS_REFRESH
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure.NODE_COMPOSITION_REFRESH
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
 * simdo-patch (2026-08-26) — Node Provisioning Protocol Interface (MshPRT 1.1 §3.11.8) 의
 * **종결 규칙** 봉인.
 *
 * ## 왜 이 테스트가 있는가
 *
 * NPPI 세 절차는 provisioning 프로토콜을 통째로 재사용한다 — 같은 PDU 흐름, 같은 crypto.
 * 다른 것은 **끝났을 때 CDB 에 무엇을 쓰는가** 뿐이고, 바로 그 부분이 라이브러리에 없었다.
 * 종전에 존재하던 유일한 종결 경로는 `MeshNetwork.remove(uuid)` + `add(node)` 였는데
 *
 * - `remove()` 는 옛 주소를 `networkExclusions` 에 넣어 **같은 주소로의 재-add 를 스스로 막고**
 *   (Device Key Refresh / Composition Refresh 는 주소를 유지해야 하므로 즉사),
 * - 노드의 AppKey 바인딩 · Publication · Subscription · NetKey/AppKey 목록을 전부 잃는다.
 *   NPPI 가 "보존한다" 고 약속한 바로 그 상태다.
 *
 * ## 무엇이 권위인가
 *
 * - Bluetooth SIG, *Mesh Remote Provisioning*: Device Key Refresh — "Existing data such as the
 *   node's element addresses and its lists of NetKeys and AppKeys are unaffected."
 *   Node Composition Refresh — "The Composition Data state of the node is updated by the
 *   procedure, but other states are left unchanged."
 * - Zephyr(NCS v3.4.0) `provisioner.c prov_node_add()`: `bt_mesh_cdb_node_key_import()` +
 *   `bt_mesh_cdb_node_update(node, addr, elem_count)` — **in-place**.
 * - 주소 규칙은 노드 쪽 `provisionee.c refresh_is_valid()` 가 강제한다.
 */
@OptIn(ExperimentalUuidApi::class)
class NodeProvisioningProtocolInterfaceTest {

    private companion object {
        val OLD_DEVICE_KEY = ByteArray(16) { 0xAA.toByte() }
        val NEW_DEVICE_KEY = ByteArray(16) { 0xBB.toByte() }

        const val TARGET_ADDRESS = 0x0100
        const val NEIGHBOUR_ADDRESS = 0x0200
        const val RELOCATED_ADDRESS = 0x0300
    }

    private class Fixture(elementCount: Int = 3) {
        val network = MeshNetwork(name = "NPPI Test Network").apply {
            add(name = "Primary Network Key", index = 0u)
        }
        val netKey: NetworkKey = network.networkKeys.first()
        val appKey: ApplicationKey = network.add(name = "App Key", boundNetworkKey = netKey)

        val target: Node = Node(
            name = "Target",
            uuid = Uuid.random(),
            deviceKey = OLD_DEVICE_KEY,
            unicastAddress = UnicastAddress(address = TARGET_ADDRESS),
            elementCount = elementCount,
            assignedNetworkKey = netKey,
            security = Insecure,
        ).also { network.add(node = it) }

        /** 대상 노드 옆에 있는 무관한 노드 — 주소 충돌 검사가 진짜로 도는지 확인용. */
        val neighbour: Node = Node(
            name = "Neighbour",
            uuid = Uuid.random(),
            deviceKey = ByteArray(16) { 0xCC.toByte() },
            unicastAddress = UnicastAddress(address = NEIGHBOUR_ADDRESS),
            elementCount = 2,
            assignedNetworkKey = netKey,
            security = Insecure,
        ).also { network.add(node = it) }

        init {
            // 노드에 "보존되어야 할 상태" 를 심는다: AppKey 목록 + primary element 의 모델 하나에
            // AppKey 바인딩 · Publication · Subscription. NPPI 가 이걸 건드리면 안 된다.
            target.addAppKey(index = appKey.index)
            target.companyIdentifier = 0x0059u
            target.productIdentifier = 0x0001u
            target.replayProtectionCount = 255u
            target.setDefaultTtl(ttl = 40u)

            val model = Model(modelId = SigModelId(0x1000u))
            target.primaryElement.add(model = model)
            model.bind(index = appKey.index)
            model.subscribe(group = Group("G", GroupAddress(0xC000u)).also { network.add(it) })
        }

        val boundModel: Model get() = target.primaryElement.models.first()
    }

    // ----------------------------------------------------------------------------------------
    // blocker ① — self-exclusion
    // ----------------------------------------------------------------------------------------

    @Test
    fun `a node's own address range is unavailable unless the node itself is ignored`() {
        val f = Fixture()

        assertFalse(
            f.network.isAddressRangeAvailable(range = f.target.unicastRange),
            "종전 동작: 대상 노드가 쓰고 있으므로 '사용 중'. 이것 때문에 주소를 유지해야 하는 " +
                "NPPI 0x00/0x02 가 시작조차 못 했다",
        )
        assertTrue(
            f.network.isAddressRangeAvailable(range = f.target.unicastRange, ignoring = f.target),
            "자기 자신을 제외하면 사용 가능해야 한다 — in-place 갱신의 전제",
        )
    }

    @Test
    fun `self-exclusion does not hide a collision with another node`() {
        val f = Fixture()

        assertFalse(
            f.network.isAddressRangeAvailable(
                range = f.neighbour.unicastRange,
                ignoring = f.target,
            ),
            "이웃 노드의 점유는 self-exclusion 과 무관하게 그대로 감지되어야 한다",
        )
    }

    @Test
    fun `self-exclusion does not bypass the network exclusion list`() {
        val f = Fixture()
        // 살아있는 노드의 주소가 exclusion list 에 들어 있는 상태 = CDB 불일치. 그대로 NPPI 를
        // 강행하면 다른 노드가 곧 같은 주소를 배정받는다 → 실패하는 편이 옳다.
        f.network._networkExclusions.add(
            ExclusionList(ivIndex = f.network.ivIndex.index).apply {
                network = f.network
                exclude(address = f.target.primaryUnicastAddress)
            }
        )

        assertFalse(
            f.network.isAddressRangeAvailable(range = f.target.unicastRange, ignoring = f.target),
            "exclusion list 는 self-exclusion 대상이 아니다",
        )
    }

    // ----------------------------------------------------------------------------------------
    // 0x00 — Device Key Refresh (§3.11.8.4)
    // ----------------------------------------------------------------------------------------

    @Test
    fun `device key refresh replaces only the device key`() {
        val f = Fixture()
        val elementsBefore = f.target.elements.toList()

        f.network.applyNodeProvisioningProtocolInterfaceResult(
            node = f.target,
            procedure = DEVICE_KEY_REFRESH,
            deviceKey = NEW_DEVICE_KEY,
            unicastAddress = f.target.primaryUnicastAddress,
            elementCount = f.target.elementsCount,
            security = Insecure,
        )

        assertContentEquals(NEW_DEVICE_KEY, f.target.deviceKey, "Device Key 는 교체된다")
        assertEquals(
            UnicastAddress(address = TARGET_ADDRESS), f.target.primaryUnicastAddress,
            "주소는 유지 — SIG: element addresses are unaffected",
        )
        assertEquals(3, f.target.elementsCount, "Element 개수 유지")
        assertContentEquals(
            elementsBefore, f.target.elements,
            "Element 객체 자체가 유지되어야 한다 (재생성 금지)",
        )
        assertNodeConfigurationPreserved(f)
        assertEquals(
            0x0059u.toUShort(), f.target.companyIdentifier,
            "composition 은 바뀌지 않으므로 재독해가 필요 없다",
        )
        assertTrue(
            f.network.networkExclusions.isEmpty(),
            "주소가 그대로이므로 exclusion 이 생기면 안 된다 — 생기면 다음 NPPI 가 막힌다",
        )
        assertSame(f.target, f.network.node(uuid = f.target.uuid), "같은 Node 인스턴스 유지")
    }

    @Test
    fun `device key refresh rejects an address change`() {
        val f = Fixture()

        assertFailsWith<AddressAlreadyInUse>(
            message = "0x00 은 주소를 바꿀 수 없다 — 노드도 refresh_is_valid() 에서 거부한다",
        ) {
            f.network.applyNodeProvisioningProtocolInterfaceResult(
                node = f.target,
                procedure = DEVICE_KEY_REFRESH,
                deviceKey = NEW_DEVICE_KEY,
                unicastAddress = UnicastAddress(address = RELOCATED_ADDRESS),
                elementCount = f.target.elementsCount,
                security = Insecure,
            )
        }
        assertContentEquals(OLD_DEVICE_KEY, f.target.deviceKey, "거부 시 아무것도 바뀌지 않는다")
    }

    // ----------------------------------------------------------------------------------------
    // 0x01 — Node Address Refresh (§3.11.8.5)
    // ----------------------------------------------------------------------------------------

    @Test
    fun `address refresh moves the node and keeps its configuration`() {
        val f = Fixture()

        f.network.applyNodeProvisioningProtocolInterfaceResult(
            node = f.target,
            procedure = NODE_ADDRESS_REFRESH,
            deviceKey = NEW_DEVICE_KEY,
            unicastAddress = UnicastAddress(address = RELOCATED_ADDRESS),
            elementCount = 3,
            security = Insecure,
        )

        assertContentEquals(NEW_DEVICE_KEY, f.target.deviceKey, "Device Key 도 함께 교체된다")
        assertEquals(
            UnicastAddress(address = RELOCATED_ADDRESS), f.target.primaryUnicastAddress,
            "primary element 주소 이동",
        )
        assertEquals(
            UnicastAddress(address = RELOCATED_ADDRESS + 2),
            f.target.elements[2].unicastAddress,
            "나머지 element 주소도 따라 움직인다 (primary + index)",
        )
        assertNodeConfigurationPreserved(f)
        assertEquals(
            0x0059u.toUShort(), f.target.companyIdentifier,
            "Element 개수가 그대로면 composition 재독해가 필요 없다 (순수 재배치)",
        )
        assertSame(f.target, f.network.node(uuid = f.target.uuid), "같은 Node 인스턴스 유지")
    }

    @Test
    fun `address refresh excludes the previous address range`() {
        val f = Fixture()
        val previousAddresses = f.target.addresses

        f.network.applyNodeProvisioningProtocolInterfaceResult(
            node = f.target,
            procedure = NODE_ADDRESS_REFRESH,
            deviceKey = NEW_DEVICE_KEY,
            unicastAddress = UnicastAddress(address = RELOCATED_ADDRESS),
            elementCount = 3,
            security = Insecure,
        )

        // 노드는 주소가 바뀌는 순간 seq 를 0 으로 리셋한다(Zephyr main.c bt_mesh_reprovision()).
        // 다른 노드들의 RPL 에는 옛 주소의 SeqAuth 가 남아 있으므로 옛 주소를 곧바로 다른 노드에
        // 주면 그 노드의 메시지가 조용히 버려진다.
        previousAddresses.forEach { old ->
            assertFalse(
                f.network.isAddressAvailable(address = old, elementCount = 1),
                "옛 주소 $old 는 IV Index 가 2 오를 때까지 재사용 금지",
            )
        }

        val provisioner = Provisioner(
            name = "P",
            allocatedUnicastRanges = mutableListOf(UnicastRange(0x0001, 0x7FFF)),
        ).apply { network = f.network }
        val next = f.network.nextAvailableUnicastAddress(elementCount = 1, provisioner = provisioner)
        assertNotNull(next)
        assertFalse(
            next.address.toInt() in TARGET_ADDRESS until TARGET_ADDRESS + 3,
            "주소 할당기도 옛 범위를 건너뛰어야 한다",
        )
    }

    @Test
    fun `address refresh rejects keeping the same address`() {
        val f = Fixture()

        assertFailsWith<AddressAlreadyInUse>(
            message = "0x01 은 주소가 반드시 바뀌어야 한다 — 노드도 그렇게 검사한다",
        ) {
            f.network.applyNodeProvisioningProtocolInterfaceResult(
                node = f.target,
                procedure = NODE_ADDRESS_REFRESH,
                deviceKey = NEW_DEVICE_KEY,
                unicastAddress = f.target.primaryUnicastAddress,
                elementCount = 3,
                security = Insecure,
            )
        }
    }

    @Test
    fun `address refresh rejects a range that overlaps the previous one`() {
        val f = Fixture()

        // 새 primary 가 옛 primary 보다 작아 노드 쪽 검사(addr < old_addr)는 통과하지만
        // 범위는 겹친다: [0x00FF..0x0101] vs [0x0100..0x0102].
        assertFailsWith<AddressAlreadyInUse> {
            f.network.applyNodeProvisioningProtocolInterfaceResult(
                node = f.target,
                procedure = NODE_ADDRESS_REFRESH,
                deviceKey = NEW_DEVICE_KEY,
                unicastAddress = UnicastAddress(address = TARGET_ADDRESS - 1),
                elementCount = 3,
                security = Insecure,
            )
        }
    }

    @Test
    fun `address refresh may grow the element count and then invalidates composition data`() {
        val f = Fixture()

        f.network.applyNodeProvisioningProtocolInterfaceResult(
            node = f.target,
            procedure = NODE_ADDRESS_REFRESH,
            deviceKey = NEW_DEVICE_KEY,
            unicastAddress = UnicastAddress(address = RELOCATED_ADDRESS),
            elementCount = 5,
            security = Insecure,
        )

        assertEquals(
            5, f.target.elementsCount,
            "Nordic main.h:538 — 'Composition data may change, including the number of elements.' " +
                "권위는 이번 세션의 Provisioning Capabilities",
        )
        assertEquals(
            UnicastAddress(address = RELOCATED_ADDRESS + 4), f.target.lastUnicastAddress,
            "점유 범위가 즉시 넓어져야 한다 — 늦추면 그 사이 다른 노드에 겹치는 주소가 배정된다",
        )
        assertNull(
            f.target.companyIdentifier,
            "Element 개수가 바뀌었으면 CDB 의 Model 목록은 신뢰할 수 없다 → CDP0 재독해 유도",
        )
        assertNodeConfigurationPreserved(f)
    }

    // ----------------------------------------------------------------------------------------
    // 0x02 — Node Composition Refresh (§3.11.8.6)
    // ----------------------------------------------------------------------------------------

    @Test
    fun `composition refresh keeps the address and invalidates composition data`() {
        val f = Fixture()

        f.network.applyNodeProvisioningProtocolInterfaceResult(
            node = f.target,
            procedure = NODE_COMPOSITION_REFRESH,
            deviceKey = NEW_DEVICE_KEY,
            unicastAddress = f.target.primaryUnicastAddress,
            elementCount = 3,
            security = Insecure,
        )

        assertContentEquals(NEW_DEVICE_KEY, f.target.deviceKey, "Device Key 교체")
        assertEquals(
            UnicastAddress(address = TARGET_ADDRESS), f.target.primaryUnicastAddress,
            "주소 유지 — 0x02 는 composition 만 바꾼다",
        )
        assertFalse(
            f.target.isCompositionDataReceived,
            "Page 128 이 Page 0 이 됐으므로 CDB 의 composition 은 stale → 재독해 필요",
        )
        assertTrue(
            f.network.networkExclusions.isEmpty(),
            "주소가 그대로이므로 exclusion 이 생기면 안 된다",
        )
        assertNodeConfigurationPreserved(f)
    }

    @Test
    fun `composition refresh keeps existing elements so that a page 0 re-read can merge bindings`() {
        val f = Fixture()

        f.network.applyNodeProvisioningProtocolInterfaceResult(
            node = f.target,
            procedure = NODE_COMPOSITION_REFRESH,
            deviceKey = NEW_DEVICE_KEY,
            unicastAddress = f.target.primaryUnicastAddress,
            elementCount = 4,
            security = Insecure,
        )

        assertEquals(4, f.target.elementsCount, "새 Element 개수 반영")
        assertEquals(
            1, f.target.primaryElement.models.size,
            "살아남는 index 의 Element 는 그대로 둔다 — Node.set(elements) 가 CDP0 재독해 때 " +
                "위치·Model ID 가 일치하는 모델의 바인딩/pub/sub 을 복사할 수 있어야 한다",
        )
        assertNodeConfigurationPreserved(f)
    }

    @Test
    fun `composition refresh rejects an address change`() {
        val f = Fixture()

        assertFailsWith<AddressAlreadyInUse> {
            f.network.applyNodeProvisioningProtocolInterfaceResult(
                node = f.target,
                procedure = NODE_COMPOSITION_REFRESH,
                deviceKey = NEW_DEVICE_KEY,
                unicastAddress = UnicastAddress(address = RELOCATED_ADDRESS),
                elementCount = 3,
                security = Insecure,
            )
        }
    }

    @Test
    fun `shrinking the element count drops the trailing elements`() {
        val f = Fixture()

        f.network.applyNodeProvisioningProtocolInterfaceResult(
            node = f.target,
            procedure = NODE_COMPOSITION_REFRESH,
            deviceKey = NEW_DEVICE_KEY,
            unicastAddress = f.target.primaryUnicastAddress,
            elementCount = 1,
            security = Insecure,
        )

        assertEquals(1, f.target.elementsCount, "줄어든 개수 반영")
        assertEquals(
            UnicastAddress(address = TARGET_ADDRESS), f.target.lastUnicastAddress,
            "풀려난 주소는 즉시 다른 노드에 배정 가능해야 한다",
        )
        assertTrue(
            f.network.isAddressAvailable(
                address = UnicastAddress(address = TARGET_ADDRESS + 1),
                elementCount = 1,
            ),
            "줄어든 만큼의 주소가 실제로 해제되어야 한다",
        )
    }

    // ----------------------------------------------------------------------------------------
    // 공통 사전조건
    // ----------------------------------------------------------------------------------------

    @Test
    fun `a node outside the network cannot be refreshed`() {
        val f = Fixture()
        val stranger = Node(name = "Stranger", address = 0x0500, elements = 1)

        assertFailsWith<DoesNotBelongToNetwork> {
            f.network.applyNodeProvisioningProtocolInterfaceResult(
                node = stranger,
                procedure = DEVICE_KEY_REFRESH,
                deviceKey = NEW_DEVICE_KEY,
                unicastAddress = stranger.primaryUnicastAddress,
                elementCount = 1,
                security = Insecure,
            )
        }
    }

    @Test
    fun `every procedure replaces the device key`() {
        // "세 절차 모두 Device Key 를 바꾼다" 는 NPPI 의 유일한 공통 불변식이다.
        listOf(
            DEVICE_KEY_REFRESH to TARGET_ADDRESS,
            NODE_ADDRESS_REFRESH to RELOCATED_ADDRESS,
            NODE_COMPOSITION_REFRESH to TARGET_ADDRESS,
        ).forEach { (procedure: NodeProvisioningProtocolInterfaceProcedure, address: Int) ->
            val f = Fixture()
            f.network.applyNodeProvisioningProtocolInterfaceResult(
                node = f.target,
                procedure = procedure,
                deviceKey = NEW_DEVICE_KEY,
                unicastAddress = UnicastAddress(address = address),
                elementCount = 3,
                security = Insecure,
            )
            assertContentEquals(
                NEW_DEVICE_KEY, f.target.deviceKey,
                "$procedure 도 Device Key 를 교체해야 한다",
            )
        }
    }

    /**
     * NPPI 가 **보존하기로 약속한** 상태 — 세 절차 전부에서 같아야 한다.
     * SIG: "Existing data such as the node's list of NetKeys and AppKeys are unaffected."
     */
    private fun assertNodeConfigurationPreserved(f: Fixture) {
        assertEquals(listOf(0u.toUShort()), f.target.netKeys.map { it.index }, "NetKey 목록 보존")
        assertEquals(
            listOf(f.appKey.index), f.target.appKeys.map { it.index },
            "AppKey 목록 보존",
        )
        assertEquals(
            listOf(f.appKey.index), f.boundModel.bind.toList(),
            "Model AppKey 바인딩 보존",
        )
        // `subscribe` getter 는 primary element 모델에 All Nodes(0xFFFF)를 항상 덧붙이므로
        // 명시적으로 설정한 구독만 보는 backing list 를 확인한다.
        assertEquals(
            listOf(GroupAddress(0xC000u) as SubscriptionAddress),
            f.boundModel._subscribe.toList(),
            "Model Subscription 보존",
        )
        assertEquals(40u.toUByte(), f.target.defaultTTL, "Default TTL 은 foundation state — 보존")
        assertEquals("Target", f.target.name, "이름 보존")
    }
}
