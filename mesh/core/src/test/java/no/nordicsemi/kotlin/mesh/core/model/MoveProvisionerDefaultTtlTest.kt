package no.nordicsemi.kotlin.mesh.core.model

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-08-25) — `MeshNetwork.moveProvisioner` 가 Provisioner 노드의
 * **Default TTL** 을 지우지 않음을 봉인한다.
 *
 * ## 왜 이 테스트가 있는가
 *
 * `moveProvisioner` 는 local Provisioner 가 바뀔 때 그 노드의 Composition Data Page 0
 * (Mesh Profile 1.0.1 §4.2.1 — CID / PID / VID / Elements) 를 지운다. 그 데이터는
 * "어느 폰이 이 Provisioner 를 들고 있는가" 에 종속이라 export/import 하면 무의미해지기
 * 때문이다. 그런데 `defaultTTL` 이 그 목록에 함께 들어 있었다.
 *
 * `defaultTTL` 은 Composition Data 가 아니라 Foundation 계층 configuration state
 * (§4.2.7, Config Default TTL Get/Set §4.3.2.24-25) 다. 운영자가 "이 망은 홉이 깊다" 고
 * 판단해 고른 네트워크 정책이며 폰 교체와 무관하게 유지되어야 한다.
 *
 * 실측 회귀(신동 광산): CDB 의 provisioner 12개가 전부 `defaultTTL = 40` 인데, 앱이 자기
 * provisioner 를 local 로 올리려고 `move(to = 0)` 하는 순간 구 local·신 local 양쪽의 40 이
 * 지워졌다. 송신 TTL 우선순위 체인이
 * `initialTtl ?: localProvisioner.node.defaultTTL ?: networkParameters.defaultTtl`
 * 이므로 값이 사라지면 조용히 라이브러리 기본값(5) 으로 떨어진다 = 최대 5홉(≈750 m).
 * 실측 21~23홉(3,377 m) 갱도에서 원격 노드 제어·설정이 구조적으로 불가능해졌다.
 */
@OptIn(ExperimentalUuidApi::class)
class MoveProvisionerDefaultTtlTest {

    private companion object {
        /** 신동 CDB 의 provisioner 12개가 실제로 들고 있던 값. */
        const val DEEP_HOP_TTL: UByte = 40u
    }

    private fun network(): MeshNetwork = MeshNetwork(name = "TTL Test Network").apply {
        // Provisioner 노드 생성이 netKeys 를 요구한다 (NoNetworkKeysAdded).
        add(name = "Primary Network Key")
    }

    private fun MeshNetwork.addProvisioner(
        name: String,
        rangeLow: Int,
        rangeHigh: Int,
        address: Int,
    ): Provisioner = Provisioner(uuid = Uuid.random(), name = name).apply {
        allocate(
            range = UnicastRange(
                lowAddress = UnicastAddress(rangeLow),
                highAddress = UnicastAddress(rangeHigh),
            )
        )
    }.also { add(provisioner = it, address = UnicastAddress(address)) }

    @Test
    fun `move(to = 0) preserves defaultTTL of both the old and the new local Provisioner`() {
        val network = network()
        val first = network.addProvisioner("P-first", 0x0001, 0x000F, 0x0001)
        val second = network.addProvisioner("P-second", 0x0010, 0x001F, 0x0010)

        // 운영자가 nRF Mesh 앱에서 provisioner 별로 손으로 넣은 값 재현.
        first.node?.defaultTTL = DEEP_HOP_TTL
        second.node?.defaultTTL = DEEP_HOP_TTL

        assertEquals(expected = first.uuid, actual = network.localProvisioner?.uuid)

        network.move(provisioner = second, to = 0)

        assertEquals(expected = second.uuid, actual = network.localProvisioner?.uuid)
        assertEquals(
            expected = DEEP_HOP_TTL,
            actual = second.node?.defaultTTL,
            message = "새 local Provisioner 의 Default TTL 이 지워지면 송신 TTL 이 " +
                "networkParameters 기본값(5) 으로 떨어진다",
        )
        assertEquals(
            expected = DEEP_HOP_TTL,
            actual = first.node?.defaultTTL,
            message = "구 local Provisioner 의 Default TTL 도 보존되어야 한다 — 다른 폰이 " +
                "이 CDB 를 import 했을 때 되살아나야 하므로",
        )
    }

    @Test
    fun `move(to = 0) still clears Composition Data Page 0 of the old local Provisioner`() {
        val network = network()
        val first = network.addProvisioner("P-first", 0x0001, 0x000F, 0x0001)
        val second = network.addProvisioner("P-second", 0x0010, 0x001F, 0x0010)

        first.node?.apply {
            productIdentifier = 0x1234u
            versionIdentifier = 0x0001u
        }
        // add(provisioner) 가 첫 Provisioner 노드에 Google CID 를 심어둔다.
        assertNotNull(first.node?.companyIdentifier)

        network.move(provisioner = second, to = 0)

        // Composition Data 3필드는 종전대로 지워져야 한다 (이번 패치의 범위 밖).
        assertNull(first.node?.companyIdentifier)
        assertNull(first.node?.productIdentifier)
        assertNull(first.node?.versionIdentifier)
        // 새 local 은 Google CID 로 재설정된다.
        assertEquals(expected = 0x00E0u.toUShort(), actual = second.node?.companyIdentifier)
    }

    /**
     * simdo-patch — `Node.setDefaultTtl` public mutator.
     *
     * local Provisioner 의 self-node 는 자기 자신에게 Config 메시지를 보내지 않으므로
     * 라이브러리 내부의 `ConfigDefaultTtlStatus` 수신 / `ConfigDefaultTtlSet` 처리 경로로
     * 절대 채워지지 않는다. 상위 stack 이 운영 정책(깊은 홉 = 높은 TTL)을 CDB 에 기록하려면
     * 이 mutator 가 유일한 경로다.
     */
    @Test
    fun `setDefaultTtl writes the value and rejects out-of-range input`() {
        val network = network()
        val provisioner = network.addProvisioner("P-only", 0x0001, 0x000F, 0x0001)
        val node = assertNotNull(provisioner.node)

        node.setDefaultTtl(DEEP_HOP_TTL)
        assertEquals(expected = DEEP_HOP_TTL, actual = node.defaultTTL)

        node.setDefaultTtl(null)
        assertNull(node.defaultTTL)

        // Config Default TTL Set (§4.3.2.25) 유효값 = 0 또는 2..127. 1 은 prohibited.
        node.setDefaultTtl(0u)
        assertEquals(expected = 0u.toUByte(), actual = node.defaultTTL)

        assertFailsWith<IllegalArgumentException> { node.setDefaultTtl(1u) }
        assertFailsWith<IllegalArgumentException> { node.setDefaultTtl(128u) }
    }

    @Test
    fun `move(to = 0) leaves defaultTTL null when it was never configured`() {
        val network = network()
        val first = network.addProvisioner("P-first", 0x0001, 0x000F, 0x0001)
        val second = network.addProvisioner("P-second", 0x0010, 0x001F, 0x0010)

        network.move(provisioner = second, to = 0)

        // 보존은 "값을 만들어내는" 것이 아니다 — 미설정이면 그대로 null 이고,
        // 송신 TTL 은 networkParameters.defaultTtl 로 폴백한다.
        assertNull(first.node?.defaultTTL)
        assertNull(second.node?.defaultTTL)
    }
}
