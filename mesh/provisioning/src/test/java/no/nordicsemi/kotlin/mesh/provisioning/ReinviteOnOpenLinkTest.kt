package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Provisioner
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.model.UnicastRange
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * 링크가 닫히지 않은 채 다시 Invite 하면 기기는 Provisioning Failed(Unexpected PDU) 로 거부한다 — 링크가 실제로 닫혀야
 * 다시 받는다 (simdo-fork, 2026-09-28).
 *
 * 실기기 2026-09-28: 안드로이드가 GATT 를 닫고 0.5 s 만에 다시 연결하면 이전 LE 링크를 그대로 재사용한다 → 조명(Zephyr)
 * 쪽 PB-GATT 링크는 닫힌 적이 없어 새 Invite 를 `0903` 으로 거부했다 (앱에는 InvalidPdu "디바이스가 잘못된 PDU").
 * 가짜 기기([FakeProvisioneeBearer])는 Zephyr 처럼 링크당 Invite 한 번만 받고, 링크가 닫혀야 초기화된다 — 이 제약을
 * 가짜가 담고 있어야 재점멸 같은 흐름이 단위 테스트에서 통과하고 실기기에서 실패하는 일이 없다.
 *
 * 참고: 라이브러리는 Invite 에 대한 Failed 를 오류 코드 없이 [InvalidPdu] 로 올린다 (다른 단계는 RemoteError →
 * ProvisioningState.Failed). 커미셔닝 퍼널·메인 앱이 이 단계 실패를 예외로 받고 있어 바꾸지 않았다.
 */
@OptIn(ExperimentalUuidApi::class)
class ReinviteOnOpenLinkTest {

    private val network: MeshNetwork = runBlocking {
        MeshNetworkManager(
            storage = InMemoryStorage(),
            secureProperties = InMemorySecureProperties(),
            ioDispatcher = Dispatchers.Unconfined,
        ).create(
            name = "Reinvite",
            provisioner = Provisioner(uuid = Uuid.random(), name = "Local").apply {
                allocate(range = UnicastRange(lowAddress = UnicastAddress(0x0001), highAddress = UnicastAddress(0x00FF)))
            },
        )
    }

    private fun manager(bearer: FakeProvisioneeBearer) = ProvisioningManager(
        unprovisionedDevice = UnprovisionedDevice(name = "emblazeTEST-5508", uuid = Uuid.random()),
        meshNetwork = network,
        bearer = bearer,
        ioDispatcher = Dispatchers.Unconfined,
    )

    /** Capabilities 까지 받고 멈춘다 (커미셔닝 앱의 Identify). */
    private fun identify(bearer: FakeProvisioneeBearer): ProvisioningState = runBlocking {
        manager(bearer).provision(attentionTimer = 50u).first { it is ProvisioningState.CapabilitiesReceived }
    }

    @Test
    fun `second Invite on a link the device still holds is rejected - accepted again only after the link closes`() {
        val bearer = FakeProvisioneeBearer(numberOfElements = 5)
        runBlocking { bearer.open() }
        assertTrue(identify(bearer) is ProvisioningState.CapabilitiesReceived)

        // 앱은 세션을 버렸지만 링크(BLE)는 닫히지 않았다 — 안드로이드의 link idle 1 s 안에 다시 연결한 경우.
        assertFailsWith<InvalidPdu> { identify(bearer) }

        // 링크가 실제로 닫히면(기기 FSM 초기화) 다시 Invite 를 받는다.
        runBlocking {
            bearer.close()
            bearer.open()
        }
        assertTrue(identify(bearer) is ProvisioningState.CapabilitiesReceived)
    }
}
