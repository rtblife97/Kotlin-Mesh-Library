package no.nordicsemi.kotlin.mesh.core.layers.access

import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigAppKeyStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigBeaconGet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigBeaconStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigCompositionDataGet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigDefaultTtlStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNetKeyStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNodeReset
import no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu.FirmwareDistributionStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu.FirmwareUpdateInformationGet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningExtendedScanReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkOpen
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUOutboundReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanCapabilitiesStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanStatus
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkState
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningScanState
import no.nordicsemi.kotlin.data.toUuid
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi

/**
 * `AccessLayer` 의 Device Key 수신 경로에서 **어떤 메시지가 네트워크 저장을 유발하는가** 를
 * 봉인하는 회귀 가드 (simdo-fork, 2026-08-12 — 감사 P1-4).
 *
 * ## 배경
 *
 * `AccessLayer.handle()` 의 Device Key 분기는 로컬 노드로 온 메시지마다
 * `NetworkManagerEvent.OnNetworkChanged` 를 emit 하고, `MeshNetworkManager` 는 그것을
 * **`save()` = 네트워크 전체 직렬화 + `Storage.save()`** 로 받는다.
 *
 * 이 동작은 Device Key 경로가 사실상 Config* 응답 전용이던 시절에는 옳았다. 그런데
 * Remote Provisioning Client 모델도 `requiresDeviceKey` 라서, RPR 구현이 들어온 순간부터
 * **Scan Report / PDU Report / Link Report 도 같은 분기**로 들어온다. 그것들은 CDB 를 한 글자도
 * 바꾸지 않으므로(핸들러가 순수 pass-through) 저장은 의미상 틀렸고, 비용은 실제로 크다:
 * 10초 스캔 세션에서 30기기 발견 = 직렬화 30회, PB-Remote 로 기기 1대 프로비저닝 ≈ 10회.
 *
 * ## 이 테스트가 봉인하는 것
 *
 * 1. **Config 계열의 기존 emit 동작은 절대 불변** — 여기서 하나라도 false 가 되면 노드 설정이
 *    저장되지 않아 앱 재시작 시 CDB 가 되돌아간다 (유령 노드 부류의 최악 회귀).
 * 2. RPR **수신 8종 + 송신 계열**은 emit 하지 않는다.
 * 3. 판정이 **블랙리스트**(RPR 만 제외)라서, 앞으로 추가되는 미지의 Config 계열 메시지의
 *    기본값이 "저장한다" 쪽이다.
 */
@OptIn(ExperimentalUuidApi::class)
class AccessLayerNetworkStateEmitTest {

    private val uuid = byteArrayOf(
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10
    ).toUuid()

    @Test
    fun `Config 계열은 여전히 네트워크 저장을 유발한다`() {
        val configMessages: List<MeshMessage> = listOf(
            ConfigCompositionDataGet(page = 0u),
            ConfigBeaconGet(),
            ConfigBeaconStatus(isEnabled = true),
            ConfigDefaultTtlStatus(ttl = 7u),
            ConfigNodeReset(),
        )
        configMessages.forEach { message ->
            assertTrue(
                "${message::class.simpleName} 은 저장을 유발해야 한다 — false 가 되면 노드 설정이 " +
                        "영속화되지 않는다",
                message.mutatesNetworkState()
            )
        }
    }

    @Test
    fun `키 상태 메시지도 저장을 유발한다`() {
        // AppKey/NetKey Status 는 CDB 의 node.netKeys/appKeys 를 갱신한다.
        val appKeyStatus = ConfigAppKeyStatus.init(byteArrayOf(0x00, 0x00, 0x00, 0x00))
        val netKeyStatus = ConfigNetKeyStatus.init(byteArrayOf(0x00, 0x00, 0x00))
        listOfNotNull(appKeyStatus, netKeyStatus).forEach { message ->
            assertTrue(
                "${message::class.simpleName}",
                (message as MeshMessage).mutatesNetworkState()
            )
        }
    }

    @Test
    fun `DFU 계열도 저장 판정은 그대로다`() {
        // DFU 모델은 requiresDeviceKey 가 아니라 AppKey 경로라 이 분기에 오지 않지만,
        // 판정 함수가 RPR 만 좁게 제외한다는 사실을 명시적으로 못박는다.
        assertTrue(FirmwareUpdateInformationGet.all().mutatesNetworkState())
        assertTrue(
            FirmwareDistributionStatus.init(byteArrayOf(0x00, 0x00))
                ?.let { (it as MeshMessage).mutatesNetworkState() } ?: true
        )
    }

    @Test
    fun `Remote Provisioning 수신 8종은 저장을 유발하지 않는다`() {
        val rprReceived: List<MeshMessage> = listOf(
            RemoteProvisioningScanCapabilitiesStatus(
                maxScannedItems = 32u,
                activeScanSupported = true
            ),
            RemoteProvisioningScanStatus(
                status = RemoteProvisioningMessageStatus.SUCCESS,
                scanningState = RemoteProvisioningScanState.MULTIPLE_DEVICE_SCAN,
                scannedItemsLimit = 32u,
                timeout = 10.seconds
            ),
            RemoteProvisioningScanReport(rssi = -60, uuid = uuid, oobInformationRaw = 0u),
            RemoteProvisioningExtendedScanReport(
                status = RemoteProvisioningMessageStatus.SUCCESS,
                uuid = uuid
            ),
            RemoteProvisioningLinkStatus(
                status = RemoteProvisioningMessageStatus.SUCCESS,
                linkState = RemoteProvisioningLinkState.LINK_ACTIVE
            ),
            RemoteProvisioningLinkReport(
                status = RemoteProvisioningMessageStatus.SUCCESS,
                linkState = RemoteProvisioningLinkState.LINK_ACTIVE
            ),
            RemoteProvisioningPDUOutboundReport(outboundPduNumber = 1u),
            RemoteProvisioningPDUReport(
                inboundPduNumber = 1u,
                provisioningPdu = byteArrayOf(0x01, 0x00)
            ),
        )
        assertEquals8(rprReceived.size)
        rprReceived.forEach { message ->
            assertFalse(
                "${message::class.simpleName} 은 CDB 를 바꾸지 않으므로 저장을 유발하면 안 된다",
                message.mutatesNetworkState()
            )
        }
    }

    @Test
    fun `Remote Provisioning 송신 메시지도 저장을 유발하지 않는다`() {
        assertFalse(RemoteProvisioningLinkOpen(uuid = uuid).mutatesNetworkState())
    }

    private fun assertEquals8(actual: Int) =
        assertTrue("수신 8종을 모두 덮어야 한다 (실제 $actual)", actual == 8)
}
