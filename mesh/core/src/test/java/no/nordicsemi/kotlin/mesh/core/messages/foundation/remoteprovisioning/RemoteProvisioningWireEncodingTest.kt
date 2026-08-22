package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.data.toUuid
import no.nordicsemi.kotlin.mesh.core.layers.foundation.RemoteProvisioningClientHandler
import no.nordicsemi.kotlin.mesh.core.messages.AdStructure
import no.nordicsemi.kotlin.mesh.core.messages.AdType
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkCloseReason
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkState
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningScanState
import no.nordicsemi.kotlin.mesh.core.model.Model
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi

/**
 * Wire encoding regression guard for the Remote Provisioning (PB-Remote) messages
 * (simdo-fork, 2026-08-12).
 *
 * ## 배경
 *
 * 이 17종 메시지는 upstream Kotlin-Mesh-Library 에 **존재하지 않는다**
 * (`RemoteProvisioningClientHandler` 가 `messageTypes = mapOf()` 인 17줄 stub 이었다).
 * upstream 브랜치에도 RPR 작업본이 없어 정답지를 두 곳에서 교차 확보했다:
 *
 *  - **NCS v3.4.0** `zephyr/subsys/bluetooth/mesh/{rpr.h, rpr_srv.c, rpr_cli.c}` +
 *    `zephyr/include/zephyr/bluetooth/mesh/rpr.h` — **wire 포맷의 최종 권위**.
 *    우리 커미셔닝 동글(`neo_mesh_commissioner`)이 바로 이 RPR Server 를 돌린다.
 *  - iOS `IOS-nRF-Mesh-Library` 4.1.0 `Mesh Messages/Foundation/Remote Provisioning/` —
 *    구조·API 형태의 참고본. **wire 는 iOS 가 틀린 곳이 있다**(아래 §OOB).
 *
 * ## 이 테스트가 잡는 것
 *
 * NCS 의 opcode 길이표(`BT_MESH_LEN_EXACT(n)` / `BT_MESH_LEN_MIN(n)`)는 **수신측 하드 게이트**다.
 * 길이가 어긋나면 handler 진입 전에 드롭되고 **응답이 아예 없다** → 앱은 원인 불명 타임아웃만 본다.
 * RPR 은 Extended Scan Start 처럼 애초에 Status 메시지가 없는 절차가 있어 이 실패 모드가 특히 잦다.
 *
 * 또한 디코더가 **예외를 던지지 않는지**를 검증한다. `ModelEventHandler.decode()` 호출 경로에는
 * try/catch 가 없어(`AccessLayer`), 디코더에서 새는 예외 하나가 RX 코루틴을 죽인다.
 *
 * ## ★ OOB Information 엔디안 — iOS 4.1.0 과 상이 (의도된 divergence)
 *
 * Scan Report / Extended Scan Report 의 OOB Information 은 **little endian** 이다:
 * `rpr_srv.c scan_report_send()` = `net_buf_simple_add_le16`,
 * `rpr_cli.c handle_scan_report()` = `net_buf_simple_pull_le16`.
 * iOS 는 `.bigEndian` 인코딩 + `readBigEndian` 디코딩이라 Zephyr 서버와 **상호운용되지 않는다**.
 * (혼동 주의: Unprovisioned Device Beacon 의 같은 이름 필드는 진짜 big endian —
 *  `beacon.c` 가 `add_be16`/`pull_be16`.)
 */
@OptIn(ExperimentalUuidApi::class)
class RemoteProvisioningWireEncodingTest {

    private val uuidBytes = byteArrayOf(
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10
    )
    private val testUuid = uuidBytes.toUuid()

    // region opcode 대조 — NCS `zephyr/subsys/bluetooth/mesh/rpr.h` 원문 값

    @Test
    fun `Remote Provisioning opcode 17종이 NCS rpr_h 와 일치`() {
        assertEquals("RPR_OP_SCAN_CAPS_GET", 0x804Fu, RemoteProvisioningScanCapabilitiesGet.opCode)
        assertEquals(
            "RPR_OP_SCAN_CAPS_STATUS",
            0x8050u,
            RemoteProvisioningScanCapabilitiesStatus.opCode
        )
        assertEquals("RPR_OP_SCAN_GET", 0x8051u, RemoteProvisioningScanGet.opCode)
        assertEquals("RPR_OP_SCAN_START", 0x8052u, RemoteProvisioningScanStart.opCode)
        assertEquals("RPR_OP_SCAN_STOP", 0x8053u, RemoteProvisioningScanStop.opCode)
        assertEquals("RPR_OP_SCAN_STATUS", 0x8054u, RemoteProvisioningScanStatus.opCode)
        assertEquals("RPR_OP_SCAN_REPORT", 0x8055u, RemoteProvisioningScanReport.opCode)
        assertEquals(
            "RPR_OP_EXTENDED_SCAN_START",
            0x8056u,
            RemoteProvisioningExtendedScanStart.opCode
        )
        assertEquals(
            "RPR_OP_EXTENDED_SCAN_REPORT",
            0x8057u,
            RemoteProvisioningExtendedScanReport.opCode
        )
        assertEquals("RPR_OP_LINK_GET", 0x8058u, RemoteProvisioningLinkGet.opCode)
        assertEquals("RPR_OP_LINK_OPEN", 0x8059u, RemoteProvisioningLinkOpen.opCode)
        assertEquals("RPR_OP_LINK_CLOSE", 0x805Au, RemoteProvisioningLinkClose.opCode)
        assertEquals("RPR_OP_LINK_STATUS", 0x805Bu, RemoteProvisioningLinkStatus.opCode)
        assertEquals("RPR_OP_LINK_REPORT", 0x805Cu, RemoteProvisioningLinkReport.opCode)
        assertEquals("RPR_OP_PDU_SEND", 0x805Du, RemoteProvisioningPDUSend.opCode)
        assertEquals(
            "RPR_OP_PDU_OUTBOUND_REPORT",
            0x805Eu,
            RemoteProvisioningPDUOutboundReport.opCode
        )
        assertEquals("RPR_OP_PDU_REPORT", 0x805Fu, RemoteProvisioningPDUReport.opCode)
    }

    @Test
    fun `opcode 는 0x804F부터 0x805F까지 연속이며 중복이 없다`() {
        val opCodes = listOf(
            RemoteProvisioningScanCapabilitiesGet.opCode,
            RemoteProvisioningScanCapabilitiesStatus.opCode,
            RemoteProvisioningScanGet.opCode,
            RemoteProvisioningScanStart.opCode,
            RemoteProvisioningScanStop.opCode,
            RemoteProvisioningScanStatus.opCode,
            RemoteProvisioningScanReport.opCode,
            RemoteProvisioningExtendedScanStart.opCode,
            RemoteProvisioningExtendedScanReport.opCode,
            RemoteProvisioningLinkGet.opCode,
            RemoteProvisioningLinkOpen.opCode,
            RemoteProvisioningLinkClose.opCode,
            RemoteProvisioningLinkStatus.opCode,
            RemoteProvisioningLinkReport.opCode,
            RemoteProvisioningPDUSend.opCode,
            RemoteProvisioningPDUOutboundReport.opCode,
            RemoteProvisioningPDUReport.opCode,
        )
        assertEquals("중복 opcode", 17, opCodes.toSet().size)
        assertEquals((0x804Fu..0x805Fu).toSet(), opCodes.toSet())
    }

    @Test
    fun `enum raw value 가 NCS rpr_h 와 일치`() {
        // enum bt_mesh_rpr_status
        assertEquals(0x00u.toUByte(), RemoteProvisioningMessageStatus.SUCCESS.value)
        assertEquals(0x0Bu.toUByte(), RemoteProvisioningMessageStatus.LINK_CLOSED_AS_CANNOT_DELIVER_PDU_REPORT.value)
        assertEquals(12, RemoteProvisioningMessageStatus.entries.size)
        // enum bt_mesh_rpr_scan
        assertEquals(0x00u.toUByte(), RemoteProvisioningScanState.IDLE.value)
        assertEquals(0x01u.toUByte(), RemoteProvisioningScanState.MULTIPLE_DEVICE_SCAN.value)
        assertEquals(0x02u.toUByte(), RemoteProvisioningScanState.SINGLE_DEVICE_SCAN.value)
        // enum bt_mesh_rpr_link_state (0x03 is BT_MESH_RPR_LINK_SENDING in NCS)
        assertEquals(0x00u.toUByte(), RemoteProvisioningLinkState.IDLE.value)
        assertEquals(0x01u.toUByte(), RemoteProvisioningLinkState.LINK_OPENING.value)
        assertEquals(0x02u.toUByte(), RemoteProvisioningLinkState.LINK_ACTIVE.value)
        assertEquals(0x03u.toUByte(), RemoteProvisioningLinkState.OUTBOUND_PACKET_TRANSFER.value)
        assertEquals(0x04u.toUByte(), RemoteProvisioningLinkState.LINK_CLOSING.value)
        // enum bt_mesh_rpr_node_refresh
        assertEquals(
            0x00u.toUByte(),
            NodeProvisioningProtocolInterfaceProcedure.DEVICE_KEY_REFRESH.value
        )
        assertEquals(
            0x01u.toUByte(),
            NodeProvisioningProtocolInterfaceProcedure.NODE_ADDRESS_REFRESH.value
        )
        assertEquals(
            0x02u.toUByte(),
            NodeProvisioningProtocolInterfaceProcedure.NODE_COMPOSITION_REFRESH.value
        )
        // prov_bearer_link_status: 0x01 is prohibited
        assertEquals(0x00u.toUByte(), RemoteProvisioningLinkCloseReason.SUCCESS.value)
        assertEquals(0x02u.toUByte(), RemoteProvisioningLinkCloseReason.FAIL.value)
    }

    @Test
    fun `Remote Provisioning Client Model ID 는 0x0005 이고 Device Key 를 요구한다`() {
        assertEquals(0x0005u.toUShort(), Model.REMOTE_PROVISIONING_CLIENT_MODEL_ID)
        assertEquals(0x0004u.toUShort(), Model.REMOTE_PROVISIONING_SERVER_MODEL_ID)
    }

    // endregion

    // region handler 등록 — 수신 8종

    @Test
    fun `RemoteProvisioningClientHandler 가 수신 8종을 등록한다`() {
        val handler = RemoteProvisioningClientHandler()
        assertEquals(8, handler.messageTypes.size)
        assertEquals(
            setOf(
                RemoteProvisioningScanCapabilitiesStatus.opCode,
                RemoteProvisioningScanStatus.opCode,
                RemoteProvisioningScanReport.opCode,
                RemoteProvisioningExtendedScanReport.opCode,
                RemoteProvisioningLinkStatus.opCode,
                RemoteProvisioningLinkReport.opCode,
                RemoteProvisioningPDUOutboundReport.opCode,
                RemoteProvisioningPDUReport.opCode,
            ),
            handler.messageTypes.keys
        )
        // 등록된 initializer 가 자기 opcode 의 메시지를 실제로 만들어내는지 (매핑 오배선 가드)
        val decoded = handler.messageTypes[RemoteProvisioningScanReport.opCode]
            ?.init(byteArrayOf(0xBA.toByte()) + uuidBytes + byteArrayOf(0x00, 0x00))
        assertTrue(decoded is RemoteProvisioningScanReport)
    }

    // endregion

    // region Scan Capabilities

    @Test
    fun `ScanCapabilitiesGet 는 파라미터가 없다`() {
        assertNull(RemoteProvisioningScanCapabilitiesGet().parameters)
        assertNotNull(RemoteProvisioningScanCapabilitiesGet.init(byteArrayOf()))
        assertNull(
            "BT_MESH_LEN_EXACT(0) — 파라미터가 붙으면 서버가 드롭",
            RemoteProvisioningScanCapabilitiesGet.init(byteArrayOf(0x00))
        )
        assertEquals(
            RemoteProvisioningScanCapabilitiesStatus.opCode,
            RemoteProvisioningScanCapabilitiesGet().responseOpCode
        )
    }

    @Test
    fun `ScanCapabilitiesStatus 는 2옥텟 - 동글 기본값 32 active`() {
        val message = RemoteProvisioningScanCapabilitiesStatus(
            maxScannedItems = 32u,
            activeScanSupported = true
        )
        assertArrayEquals(byteArrayOf(0x20, 0x01), message.parameters)

        val decoded = RemoteProvisioningScanCapabilitiesStatus.init(byteArrayOf(0x20, 0x01))!!
        assertEquals(32u.toUByte(), decoded.maxScannedItems)
        assertTrue(decoded.activeScanSupported)

        assertNull(RemoteProvisioningScanCapabilitiesStatus.init(byteArrayOf(0x20)))
        assertNull(RemoteProvisioningScanCapabilitiesStatus.init(byteArrayOf(0x20, 0x01, 0x00)))
    }

    // endregion

    // region Scan Start / Stop / Get / Status

    @Test
    fun `ScanStart 는 uuid 없이 2옥텟`() {
        val message = RemoteProvisioningScanStart(scannedItemsLimit = 0u, timeout = 10.seconds)
        assertArrayEquals(byteArrayOf(0x00, 0x0A), message.parameters)
        assertEquals(RemoteProvisioningScanStatus.opCode, message.responseOpCode)
    }

    @Test
    fun `ScanStart 는 uuid 와 함께 18옥텟이고 UUID 는 big endian 순서 그대로`() {
        val message = RemoteProvisioningScanStart(
            scannedItemsLimit = 5u,
            timeout = 30.seconds,
            uuid = testUuid
        )
        assertArrayEquals(byteArrayOf(0x05, 0x1E) + uuidBytes, message.parameters)
        assertEquals(18, message.parameters.size)
    }

    @Test
    fun `ScanStart 라운드트립`() {
        val encoded = RemoteProvisioningScanStart(
            scannedItemsLimit = 32u,
            timeout = 255.seconds,
            uuid = testUuid
        ).parameters
        val decoded = RemoteProvisioningScanStart.init(encoded)!!
        assertEquals(32u.toUByte(), decoded.scannedItemsLimit)
        assertEquals(255L, decoded.timeout.inWholeSeconds)
        assertEquals(testUuid, decoded.uuid)
        assertArrayEquals(encoded, decoded.parameters)
    }

    @Test
    fun `ScanStart timeout 0 은 서버가 EINVAL 로 드롭하므로 생성도 디코드도 거부`() {
        // 서버: `if (!timeout) return -EINVAL;` — 응답이 아예 없다.
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningScanStart(timeout = 0.seconds)
        }
        assertNull(RemoteProvisioningScanStart.init(byteArrayOf(0x00, 0x00)))
    }

    @Test
    fun `ScanStart timeout 은 1octet 이라 256초 이상은 거부`() {
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningScanStart(timeout = 256.seconds)
        }
    }

    @Test
    fun `ScanStart 는 uuid 가 정확히 16옥텟이 아니면 디코드 거부`() {
        // 서버: `if (buf->len == 16) … else if (buf->len) return -EINVAL;`
        assertNull(RemoteProvisioningScanStart.init(byteArrayOf(0x00, 0x0A, 0x01)))
        assertNull(RemoteProvisioningScanStart.init(byteArrayOf(0x00, 0x0A) + ByteArray(15)))
        assertNull(RemoteProvisioningScanStart.init(byteArrayOf(0x00, 0x0A) + ByteArray(17)))
    }

    @Test
    fun `ScanGet 과 ScanStop 은 파라미터가 없고 ScanStatus 를 응답으로 받는다`() {
        assertNull(RemoteProvisioningScanGet().parameters)
        assertNull(RemoteProvisioningScanStop().parameters)
        assertEquals(RemoteProvisioningScanStatus.opCode, RemoteProvisioningScanGet().responseOpCode)
        assertEquals(
            RemoteProvisioningScanStatus.opCode,
            RemoteProvisioningScanStop().responseOpCode
        )
        assertNotNull(RemoteProvisioningScanGet.init(byteArrayOf()))
        assertNull(RemoteProvisioningScanGet.init(byteArrayOf(0x00)))
        assertNotNull(RemoteProvisioningScanStop.init(byteArrayOf()))
        assertNull(RemoteProvisioningScanStop.init(byteArrayOf(0x00)))
    }

    @Test
    fun `ScanStatus 는 4옥텟 exact 라운드트립`() {
        val message = RemoteProvisioningScanStatus(
            status = RemoteProvisioningMessageStatus.SUCCESS,
            scanningState = RemoteProvisioningScanState.MULTIPLE_DEVICE_SCAN,
            scannedItemsLimit = 32u,
            timeout = 10.seconds
        )
        assertArrayEquals(byteArrayOf(0x00, 0x01, 0x20, 0x0A), message.parameters)

        val decoded = RemoteProvisioningScanStatus.init(message.parameters)!!
        assertEquals(RemoteProvisioningMessageStatus.SUCCESS, decoded.status)
        assertEquals(RemoteProvisioningScanState.MULTIPLE_DEVICE_SCAN, decoded.scanningState)
        assertEquals(32u.toUByte(), decoded.scannedItemsLimit)
        assertEquals(10L, decoded.timeout.inWholeSeconds)
        assertTrue(decoded.isSuccess)

        assertNull(RemoteProvisioningScanStatus.init(byteArrayOf(0x00, 0x01, 0x20)))
        assertNull(RemoteProvisioningScanStatus.init(byteArrayOf(0x00, 0x01, 0x20, 0x0A, 0x00)))
    }

    @Test
    fun `ScanStatus 는 미정의 status 나 scan state 를 예외 대신 null 로 처리`() {
        // 디코더가 throw 하면 AccessLayer 의 RX 코루틴이 죽는다 (try-catch 없음).
        assertNull(RemoteProvisioningScanStatus.init(byteArrayOf(0x7F, 0x01, 0x20, 0x0A)))
        assertNull(RemoteProvisioningScanStatus.init(byteArrayOf(0x00, 0x7F, 0x20, 0x0A)))
    }

    @Test
    fun `ScanStatus 실패 상태의 isSuccess 는 false`() {
        val decoded = RemoteProvisioningScanStatus.init(byteArrayOf(0x01, 0x00, 0x00, 0x00))!!
        assertEquals(RemoteProvisioningMessageStatus.SCANNING_CANNOT_START, decoded.status)
        assertTrue(!decoded.isSuccess)
    }

    // endregion

    // region Scan Report — RSSI int8 + OOB little endian

    @Test
    fun `ScanReport 19옥텟 - RSSI 는 int8 로 부호가 보존된다`() {
        val message = RemoteProvisioningScanReport(
            rssi = -70,
            uuid = testUuid,
            oobInformationRaw = 0x0000u
        )
        assertEquals(19, message.parameters.size)
        assertEquals(0xBA.toByte(), message.parameters[0])

        val decoded = RemoteProvisioningScanReport.init(message.parameters)!!
        assertEquals((-70).toByte(), decoded.rssi)
        assertEquals(-70, decoded.rssiDbm)
        assertEquals(testUuid, decoded.uuid)
        assertNull(decoded.uriHash)
    }

    @Test
    fun `ScanReport RSSI 경계 -128 과 127`() {
        val min = RemoteProvisioningScanReport.init(
            byteArrayOf(0x80.toByte()) + uuidBytes + byteArrayOf(0x00, 0x00)
        )!!
        assertEquals(-128, min.rssiDbm)
        val max = RemoteProvisioningScanReport.init(
            byteArrayOf(0x7F) + uuidBytes + byteArrayOf(0x00, 0x00)
        )!!
        assertEquals(127, max.rssiDbm)
    }

    /**
     * ★ 핵심 negative control. 이 assert 를 BIG_ENDIAN 으로 뒤집으면 (= iOS 4.1.0 동작)
     * 정확히 이 테스트만 FAIL 한다.
     */
    @Test
    fun `ScanReport 의 OOB Information 은 little endian - iOS 는 big endian 이라 상호운용 실패`() {
        val message = RemoteProvisioningScanReport(
            rssi = -40,
            uuid = testUuid,
            oobInformationRaw = 0x8004u // On Device | QR Code
        )
        // rpr_srv.c: net_buf_simple_add_le16(&buf, dev->oob)
        assertEquals("OOB LSB", 0x04.toByte(), message.parameters[17])
        assertEquals("OOB MSB", 0x80.toByte(), message.parameters[18])

        val decoded = RemoteProvisioningScanReport.init(message.parameters)!!
        assertEquals(0x8004u.toUShort(), decoded.oobInformationRaw)
    }

    @Test
    fun `ScanReport 는 복합 OOB 비트필드에서 예외를 던지지 않고 oobInformation 만 null 이 된다`() {
        // OobInformation.from() 은 단일 플래그가 아니면 IllegalArgumentException 을 던진다.
        // 그 예외가 디코더 밖으로 새면 RX 코루틴이 죽으므로 raw 값만 보존하고 typed 는 null.
        val decoded = RemoteProvisioningScanReport.init(
            byteArrayOf(0xD8.toByte()) + uuidBytes + byteArrayOf(0x04, 0x80.toByte())
        )!!
        assertEquals(0x8004u.toUShort(), decoded.oobInformationRaw)
        assertNull(decoded.oobInformation)

        // 단일 플래그면 typed 값이 나온다 (QR Code = 1 shl 2).
        val single = RemoteProvisioningScanReport.init(
            byteArrayOf(0xD8.toByte()) + uuidBytes + byteArrayOf(0x04, 0x00)
        )!!
        assertNotNull(single.oobInformation)
    }

    @Test
    fun `ScanReport 23옥텟이면 URI Hash 4옥텟이 붙는다`() {
        val hash = byteArrayOf(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte())
        val message = RemoteProvisioningScanReport(
            rssi = -55,
            uuid = testUuid,
            oobInformationRaw = 0x0002u,
            uriHash = hash
        )
        assertEquals(23, message.parameters.size)
        val decoded = RemoteProvisioningScanReport.init(message.parameters)!!
        assertArrayEquals(hash, decoded.uriHash)
        assertArrayEquals(message.parameters, decoded.parameters)
    }

    @Test
    fun `ScanReport 는 19나 23 이외의 길이를 거부`() {
        // rpr_cli.c: 남은 길이가 4가 아니면서 0도 아니면 -EINVAL
        listOf(0, 18, 20, 21, 22, 24).forEach { size ->
            assertNull("size=$size", RemoteProvisioningScanReport.init(ByteArray(size)))
        }
    }

    @Test
    fun `ScanReport 는 세그먼트 전송을 강제한다`() {
        // MshPRT 1.1 4.4.5.5.1.7 / rpr_srv.c LINK_CTX(.., send_rel = true)
        assertTrue(
            RemoteProvisioningScanReport(
                rssi = 0,
                uuid = testUuid,
                oobInformationRaw = 0u
            ).isSegmented
        )
    }

    // endregion

    // region Extended Scan Start

    @Test
    fun `ExtendedScanStart self 모드는 필터만 보낸다`() {
        val message = RemoteProvisioningExtendedScanStart(
            adTypeFilter = listOf(AdType.URI)
        )
        assertArrayEquals(byteArrayOf(0x01, 0x24), message.parameters)

        val decoded = RemoteProvisioningExtendedScanStart.init(message.parameters)!!
        assertNull(decoded.uuid)
        assertNull(decoded.timeout)
        assertEquals(listOf(AdType.URI), decoded.adTypeFilter)
    }

    @Test
    fun `ExtendedScanStart 는 uuid 와 timeout 을 함께 실어 19옥텟`() {
        val message = RemoteProvisioningExtendedScanStart(
            adTypeFilter = listOf(AdType.COMPLETE_LOCAL_NAME),
            uuid = testUuid,
            timeout = 5.seconds
        )
        assertArrayEquals(byteArrayOf(0x01, 0x09) + uuidBytes + byteArrayOf(0x05), message.parameters)
        assertEquals(19, message.parameters.size)

        val decoded = RemoteProvisioningExtendedScanStart.init(message.parameters)!!
        assertEquals(testUuid, decoded.uuid)
        assertEquals(5L, decoded.timeout!!.inWholeSeconds)
        assertArrayEquals(message.parameters, decoded.parameters)
    }

    @Test
    fun `ExtendedScanStart 는 우리 동글이 다루는 AD Type 4종을 순서대로 실어보낸다`() {
        // CONFIG_BT_MESH_RPR_AD_TYPES_MAX = 4. 서버는 초과분을 조용히 잘라내므로 우선순위 순서.
        val filter = listOf(
            AdType.COMPLETE_LOCAL_NAME,
            AdType.URI,
            AdType.TX_POWER_LEVEL,
            AdType.MANUFACTURER_SPECIFIC_DATA
        )
        val message = RemoteProvisioningExtendedScanStart(
            adTypeFilter = filter,
            uuid = testUuid,
            timeout = 21.seconds
        )
        assertArrayEquals(
            byteArrayOf(0x04, 0x09, 0x24, 0x0A, 0xFF.toByte()) + uuidBytes + byteArrayOf(0x15),
            message.parameters
        )
        assertEquals(filter, RemoteProvisioningExtendedScanStart.init(message.parameters)!!.adTypeFilter)
    }

    @Test
    fun `ExtendedScanStart 는 금지 AD Type 을 거부한다`() {
        // 서버는 이 경우 -EINVAL → 리포트가 아예 오지 않는다. 앱은 원인 불명 타임아웃만 본다.
        AdType.prohibitedInFilter.forEach { prohibited ->
            assertThrows(IllegalArgumentException::class.java) {
                RemoteProvisioningExtendedScanStart(adTypeFilter = listOf(prohibited))
            }
        }
        assertNull(RemoteProvisioningExtendedScanStart.init(byteArrayOf(0x01, 0x08)))
    }

    @Test
    fun `ExtendedScanStart 는 중복과 빈 필터와 17개 이상을 거부한다`() {
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningExtendedScanStart(
                adTypeFilter = listOf(AdType.URI, AdType.URI)
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningExtendedScanStart(adTypeFilter = emptyList())
        }
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningExtendedScanStart(
                adTypeFilter = (0x30..0x41).map { it.toUByte() } // 18개
            )
        }
        assertNull(
            "중복 필터",
            RemoteProvisioningExtendedScanStart.init(byteArrayOf(0x02, 0x24, 0x24))
        )
        assertNull("count=0", RemoteProvisioningExtendedScanStart.init(byteArrayOf(0x00, 0x24)))
    }

    @Test
    fun `ExtendedScanStart timeout 은 1부터 21초 사이여야 한다`() {
        // BT_MESH_RPR_EXT_SCAN_TIME_MIN / _MAX
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningExtendedScanStart(
                adTypeFilter = listOf(AdType.URI), uuid = testUuid, timeout = 22.seconds
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningExtendedScanStart(
                adTypeFilter = listOf(AdType.URI), uuid = testUuid, timeout = 0.seconds
            )
        }
        assertNull(
            RemoteProvisioningExtendedScanStart.init(
                byteArrayOf(0x01, 0x24) + uuidBytes + byteArrayOf(0x16)
            )
        )
    }

    @Test
    fun `ExtendedScanStart 는 uuid 와 timeout 을 짝으로만 허용`() {
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningExtendedScanStart(
                adTypeFilter = listOf(AdType.URI), uuid = testUuid, timeout = null
            )
        }
        // 잘린 tail (uuid 는 있는데 timeout 이 없음) → 서버가 -EINVAL
        assertNull(
            RemoteProvisioningExtendedScanStart.init(byteArrayOf(0x01, 0x24) + uuidBytes)
        )
    }

    // endregion

    // region Extended Scan Report

    @Test
    fun `ExtendedScanReport 17옥텟은 기기를 못 들은 경우 - oob 도 AD 도 없다`() {
        val encoded = byteArrayOf(0x00) + uuidBytes
        val decoded = RemoteProvisioningExtendedScanReport.init(encoded)!!
        assertEquals(RemoteProvisioningMessageStatus.SUCCESS, decoded.status)
        assertEquals(testUuid, decoded.uuid)
        assertNull(decoded.oobInformationRaw)
        assertTrue(decoded.adStructures.isEmpty())
        assertNull(decoded.localName)
        assertArrayEquals(encoded, decoded.parameters)
    }

    @Test
    fun `ExtendedScanReport 19옥텟은 기기는 들었으나 AD 가 없는 경우 - OOB 는 little endian`() {
        val encoded = byteArrayOf(0x00) + uuidBytes + byteArrayOf(0x04, 0x80.toByte())
        val decoded = RemoteProvisioningExtendedScanReport.init(encoded)!!
        assertEquals(0x8004u.toUShort(), decoded.oobInformationRaw)
        assertTrue(decoded.adStructures.isEmpty())
        assertArrayEquals(encoded, decoded.parameters)
    }

    @Test
    fun `ExtendedScanReport 의 Complete Local Name 이 기기 식별의 핵심`() {
        val name = "NEO-1"
        val ad = byteArrayOf((name.length + 1).toByte(), AdType.COMPLETE_LOCAL_NAME.toByte()) +
                name.toByteArray(Charsets.UTF_8)
        val encoded = byteArrayOf(0x00) + uuidBytes + byteArrayOf(0x00, 0x00) + ad

        val decoded = RemoteProvisioningExtendedScanReport.init(encoded)!!
        assertEquals(1, decoded.adStructures.size)
        assertEquals(name, decoded.localName)
        assertArrayEquals(encoded, decoded.parameters)
    }

    @Test
    fun `ExtendedScanReport 는 여러 AD Structure 를 순서대로 파싱`() {
        val nameAd = byteArrayOf(0x06, 0x09, 0x4E, 0x45, 0x4F, 0x2D, 0x31) // "NEO-1"
        val txPowerAd = byteArrayOf(0x02, 0x0A, 0xF4.toByte())             // -12 dBm
        val uriAd = byteArrayOf(0x05, 0x24, 0x2F, 0x2F, 0x61, 0x62)        // "//ab"
        val encoded = byteArrayOf(0x00) + uuidBytes + byteArrayOf(0x00, 0x00) +
                nameAd + txPowerAd + uriAd

        val decoded = RemoteProvisioningExtendedScanReport.init(encoded)!!
        assertEquals(3, decoded.adStructures.size)
        assertEquals("NEO-1", decoded.localName)
        assertEquals(-12, decoded.txPowerLevel)
        assertEquals("//ab", decoded.uri)
        assertArrayEquals(encoded, decoded.parameters)
    }

    @Test
    fun `ExtendedScanReport 는 잘린 AD tail 때문에 리포트 전체를 버리지 않는다`() {
        // 첫 구조는 온전하고 두 번째가 길이만큼 데이터가 없는 경우.
        val goodAd = byteArrayOf(0x06, 0x09, 0x4E, 0x45, 0x4F, 0x2D, 0x31)
        val truncated = byteArrayOf(0x09, 0x24, 0x2F)
        val encoded = byteArrayOf(0x00) + uuidBytes + byteArrayOf(0x00, 0x00) + goodAd + truncated

        val decoded = RemoteProvisioningExtendedScanReport.init(encoded)!!
        assertEquals(1, decoded.adStructures.size)
        assertEquals("NEO-1", decoded.localName)
        assertNull(decoded.uri)
    }

    @Test
    fun `ExtendedScanReport 는 17옥텟 미만과 미정의 status 를 거부`() {
        assertNull(RemoteProvisioningExtendedScanReport.init(ByteArray(16)))
        assertNull(RemoteProvisioningExtendedScanReport.init(byteArrayOf(0x7F) + uuidBytes))
    }

    @Test
    fun `ExtendedScanReport 실패 응답은 status 를 그대로 노출`() {
        // 서버 rsp 경로: status + uuid 만 (Link 가 idle 이 아닐 때 등)
        val decoded = RemoteProvisioningExtendedScanReport.init(byteArrayOf(0x03) + uuidBytes)!!
        assertEquals(RemoteProvisioningMessageStatus.LIMITED_RESOURCES, decoded.status)
        assertTrue(!decoded.isSuccess)
    }

    @Test
    fun `AdStructure parse 는 길이 0 을 종결자로 취급`() {
        val data = byteArrayOf(0x02, 0x0A, 0x10, 0x00, 0x06, 0x09, 0x41, 0x42, 0x43, 0x44)
        val parsed = AdStructure.parse(data = data, offset = 0)
        assertEquals(1, parsed.size)
        assertEquals(AdType.TX_POWER_LEVEL, parsed[0].type)
    }

    // endregion

    // region Link

    @Test
    fun `LinkGet 은 파라미터가 없고 LinkStatus 를 응답으로 받는다`() {
        assertNull(RemoteProvisioningLinkGet().parameters)
        assertEquals(RemoteProvisioningLinkStatus.opCode, RemoteProvisioningLinkGet().responseOpCode)
        assertNotNull(RemoteProvisioningLinkGet.init(byteArrayOf()))
        assertNull(RemoteProvisioningLinkGet.init(byteArrayOf(0x00)))
    }

    @Test
    fun `LinkOpen 은 uuid 만이면 16옥텟`() {
        val message = RemoteProvisioningLinkOpen(uuid = testUuid)
        assertArrayEquals(uuidBytes, message.parameters)
        assertEquals(RemoteProvisioningLinkStatus.opCode, message.responseOpCode)

        val decoded = RemoteProvisioningLinkOpen.init(uuidBytes)!!
        assertEquals(testUuid, decoded.uuid)
        assertNull(decoded.timeout)
        assertNull(decoded.nppiProcedure)
    }

    @Test
    fun `LinkOpen 은 timeout 과 함께 17옥텟이며 1부터 60초 범위`() {
        val message = RemoteProvisioningLinkOpen(uuid = testUuid, timeout = 60.seconds)
        assertArrayEquals(uuidBytes + byteArrayOf(0x3C), message.parameters)

        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningLinkOpen(uuid = testUuid, timeout = 61.seconds)
        }
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningLinkOpen(uuid = testUuid, timeout = 0.seconds)
        }
        // 서버: `if (!timeout || timeout > 0x3c) return -EINVAL;`
        assertNull(RemoteProvisioningLinkOpen.init(uuidBytes + byteArrayOf(0x3D)))
        assertNull(RemoteProvisioningLinkOpen.init(uuidBytes + byteArrayOf(0x00)))
    }

    @Test
    fun `LinkOpen NPPI 모드는 1옥텟`() {
        val message = RemoteProvisioningLinkOpen(
            nppiProcedure = NodeProvisioningProtocolInterfaceProcedure.NODE_COMPOSITION_REFRESH
        )
        assertArrayEquals(byteArrayOf(0x02), message.parameters)

        val decoded = RemoteProvisioningLinkOpen.init(byteArrayOf(0x01))!!
        assertEquals(
            NodeProvisioningProtocolInterfaceProcedure.NODE_ADDRESS_REFRESH,
            decoded.nppiProcedure
        )
        assertNull(decoded.uuid)
        // 서버: `if (refresh > BT_MESH_RPR_NODE_REFRESH_COMPOSITION) return -EINVAL;`
        assertNull(RemoteProvisioningLinkOpen.init(byteArrayOf(0x03)))
    }

    @Test
    fun `LinkOpen 은 1 16 17 이외의 길이를 거부`() {
        listOf(0, 2, 15, 18).forEach { size ->
            assertNull("size=$size", RemoteProvisioningLinkOpen.init(ByteArray(size)))
        }
    }

    @Test
    fun `LinkClose 는 1옥텟이고 금지 reason 은 FAIL 로 강제된다`() {
        assertArrayEquals(
            byteArrayOf(0x00),
            RemoteProvisioningLinkClose(RemoteProvisioningLinkCloseReason.SUCCESS).parameters
        )
        assertArrayEquals(
            byteArrayOf(0x02),
            RemoteProvisioningLinkClose(RemoteProvisioningLinkCloseReason.FAIL).parameters
        )
        // 서버는 0x00 / 0x02 이외를 -EINVAL 로 드롭한다 → 링크가 영원히 안 닫힌다.
        assertArrayEquals(
            byteArrayOf(0x02),
            RemoteProvisioningLinkClose(RemoteProvisioningLinkCloseReason.UNRECOGNIZED).parameters
        )
        assertEquals(
            RemoteProvisioningLinkStatus.opCode,
            RemoteProvisioningLinkClose(RemoteProvisioningLinkCloseReason.SUCCESS).responseOpCode
        )
        assertNull(RemoteProvisioningLinkClose.init(byteArrayOf()))
        assertNull(RemoteProvisioningLinkClose.init(byteArrayOf(0x00, 0x00)))
    }

    @Test
    fun `LinkStatus 는 2옥텟 exact 라운드트립`() {
        val message = RemoteProvisioningLinkStatus(
            status = RemoteProvisioningMessageStatus.SUCCESS,
            linkState = RemoteProvisioningLinkState.LINK_OPENING
        )
        assertArrayEquals(byteArrayOf(0x00, 0x01), message.parameters)

        val decoded = RemoteProvisioningLinkStatus.init(message.parameters)!!
        assertEquals(RemoteProvisioningLinkState.LINK_OPENING, decoded.linkState)
        assertTrue(decoded.isSuccess)

        assertNull(RemoteProvisioningLinkStatus.init(byteArrayOf(0x00)))
        assertNull(RemoteProvisioningLinkStatus.init(byteArrayOf(0x00, 0x01, 0x00)))
        assertNull(RemoteProvisioningLinkStatus.init(byteArrayOf(0x00, 0x05)))
    }

    @Test
    fun `LinkReport 는 reason 없이 2옥텟 있으면 3옥텟`() {
        val withoutReason = RemoteProvisioningLinkReport(
            status = RemoteProvisioningMessageStatus.SUCCESS,
            linkState = RemoteProvisioningLinkState.LINK_ACTIVE
        )
        assertArrayEquals(byteArrayOf(0x00, 0x02), withoutReason.parameters)
        assertTrue(withoutReason.isSegmented)

        val withReason = RemoteProvisioningLinkReport(
            status = RemoteProvisioningMessageStatus.LINK_CLOSED_BY_DEVICE,
            linkState = RemoteProvisioningLinkState.IDLE,
            reason = RemoteProvisioningLinkCloseReason.FAIL
        )
        assertArrayEquals(byteArrayOf(0x06, 0x00, 0x02), withReason.parameters)

        val decoded = RemoteProvisioningLinkReport.init(withReason.parameters)!!
        assertEquals(RemoteProvisioningMessageStatus.LINK_CLOSED_BY_DEVICE, decoded.status)
        assertEquals(RemoteProvisioningLinkState.IDLE, decoded.linkState)
        assertEquals(RemoteProvisioningLinkCloseReason.FAIL, decoded.reason)
    }

    @Test
    fun `LinkReport 는 미지의 reason 때문에 리포트를 버리지 않는다`() {
        // linkState 가 이 메시지의 핵심 정보라 reason 하나 때문에 드롭하면 bearer 가 멈춘다.
        val decoded = RemoteProvisioningLinkReport.init(byteArrayOf(0x07, 0x00, 0x55))!!
        assertEquals(RemoteProvisioningLinkState.IDLE, decoded.linkState)
        assertEquals(RemoteProvisioningLinkCloseReason.UNRECOGNIZED, decoded.reason)
    }

    @Test
    fun `LinkReport 는 2나 3 이외의 길이와 미정의 상태를 거부`() {
        assertNull(RemoteProvisioningLinkReport.init(byteArrayOf(0x00)))
        assertNull(RemoteProvisioningLinkReport.init(byteArrayOf(0x00, 0x02, 0x00, 0x00)))
        assertNull(RemoteProvisioningLinkReport.init(byteArrayOf(0x7F, 0x02)))
        assertNull(RemoteProvisioningLinkReport.init(byteArrayOf(0x00, 0x7F)))
    }

    // endregion

    // region PDU transfer

    @Test
    fun `PDUSend 는 번호 1옥텟에 raw provisioning PDU 를 붙인다`() {
        // Provisioning Invite (type 0x00, attention timer 5)
        val invite = byteArrayOf(0x00, 0x05)
        val message = RemoteProvisioningPDUSend(outboundPduNumber = 1u, provisioningPdu = invite)
        assertArrayEquals(byteArrayOf(0x01, 0x00, 0x05), message.parameters)
        assertTrue("rpr_cli.c send() 는 send_rel = true", message.isSegmented)

        val decoded = RemoteProvisioningPDUSend.init(message.parameters)!!
        assertEquals(1u.toUByte(), decoded.outboundPduNumber)
        assertArrayEquals(invite, decoded.provisioningPdu)
    }

    @Test
    fun `PDUSend 는 Provisioning Data PDU 도 그대로 통과시킨다`() {
        // ProvisioningRequest.from() 은 DATA(0x07) 분기가 없어 InvalidPdu 를 던진다.
        // iOS 처럼 파싱 타입으로 모델링했다면 프로비저닝 마지막 단계에서 깨졌을 자리.
        val data = byteArrayOf(0x07) + ByteArray(33) { it.toByte() }
        val message = RemoteProvisioningPDUSend(outboundPduNumber = 5u, provisioningPdu = data)
        assertEquals(35, message.parameters.size)
        assertArrayEquals(data, RemoteProvisioningPDUSend.init(message.parameters)!!.provisioningPdu)
    }

    @Test
    fun `PDUSend 는 빈 PDU 를 거부`() {
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningPDUSend(outboundPduNumber = 1u, provisioningPdu = byteArrayOf())
        }
        assertNull(RemoteProvisioningPDUSend.init(byteArrayOf(0x01)))
        assertNull(RemoteProvisioningPDUSend.init(byteArrayOf()))
    }

    @Test
    fun `PDUOutboundReport 는 1옥텟 exact`() {
        val message = RemoteProvisioningPDUOutboundReport(outboundPduNumber = 3u)
        assertArrayEquals(byteArrayOf(0x03), message.parameters)
        assertEquals(
            3u.toUByte(),
            RemoteProvisioningPDUOutboundReport.init(byteArrayOf(0x03))!!.outboundPduNumber
        )
        assertNull(RemoteProvisioningPDUOutboundReport.init(byteArrayOf()))
        assertNull(RemoteProvisioningPDUOutboundReport.init(byteArrayOf(0x03, 0x00)))
    }

    @Test
    fun `PDUOutboundReport 번호는 255까지 부호확장 없이 읽힌다`() {
        val decoded = RemoteProvisioningPDUOutboundReport.init(byteArrayOf(0xFF.toByte()))!!
        assertEquals(255u.toUByte(), decoded.outboundPduNumber)
    }

    @Test
    fun `PDUReport 는 번호 1옥텟에 raw provisioning PDU`() {
        // Provisioning Capabilities (type 0x01) + 11 octets
        val capabilities = byteArrayOf(0x01) + ByteArray(11) { (it + 1).toByte() }
        val message = RemoteProvisioningPDUReport(
            inboundPduNumber = 2u,
            provisioningPdu = capabilities
        )
        assertArrayEquals(byteArrayOf(0x02) + capabilities, message.parameters)
        assertTrue(message.isSegmented)

        val decoded = RemoteProvisioningPDUReport.init(message.parameters)!!
        assertEquals(2u.toUByte(), decoded.inboundPduNumber)
        assertArrayEquals(capabilities, decoded.provisioningPdu)
    }

    @Test
    fun `PDUReport 는 2옥텟 미만을 거부`() {
        assertNull(RemoteProvisioningPDUReport.init(byteArrayOf(0x01)))
        assertNull(RemoteProvisioningPDUReport.init(byteArrayOf()))
        assertThrows(IllegalArgumentException::class.java) {
            RemoteProvisioningPDUReport(inboundPduNumber = 1u, provisioningPdu = byteArrayOf())
        }
    }

    // endregion
}
