package no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration

import no.nordicsemi.kotlin.mesh.core.model.Insecure
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.SigModelId
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * [ConfigCompositionDataStatus] / [Page0] strict parser 회귀 테스트.
 *
 * simdo S21U Configure-all e2e (2026-05-26) 에서 NLC-BLC-DUT(0x0106) 의 Composition Data Get 이
 * "Unexpected response type: UnknownMessage" 로 실패. 진단 결과 parser 자체는 정상 — 실패 원인은
 * 로컬 provisioner node 의 Config* model handler 미attach (transient eventHandler null) 였음.
 *
 * 본 테스트는 그 실측 53-byte payload 가 parser 레벨에서 정상 parse 됨을 못박아, 향후 parser
 * 회귀(잘못된 방향의 "수정")를 차단한다.
 */
@OptIn(ExperimentalUuidApi::class)
class ConfigCompositionDataStatusParseTest {

    /** S21U 실측 Composition Data Page 0 응답 (opcode 0x02 제거 후 parameters). */
    // raw access payload(opcode 0x02 제거) = S21U logcat 실측 그대로.
    private val nlcBlcPage0Params: ByteArray = byteArrayOf(
        0x00, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF.toByte(), 0x00, 0x07, 0x00,
        // Element 1: Loc=0x0001, NumS=14, NumV=0 (28 byte SIG model body)
        0x01, 0x00, 0x0E, 0x00,
        0x00, 0x00, 0x02, 0x00, 0x0E, 0x00, 0x02, 0x10, 0x00, 0x10, 0x04, 0x10,
        0x06, 0x10, 0x07, 0x10, 0x00, 0x13, 0x01, 0x13, 0x03, 0x12, 0x04, 0x12,
        0x00, 0x11, 0x01, 0x11,
        // Element 2: Loc=0x0002, NumS=3, NumV=0 (6 byte SIG model body)
        0x02, 0x00, 0x03, 0x00,
        0x00, 0x10, 0x0F, 0x13, 0x10, 0x13,
    )

    @Test
    fun `S21U NLC-BLC page 0 parses into ConfigCompositionDataStatus`() {
        val status = ConfigCompositionDataStatus.init(nlcBlcPage0Params)

        assertNotNull(status, "53-byte NLC-BLC page 0 must parse — parser is not the failure source")
        val page0 = status.page as Page0
        assertEquals(0x0059u.toUShort(), page0.companyIdentifier, "CID = Nordic 0x0059")
        assertEquals(255u.toUShort(), page0.minimumNumberOfReplayProtectionList, "CRPL = 255")
        assertEquals(2, page0.elements.size, "two elements")
        assertEquals(14, page0.elements[0].models.size, "element 1 has 14 SIG models")
        assertEquals(3, page0.elements[1].models.size, "element 2 has 3 SIG models")
        // spot-check a couple of model ids
        assertEquals(
            SigModelId(0x0000u.toUShort()),
            page0.elements[0].models[0].modelId,
            "element 1 model 0 = Config Server",
        )
        assertEquals(
            SigModelId(0x1310u.toUShort()),
            page0.elements[1].models[2].modelId,
            "element 2 last model = 0x1310",
        )
    }

    @Test
    fun `opcode 0x02 is registered for single-octet decode`() {
        assertEquals(0x02u, ConfigCompositionDataStatus.opCode, "Config Composition Data Status = 0x02 (1-octet)")
    }

    @Test
    fun `truncated page 0 returns null (strict size guard)`() {
        // header(11) + element header claims NumS=14 (28 bytes) but body truncated
        val truncated = nlcBlcPage0Params.copyOfRange(0, 20)
        assertNull(
            ConfigCompositionDataStatus.init(truncated),
            "truncated element body must fail the require() size guard → null",
        )
    }

    // ------------------------------------------------------------------------------------------
    // simdo-patch (2026-08-26) — Composition Data Page 128 (MshPRT 1.1 §4.2.1.2)
    //
    // NPPI 의 Node Composition Refresh(§3.11.8.6)가 Page 0 으로 승격시킬 **예정** composition.
    // 종전 parser 는 첫 옥텟이 0 이 아니면 무조건 null 을 돌려줘 CDP128 을 전혀 못 읽었다.
    // ------------------------------------------------------------------------------------------

    /** 같은 실측 payload 의 page 옥텟만 0x80 으로 바꾼 것 — 나머지 레이아웃은 Page 0 과 동일. */
    private val nlcBlcPage128Params: ByteArray =
        nlcBlcPage0Params.copyOf().also { it[0] = 0x80.toByte() }

    @Test
    fun `page 128 parses with the same layout as page 0`() {
        val status = ConfigCompositionDataStatus.init(nlcBlcPage128Params)

        assertNotNull(status, "Page 128 must decode — it is the wire twin of Page 0")
        val page128 = status.page as Page128
        assertEquals(0x80u.toUByte(), page128.page, "page number = 0x80")
        assertEquals(0x0059u.toUShort(), page128.companyIdentifier, "CID = Nordic 0x0059")
        assertEquals(2, page128.elements.size, "two elements")
        assertEquals(14, page128.elements[0].models.size, "element 1 has 14 SIG models")
        assertEquals(3, page128.elements[1].models.size, "element 2 has 3 SIG models")
    }

    @Test
    fun `page 128 round-trips through parameters`() {
        val page128 = (ConfigCompositionDataStatus.init(nlcBlcPage128Params)?.page as Page128)

        // ⚠️ Features 옥텟(9..10)은 **의도적으로 제외**한다. 라이브러리의 `Features` 는
        //   (a) `rawValue` 가 `shl` 대신 `shr` 을 써서 항상 0 을 만들고,
        //   (b) `Features.init(mask)` 가 "지원함" 비트를 `null`(= 상태 미상)로 매핑해
        //       애초에 왕복이 불가능한 표현을 쓴다.
        // 둘 다 **이 패치 이전부터 있던 결함**이라 여기서 고치지 않는다 —
        // Page 0 도 정확히 같은 코드를 쓰므로 CDP128 만의 문제가 아니다. 이 테스트가 봉인하는
        // 것은 "CDP128 의 나머지 레이아웃이 CDP0 과 바이트 동일하게 왕복한다" 는 사실이다.
        val roundTripped = page128.parameters
        assertContentEquals(
            expected = nlcBlcPage128Params.copyOfRange(0, 9),
            actual = roundTripped.copyOfRange(0, 9),
            message = "page 옥텟 + CID/PID/VID/CRPL 이 그대로 재인코딩되어야 한다",
        )
        assertContentEquals(
            expected = nlcBlcPage128Params.copyOfRange(11, nlcBlcPage128Params.size),
            actual = roundTripped.copyOfRange(11, roundTripped.size),
            message = "Element/Model 본문이 바이트 단위로 그대로 재인코딩되어야 한다",
        )
        assertEquals(0x80.toByte(), roundTripped[0], "재인코딩된 page 번호는 0x80")
    }

    @Test
    fun `page 128 and page 0 of the same payload decode to different types`() {
        val page0 = ConfigCompositionDataStatus.init(nlcBlcPage0Params)?.page
        val page128 = ConfigCompositionDataStatus.init(nlcBlcPage128Params)?.page

        assertTrue(page0 is Page0, "0x00 → Page0")
        assertTrue(page128 is Page128, "0x80 → Page128")
    }

    @Test
    fun `page 128 parser rejects a page 0 payload and vice versa`() {
        assertNull(Page128.init(nlcBlcPage0Params), "Page128.init must not accept a page-0 payload")
        assertNull(Page0.init(nlcBlcPage128Params), "Page0.init must not accept a page-128 payload")
    }

    @Test
    fun `unsupported page numbers still decode to null`() {
        // Page 1 / 2 는 아직 미지원 — 조용히 null 이어야 하고 예외를 던지면 안 된다
        // (ModelEventHandler.decode() 에 try/catch 가 없어 RX 코루틴이 죽는다).
        val page1 = nlcBlcPage0Params.copyOf().also { it[0] = 0x01 }
        assertNull(ConfigCompositionDataStatus.init(page1), "page 1 is not supported yet")
    }

    @Test
    fun `applying a page 128 status leaves the node untouched`() {
        // Page 128 은 아직 활성 composition 이 아니다. CDB 에 반영하면 안 되고,
        // 예외를 던져서도 안 된다 (종전 requireNotNull(page as? Page0) 는 던졌다).
        val network = MeshNetwork(name = "CDP128").apply { add(name = "NetKey") }
        val node = Node(
            name = "Target",
            uuid = Uuid.random(),
            deviceKey = ByteArray(16) { 0x11 },
            unicastAddress = UnicastAddress(address = 0x0100u),
            elementCount = 1,
            assignedNetworkKey = network.networkKeys.first(),
            security = Insecure,
        )
        network.add(node = node)
        val before = node.elementsCount

        val status = ConfigCompositionDataStatus.init(nlcBlcPage128Params)!!
        node.apply(compositionData = status)

        assertEquals(before, node.elementsCount, "Page 128 must not rewrite the Element list")
        assertNull(node.companyIdentifier, "Page 128 must not set the Composition Data markers")
    }
}
