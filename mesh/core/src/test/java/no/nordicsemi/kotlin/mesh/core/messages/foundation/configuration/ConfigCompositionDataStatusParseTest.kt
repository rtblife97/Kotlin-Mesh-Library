package no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration

import no.nordicsemi.kotlin.mesh.core.model.SigModelId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

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
}
