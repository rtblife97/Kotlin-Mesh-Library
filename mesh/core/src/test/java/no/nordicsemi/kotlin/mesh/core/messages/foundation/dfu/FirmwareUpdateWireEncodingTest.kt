package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.mesh.core.messages.FirmwareId
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareUpdateAdditionalInformation
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareUpdateMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareUpdatePhase
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Firmware Update (DFU target 대면) 메시지의 wire 인코딩·파싱 회귀 가드 (simdo-fork, 2026-08-12).
 *
 * 기준은 NCS `zephyr/subsys/bluetooth/mesh/dfu_srv.c` / `dfu_cli.c` / `dfu.h`.
 * 배경과 방법론은 [FirmwareDistributionWireEncodingTest] 의 헤더 주석 참조.
 *
 * ## 이 파일의 초점
 *
 * 우리는 **클라이언트**다. 따라서
 *  - 요청(*Get / *Check / Start / Cancel / Apply) 의 **인코딩**
 *  - 응답(*Status) 의 **디코딩**
 * 이 실제 필드 경로다. 특히 수신 파싱은 `ModelEventHandler.decode()` 가
 * try/catch 없는 코루틴(`NetworkManager.handle()` 의 `scope.launch`)에서 돌기 때문에,
 * **디코더가 예외를 던지면 RX 경로 자체가 죽는다**. 디코더는 언제나 null 로 degrade 해야 한다.
 */
class FirmwareUpdateWireEncodingTest {

    // region ★ EFFECT_UNPROV 사전 탐지 경로

    /**
     * `FirmwareUpdateFirmwareMetadataStatus` 의 Additional Information 필드는
     * "업데이트 적용 후 노드가 unprovisioned 가 되는가"(`BT_MESH_DFU_EFFECT_UNPROV`) 를
     * **배포 전에** 알아낼 수 있는 유일한 프로토콜 수단이다. comp_hash 변경으로 노드가
     * 망에서 이탈해 버리는 최악의 시나리오를 막는 게이트라서 가장 중요하게 검증한다.
     *
     * 인코딩(NCS dfu_srv.c): `(status & BIT_MASK(3)) | (effect << 3)`.
     */
    @Test
    fun `MetadataStatus 는 DEVICE_UNPROVISIONED 를 정확히 디코드한다`() {
        // status=SUCCESS(0), effect=DEVICE_UNPROVISIONED(0x3) → byte0 = 0 | (3 shl 3) = 0x18
        val decoded = FirmwareUpdateFirmwareMetadataStatus.init(
            parameters = byteArrayOf(0x18, 0x00)
        )

        assertNotNull(decoded)
        assertEquals(FirmwareUpdateMessageStatus.SUCCESS, decoded!!.status)
        assertEquals(
            FirmwareUpdateAdditionalInformation.DEVICE_UNPROVISIONED,
            decoded.additionalInformation
        )
        assertEquals(0x03.toUByte(), decoded.rawAdditionalInformation)
        assertEquals(0x00.toUByte(), decoded.imageIndex)
    }

    @Test
    fun `MetadataStatus 는 알려진 effect 4종을 모두 디코드한다`() {
        mapOf(
            0x00 to FirmwareUpdateAdditionalInformation.COMPOSITION_DATA_UNCHANGED,
            0x01 to FirmwareUpdateAdditionalInformation.COMPOSITION_DATA_CHANGED_AND_RPR_UNSUPPORTED,
            0x02 to FirmwareUpdateAdditionalInformation.COMPOSITION_DATA_CHANGED_AND_RPR_SUPPORTED,
            0x03 to FirmwareUpdateAdditionalInformation.DEVICE_UNPROVISIONED,
        ).forEach { (raw, expected) ->
            val decoded = FirmwareUpdateFirmwareMetadataStatus.init(
                parameters = byteArrayOf((raw shl 3).toByte(), 0x00)
            )
            assertNotNull("effect=0x$raw 디코드 실패", decoded)
            assertEquals(expected, decoded!!.additionalInformation)
        }
    }

    /**
     * Additional Information 은 5비트라 0x04~0x1F 가 wire 상 합법(RFU)이다.
     * upstream 은 매핑 실패 시 메시지 **전체**를 버려서, 유효한 status/imageIndex 까지 잃고
     * 앱은 원인 불명 타임아웃만 봤다. Zephyr 타깃은 0x3 을 넘지 않지만 서드파티(Emblaze)는
     * 그렇지 않을 수 있다.
     *
     * 정정 후: 매핑 실패해도 메시지는 살리고, 원값을 [rawAdditionalInformation] 으로 노출해
     * 호출자가 "모르면 위험한 것으로 취급" 하는 보수적 판단을 할 수 있게 한다.
     */
    @Test
    fun `MetadataStatus 는 RFU effect 값에서도 메시지를 버리지 않는다`() {
        // effect = 0x1F (RFU 최대), status = SUCCESS → byte0 = 0x1F shl 3 = 0xF8
        val decoded = FirmwareUpdateFirmwareMetadataStatus.init(
            parameters = byteArrayOf(0xF8u.toByte(), 0x02)
        )

        assertNotNull("RFU effect 때문에 메시지 전체가 폐기됐다", decoded)
        assertEquals(FirmwareUpdateMessageStatus.SUCCESS, decoded!!.status)
        assertNull("알려지지 않은 값은 enum 으로 매핑되지 않는다", decoded.additionalInformation)
        assertEquals("원값은 보존돼야 한다", 0x1F.toUByte(), decoded.rawAdditionalInformation)
        assertEquals(0x02.toUByte(), decoded.imageIndex)
    }

    @Test
    fun `MetadataStatus 는 길이가 2가 아니면 null`() {
        assertNull(FirmwareUpdateFirmwareMetadataStatus.init(parameters = byteArrayOf(0x00)))
        assertNull(
            FirmwareUpdateFirmwareMetadataStatus.init(parameters = byteArrayOf(0x00, 0x00, 0x00))
        )
        assertNull(FirmwareUpdateFirmwareMetadataStatus.init(parameters = null))
    }

    // endregion

    // region UpdateStatus 디코딩

    /**
     * NCS `dfu_cli.c`: byte0 은 `status`(bits 0-2) 와 `phase`(bits 5-7) 를 함께 싣는다.
     * 전체 14옥텟 형식 = byte0 | ttl | addInfo(5bit) | timeoutBase(le16) | blobId(le64) | imageIndex.
     */
    @Test
    fun `UpdateStatus 14옥텟 형식을 디코드한다 - blobId 는 LE64`() {
        // status=SUCCESS(0), phase=VERIFICATION_SUCCEEDED(0x4) → byte0 = 0 | (4 shl 5) = 0x80
        val decoded = FirmwareUpdateStatus.init(
            parameters = byteArrayOf(
                0x80u.toByte(),                                 // status | phase shl 5
                0x7F,                                           // ttl
                0x03,                                           // additional information (5 bits)
                0x02, 0x00,                                     // timeout base LE16
                0x88u.toByte(), 0x77, 0x66, 0x55,               // blob id LE64 ...
                0x44, 0x33, 0x22, 0x11,                         // ... = 0x1122334455667788
                0x01,                                           // image index
            )
        )

        assertNotNull(decoded)
        assertEquals(FirmwareUpdateMessageStatus.SUCCESS, decoded!!.status)
        assertEquals(FirmwareUpdatePhase.VERIFICATION_SUCCEEDED, decoded.updatePhase)
        assertEquals(0x7F.toUByte(), decoded.updateTtl)
        assertEquals(
            FirmwareUpdateAdditionalInformation.DEVICE_UNPROVISIONED,
            decoded.additionalInformation
        )
        assertEquals(0x0002.toUShort(), decoded.updateTimeoutBase)
        assertEquals(0x1122334455667788uL, decoded.blobId)
        assertEquals(0x01.toUByte(), decoded.imageIndex)
    }

    @Test
    fun `UpdateStatus 단축 1옥텟 형식도 디코드한다`() {
        val decoded = FirmwareUpdateStatus.init(parameters = byteArrayOf(0x00))

        assertNotNull(decoded)
        assertEquals(FirmwareUpdateMessageStatus.SUCCESS, decoded!!.status)
        assertEquals(FirmwareUpdatePhase.IDLE, decoded.updatePhase)
        assertNull(decoded.blobId)
    }

    // endregion

    // region 디코더는 절대 예외를 던지지 않는다 (RX 코루틴 보호)

    /**
     * `FirmwareUpdateInformationStatus.init()` 은 잘린 페이로드에서 인자 1개짜리 `require()`
     * 때문에 `IllegalArgumentException` 을 던졌다. 바로 위/아래의 require 두 개는
     * `{ return@let null }` 로 null 을 반환하는데 이 한 줄만 빠져 있었다.
     */
    @Test
    fun `InformationStatus 는 잘린 페이로드에서 예외 대신 null`() {
        // totalCount, firstIndex 뒤에 fwid 길이(0x04)만 있고 본체가 없는 잘린 PDU.
        val truncated = byteArrayOf(0x01, 0x00, 0x04, 0x59, 0x00, 0x01, 0x02)
        // fwid 4바이트는 채워지지만 그 뒤 Update URI 길이 옥텟이 없다 → 예전엔 여기서 throw.
        val decoded = FirmwareUpdateInformationStatus.init(parameters = truncated)

        assertNull("잘린 PDU 는 예외가 아니라 null 이어야 한다", decoded)
    }

    @Test
    fun `InformationStatus 는 잘린 길이 조합 전수에서 예외를 던지지 않는다`() {
        val full = byteArrayOf(0x01, 0x00, 0x04, 0x59, 0x00, 0x01, 0x02, 0x00)
        // 모든 prefix 길이에 대해 예외 없이 끝나야 한다.
        for (len in 0..full.size) {
            val slice = full.copyOfRange(0, len)
            // 예외가 나면 이 테스트는 실패한다 (assert 없이 호출 자체가 검증).
            FirmwareUpdateInformationStatus.init(parameters = slice)
        }
    }

    @Test
    fun `InformationStatus 는 URI 로 파싱 불가한 바이트에서도 예외를 던지지 않는다`() {
        // Update URI 자리에 URI 문법상 불법인 제어문자/공백을 넣는다.
        // upstream 은 URI.create() / toURL() 을 무방비로 호출해 RX 코루틴으로 예외를 흘렸다.
        val pdu = byteArrayOf(
            0x01, 0x00,             // totalCount, firstIndex
            0x04,                   // current firmware id length (cid 2 + version 2)
            0x59, 0x00, 0x01, 0x02, // cid LE16 + version
            0x03,                   // update uri length
            0x17, 0x20, 0x7F,       // URI 로 해석 불가한 바이트열
        )
        val decoded = FirmwareUpdateInformationStatus.init(parameters = pdu)

        // 메시지 자체는 살아야 하고, updateUri 만 null 로 떨어진다.
        assertNotNull("URI 파싱 실패가 메시지 전체를 죽여선 안 된다", decoded)
        assertEquals(1, decoded!!.list.size)
        assertNull(decoded.list.single().updateUri)
        assertEquals(0x0059.toUShort(), decoded.list.single().currentFirmwareId.companyIdentifier)
    }

    /**
     * `FirmwareUpdateStart.init()` 의 술어가 `it.isEmpty()` 로 뒤집혀 있었다.
     * 정상 메시지는 무시되고, 빈 페이로드는 술어를 통과한 뒤 `params[0]` 에서
     * ArrayIndexOutOfBoundsException 을 던졌다. NCS 는 `BT_MESH_LEN_MIN(12)`.
     */
    @Test
    fun `UpdateStart 디코더는 빈 페이로드에서 터지지 않고 12옥텟 이상만 받는다`() {
        assertNull("빈 페이로드는 null (예전엔 AIOOBE)", FirmwareUpdateStart.init(parameters = byteArrayOf()))
        assertNull(FirmwareUpdateStart.init(parameters = ByteArray(size = 11)))
        assertNotNull("12옥텟 이상은 파싱돼야 한다", FirmwareUpdateStart.init(parameters = ByteArray(size = 12)))
    }

    @Test
    fun `UpdateStart 는 wire 왕복이 보존된다`() {
        val original = FirmwareUpdateStart(
            updateTtl = 0x7Fu,
            updateTimeoutBase = 0x0002u,
            blobId = 0x1122334455667788uL,
            imageIndex = 0x01u,
            metaData = byteArrayOf(0xAAu.toByte()),
        )
        assertEquals("ttl+timeout+blobId+idx+meta = 1+2+8+1+1", 13, original.parameters.size)

        val decoded = FirmwareUpdateStart.init(parameters = original.parameters)
        assertNotNull(decoded)
        assertEquals(0x7F.toUByte(), decoded!!.updateTtl)
        assertEquals(0x0002.toUShort(), decoded.updateTimeoutBase)
        assertEquals(0x1122334455667788uL, decoded.blobId)
        assertEquals(0x01.toUByte(), decoded.imageIndex)
    }

    // endregion

    // region FirmwareId 표시

    /**
     * 8바이트 버전 형식은 (major U8, minor U8, revision U16, build U32).
     * upstream 은 `8 ->` 분기에서 build 만 읽어 revision 이 항상 0 으로 보였다.
     */
    @Test
    fun `versionString 은 8바이트 형식에서 revision 을 잃지 않는다`() {
        val firmwareId = FirmwareId(
            companyIdentifier = 0x0059u,
            version = byteArrayOf(
                0x01,                   // major = 1
                0x02,                   // minor = 2
                0x03, 0x00,             // revision = 3 (LE16)
                0x04, 0x00, 0x00, 0x00, // build = 4 (LE32)
            )
        )
        assertEquals("1.2.3+4", firmwareId.versionString)
    }

    @Test
    fun `versionString 4바이트 형식은 revision 까지만 읽는다`() {
        val firmwareId = FirmwareId(
            companyIdentifier = 0x0059u,
            version = byteArrayOf(0x01, 0x02, 0x03, 0x00)
        )
        assertEquals("1.2.3", firmwareId.versionString)
    }

    @Test
    fun `FirmwareId 는 CID LE16 + version 으로 직렬화되고 왕복한다`() {
        val original = FirmwareId(
            companyIdentifier = 0x0059u,
            version = byteArrayOf(0x01, 0x02)
        )
        assertEquals(0x59.toByte(), original.bytes[0])
        assertEquals(0x00.toByte(), original.bytes[1])

        val roundTripped = FirmwareId(original.bytes)
        assertEquals(0x0059.toUShort(), roundTripped.companyIdentifier)
        assertTrue(roundTripped.version.contentEquals(byteArrayOf(0x01, 0x02)))
    }

    /**
     * `COMPOSITION_DATA_CHANGED_AND_RPR_SUPPORTED` 가 `_UNSUPPORTED` 와 동일한 문자열을
     * 반환해, 하필 이 effect 필드를 로그로 디버깅할 때 오독을 유발했다.
     */
    @Test
    fun `effect 설명 문자열이 SUPPORTED 와 UNSUPPORTED 를 구분한다`() {
        val supported =
            FirmwareUpdateAdditionalInformation.COMPOSITION_DATA_CHANGED_AND_RPR_SUPPORTED
        val unsupported =
            FirmwareUpdateAdditionalInformation.COMPOSITION_DATA_CHANGED_AND_RPR_UNSUPPORTED

        assertTrue(supported.debugDescription != unsupported.debugDescription)
        assertTrue(supported.debugDescription.endsWith("Provisioning Supported"))
        assertTrue(unsupported.debugDescription.endsWith("Provisioning Unsupported"))
    }

    // endregion
}
