package no.nordicsemi.kotlin.mesh.bearer

import org.junit.Assert
import org.junit.Test

/**
 * simdo-patch (2026-08-12): upstream 의 이 두 테스트는 **항상 실패**하고 있었다.
 * hex 문자열을 `String.toByteArray()` 로 바꿔 놓고(= 그 텍스트의 UTF-8 인코딩) 결과를 hex 로
 * 비교했기 때문이다. 의도는 `hexToByteArray()` 다. 프로덕션 [ProxyProtocolHandler] 는 무관하며
 * 테스트만 고쳤다. (감사 목록 외 항목 — `:mesh:bearer:test` 를 green 으로 만들기 위한 정리)
 */
@OptIn(ExperimentalStdlibApi::class)
class ProxyProtocolHandlerTest {

    @Test
    fun segment() {
        val expectedMessage =
            "430339C31CD3EFB53FD664443882AA4E4E4DDED7BE6063D16E5EA84CBF09" +
                    "8337205C8D0854AE88FC98873FE58B699FD4638924C3D21824C0CCD64722" +
                    "C38F1535C73D6FC8"
        val message =
            ("0339c31cd3efb53fd664443882aa4e4e4dded7be6063d16e5ea84cbf0937205c8d0854ae88fc9887" +
                    "3fe58b699fd4638924c3d21824c0ccd647228f1535c73d6fc8").uppercase()
        val handler = ProxyProtocolHandler()
        val actualMessage =
            handler.segment(data = message.hexToByteArray(), type = PduType.PROVISIONING_PDU, mtu = 30)
                .joinToString("") { it.toHexString() }
                // toHexString() 은 소문자를 내보낸다. 기대값은 대문자 리터럴.
                .uppercase()
        Assert.assertEquals(expectedMessage, actualMessage)
    }

    @Test
    fun reassemble() {
        val expectedMessage =
            ("0339c31cd3efb53fd664443882aa4e4e4dded7be6063d16e5ea84cbf0937205c8d0854ae88fc9887" +
                    "3fe58b699fd4638924c3d21824c0ccd647228f1535c73d6fc8").uppercase()
        val messages = listOf(
            "430339C31CD3EFB53FD664443882AA4E4E4DDED7BE6063D16E5EA84CBF09".hexToByteArray(),
            "8337205C8D0854AE88FC98873FE58B699FD4638924C3D21824C0CCD64722".hexToByteArray(),
            "C38F1535C73D6FC8".hexToByteArray()
        )
        val handler = ProxyProtocolHandler()
        var actualMessage = ""
        messages.forEach { message ->
            handler.reassemble(message)?.let {
                actualMessage += it.data.toHexString().uppercase()
            }
        }
        Assert.assertEquals(expectedMessage, actualMessage)
    }
}