package no.nordicsemi.kotlin.mesh.core.layers.access

import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigAppKeyAdd
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigAppKeyUpdate
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNetKeyAdd
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNetKeyUpdate
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * simdo-fork (2026-09-29) — **키 자료는 어떤 로그 문자열에도 나오지 않는다** (불변). 실기기 logcat 에 `Mesh-FOUNDATION_MODEL` 의
 * `ConfigNetKeyAdd(… key: 0x…)` 와 `Mesh-ACCESS` 의 `Sending Access PDU (opCode: 0x8040, parameters: 0x…)` 로 NetKey·AppKey 가 평문으로
 * 찍혔다. 두 로그 모두 이 문자열(`toString`) 을 그대로 쓴다. 키 인덱스는 식별에 필요하므로 남는다.
 */
class KeyMaterialNotLoggedTest {

    private val key = ByteArray(16) { (0xA0 + it).toByte() }

    @OptIn(ExperimentalStdlibApi::class)
    private val keyHex = key.toHexString(HexFormat.UpperCase)

    private fun accessPdu(m: MeshMessage) = AccessPdu.init(
        message = m,
        source = 0x0001u,
        destination = UnicastAddress(address = 0x0102u),
        userInitiated = true,
    )

    private fun assertNoKey(label: String, text: String) {
        assertFalse("$label 에 키가 보임: $text", text.uppercase().contains(keyHex))
        assertTrue("$label 에 숨김 표시가 없음: $text", text.contains("숨김"))
    }

    @Test
    fun `key-carrying config messages never print the key in their message or access pdu log strings`() {
        val messages = listOf(
            ConfigAppKeyAdd(applicationKeyIndex = 1u, key = key, networkKeyIndex = 0u),
            ConfigAppKeyUpdate(applicationKeyIndex = 1u, key = key, networkKeyIndex = 0u),
            ConfigNetKeyAdd(networkKeyIndex = 1u, key = key),
            ConfigNetKeyUpdate(networkKeyIndex = 1u, newKey = key),
        )
        for (m in messages) {
            assertNoKey(m.javaClass.simpleName, m.toString())
            assertNoKey("${m.javaClass.simpleName} Access PDU", accessPdu(m).toString())
        }
        // 키 인덱스(식별 정보)는 남는다: NetKey Add = NetKeyIndex 2 B (0x0100 리틀엔디언).
        assertTrue(accessPdu(ConfigNetKeyAdd(networkKeyIndex = 1u, key = key)).toString().contains("parameters: 0x0100"))
    }
}
