package no.nordicsemi.kotlin.mesh.core.layers.network

import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.hours
import kotlin.time.ExperimentalTime

/**
 * simdo-fork (2026-09-30) — IV Index Recovery (Mesh Profile 1.0.1 §3.10.6).
 *
 * 실기기 2026-09-30 16:08 (파인테크닉스 f_idx 587): 앱은 IV 0 을 1 시간 전에 기억했고 망은 IV 2. 비콘 IV 2 를 "상태 4 단계 = 384 h 필요" 로
 * 버려 앱이 보내는 메시지가 전부 조명에서 버려졌다.
 */
@OptIn(ExperimentalTime::class)
class IvIndexRecoveryTest {
    private val key = NetworkKey(index = 0u, _name = "primary")
    private fun beacon(index: UInt, active: Boolean = false) =
        SecureNetworkBeacon(ByteArray(22), key, validForKeyRefreshProcedure = false, keyRefreshFlag = false, ivIndex = IvIndex(index, active))

    @Test
    fun `a - 1 시간 전에 IV 0 을 본 뒤 비콘 IV 2 는 복구로 받는다`() {
        val now = Clock.System.now()
        val last = IvIndex(0u, false, now - 1.hours)
        assertTrue(
            beacon(2u).canOverWrite(
                target = last, updatedAt = last.transitionDate, isIvRecoveryActive = false,
                isIvTestModeActive = false, ivRecoveryOver42Allowed = false,
            ),
        )
    }
}
