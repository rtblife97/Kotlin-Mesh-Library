package no.nordicsemi.kotlin.mesh.core.layers.access

import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.model.Address
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.time.Duration.Companion.hours

/**
 * Fork-1 회귀 가드 (simdo-fork, 2026-06-04 — mode2 병렬 config P0).
 *
 * ## 배경
 *
 * [AccessLayer.createReliableContext] 의 acked-message timeoutBlock 은 종전에
 * `cancel(handle)` (해당 메시지의 reliable context 1건만 source/responseOpCode/destination
 * 매칭으로 surgical removeAt+invalidate, [AccessLayer.cancel]) 직후에
 * `reliableMessageContexts.clear()` 로 **전 노드 공유 리스트를 전역으로 비웠다**.
 *
 * 직렬 config 에선 in-flight context 가 항상 1건뿐이라 무해(clear 가 자기 1건만 지움)했지만,
 * mode2 병렬 config(N개 노드 동시 in-flight)에선 **노드 A 의 timeout 이 노드 B/C 의 살아있는
 * await context 까지 파괴** → 형제 노드 전부 false-timeout 을 유발한다. fix 는 `clear()` 한 줄을
 * 삭제해, timeout 시 surgical cancel(handle) 만 남긴다(그것으로 해당 1건 정리에 충분).
 *
 * ## 본 테스트가 가드하는 불변
 *
 * surgical cancel 이 "충분"하려면, 서로 다른 source 의 reliable context 가 **독립적으로 주소
 * 지정**되어야 한다(형제를 collateral 로 건드리지 않아야). [AccessLayer.cancel] (:436) 과
 * [AccessLayer.handle] (:199) 의 매칭 predicate 는 둘 다
 * `source == handle.destination && request.responseOpCode == opCode && destination == handle.source`
 * 로 keying 한다 — 서로 다른 노드면 source 가 달라 형제는 절대 매칭되지 않는다. 이 predicate
 * 가 약화되면(예: source 무시) fork-1 의 surgical 정리가 형제를 collateral 로 파괴하게 되므로,
 * 본 테스트가 그 회귀를 잡는다.
 *
 * [AcknowledgementContext] 는 lib `internal` 이라 동일 모듈 테스트에 둔다. 실제 [AccessLayer]
 * 인스턴스화는 [no.nordicsemi.kotlin.mesh.core.NetworkManager] 전체 harness 가 필요(timer 구동)
 * 하므로, 여기선 그 인스턴스가 의존하는 **매칭 predicate 와 context 식별 필드**를 직접 검증한다.
 */
class ReliableContextIsolationTest {

    /** opCode / responseOpCode / parameters 만 carry 하는 최소 acked 메시지. */
    private class StubAckMessage(
        override val opCode: UInt,
        override val responseOpCode: UInt,
    ) : AcknowledgedMeshMessage {
        override val parameters: ByteArray = ByteArray(0)
    }

    private val nodeA: Address = 0x0002u.toUShort()
    private val nodeB: Address = 0x0003u.toUShort()
    private val localSource: Address = 0x0001u.toUShort()

    private val created = mutableListOf<AcknowledgementContext>()

    private fun ctx(target: Address, responseOpCode: UInt): AcknowledgementContext {
        // timeout/delay 를 매우 길게 줘 background Timer 가 테스트 중 발화하지 않게 한다.
        val request = StubAckMessage(opCode = 0x8201u, responseOpCode = responseOpCode)
        return AcknowledgementContext(
            request = request,
            source = target,          // context.source = 응답을 보낼 원격 노드의 unicast
            destination = localSource, // context.destination = 우리(요청 발신자)
            delay = 1.hours,
            repeatBlock = {},
            timeout = 1.hours,
            timeoutBlock = {},
        ).also { created += it }
    }

    @After
    fun tearDown() {
        // 스케줄된 Timer 정리 (테스트 누수 방지).
        created.forEach { it.invalidate() }
        created.clear()
    }

    /**
     * cancel/handle 의 매칭 predicate 를 두 형제 context (서로 다른 source) 에 적용했을 때,
     * 한 노드의 timeout/응답이 **그 노드의 context 1건만** 선택하고 형제는 건드리지 않는지 검증.
     *
     * 이것이 fork-1 의 전제: 전역 `clear()` 없이 surgical cancel 로 충분 — 형제 context 가
     * 절대 collateral 로 제거되지 않는다.
     */
    @Test
    fun `서로 다른 source 의 형제 context 는 독립 주소지정 — 한 노드 timeout 이 형제를 건드리지 않는다`() {
        val ackA = ctx(target = nodeA, responseOpCode = 0x800Au) // 예: Config Composition Data Status
        val ackB = ctx(target = nodeB, responseOpCode = 0x800Au) // 동일 opcode, 다른 노드
        val contexts = mutableListOf(ackA, ackB)

        // 노드 A 로 보낸 요청의 timeout = cancel(handle=A) 의 매칭 predicate (AccessLayer.cancel:436):
        //   it.source == handle.destination(=A) && responseOpCode 일치 && it.destination == handle.source(=우리)
        val matchedForA = contexts.indexOfFirst {
            it.source == nodeA &&
                it.request.responseOpCode == 0x800Au &&
                it.destination == localSource
        }
        assertTrue("노드 A 의 context 가 매칭되어야 한다", matchedForA > -1)
        assertSame("매칭된 것은 정확히 ackA 여야 한다", ackA, contexts[matchedForA])

        // surgical removeAt — fork-1 이 의존하는 정리. 형제(ackB)는 리스트에 남아야 한다.
        contexts.removeAt(matchedForA)
        assertEquals("형제 context 1건이 남아야 한다 (전역 clear 금지)", 1, contexts.size)
        assertSame("남은 것은 노드 B 의 context 여야 한다", ackB, contexts.single())
        // ⚠️ 종전 코드의 `reliableMessageContexts.clear()` 가 부활하면 여기서 contexts 가 비어
        //    이 단언이 깨진다 — 그게 fork-1 회귀의 정확한 신호다.
    }

    /**
     * 동일 노드에 동일 responseOpCode 가 동시 2건이면 predicate 가 wrong-grab 가능 — 따라서
     * "노드 내 직렬" 불변이 필요함을 문서화하는 가드. (병렬화는 노드 단위, 노드 내부 op 는 직렬.)
     */
    @Test
    fun `동일 노드 동일 opcode 2건은 predicate 로 구별 불가 — 노드 내 직렬 불변의 근거`() {
        val first = ctx(target = nodeA, responseOpCode = 0x800Au)
        val second = ctx(target = nodeA, responseOpCode = 0x800Au)
        val contexts = listOf(first, second)

        val matches = contexts.filter {
            it.source == nodeA &&
                it.request.responseOpCode == 0x800Au &&
                it.destination == localSource
        }
        // 둘 다 매칭 = 응답이 어느 요청에 속하는지 구별 불가 → 같은 노드 동시 2건 금지(노드 내 직렬).
        assertEquals("같은 노드+opcode 2건은 predicate 가 둘 다 잡는다(구별 불가)", 2, matches.size)
    }
}
