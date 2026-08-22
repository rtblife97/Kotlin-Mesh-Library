@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import no.nordicsemi.kotlin.data.toUuid
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkCloseReason
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkState
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkClose
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkOpen
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUOutboundReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUSend
import no.nordicsemi.kotlin.mesh.provisioning.bearer.PBRemoteBearer
import no.nordicsemi.kotlin.mesh.provisioning.bearer.PBRemoteBearerError
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * PB-Remote 링크 상태머신의 **동시성 회귀 가드** (simdo-fork, 2026-08-12 — 감사 P1-1 / P1-2 / P2-d).
 *
 * 감사가 지적한 공백: `PBRemoteBearerContractTest` 11건은 전부 사전조건만 보고
 * `open → send → close → 비동기 Link Report` 경로를 한 줄도 덮지 않는다. P1-1(닫힌 베어러 부활)과
 * P1-2(코루틴 간 필드 가시성)는 정확히 그 공백에서 나왔다.
 *
 * 여기서 잠그는 세 가지 race:
 *  (a) Link Report ACTIVE 직후 IDLE 이 연속 도착 — 베어러가 되살아나면 안 된다
 *  (b) `send()` 가 Outbound Report 를 기다리는 도중 링크가 죽음 — 15초 타임아웃이 아니라 즉시,
 *      원인을 담아 실패해야 한다
 *  (c) 재시도 진행 중 링크가 죽음 — 두 번째 시도가 헛돌지 않아야 한다
 *
 * 가상 시간(`runTest` + `StandardTestDispatcher`)을 쓰므로 15초/30초 타임아웃도 즉시 검증된다.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PBRemoteBearerRaceTest {

    private val uuid: Uuid = byteArrayOf(
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10
    ).toUuid()

    private val server: UShort = 0x0002u

    private fun linkStatus(state: RemoteProvisioningLinkState) = RemoteProvisioningLinkStatus(
        status = RemoteProvisioningMessageStatus.SUCCESS,
        linkState = state
    )

    private fun linkReport(
        state: RemoteProvisioningLinkState,
        status: RemoteProvisioningMessageStatus = RemoteProvisioningMessageStatus.SUCCESS,
    ) = RemoteProvisioningLinkReport(status = status, linkState = state)

    // ── (a) ACTIVE → IDLE 연속 도착 (감사 P1-1) ────────────────────────────────

    /**
     * `open()` 이 `active.await()` 에서 깨어나기 전에 서버가 IDLE 을 보내면, teardown 이 먼저
     * 끝난 상태에서 `_state = Opened` 를 덮게 된다. 그러면 `isOpen == true` 인데 `pdus` 채널은
     * 이미 닫혀 있는 **좀비 베어러**가 되고, 실패는 `ProvisioningManager` 가 provision() 을
     * 시작한 뒤 첫 PDU 에서야 드러난다.
     */
    @Test
    fun `ACTIVE 직후 IDLE 이 오면 베어러가 되살아나지 않는다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { message ->
            when (message) {
                is RemoteProvisioningLinkOpen -> linkStatus(RemoteProvisioningLinkState.LINK_OPENING)
                is RemoteProvisioningLinkClose -> linkStatus(RemoteProvisioningLinkState.IDLE)
                else -> null
            }
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            // 이 테스트만 Unconfined: observer 와 supervisor 가 emit 시점에 **즉시** 돌아야
            // "teardown 이 먼저 끝난 뒤 open() 이 깨어난다" 는 순서를 결정적으로 만들 수 있다.
            // (testScheduler 를 공유해야 한다 — 고아 UnconfinedTestDispatcher() 금지.)
            ioDispatcher = UnconfinedTestDispatcher(testScheduler),
        )

        val opening = async { runCatching { bearer.open() } }
        runCurrent()

        // 서버가 ACTIVE 를 알린 직후 링크를 잃는다 (기기가 광고를 멈춤 / 범위 이탈).
        transport.receive(linkReport(RemoteProvisioningLinkState.LINK_ACTIVE))
        transport.receive(
            linkReport(
                state = RemoteProvisioningLinkState.IDLE,
                status = RemoteProvisioningMessageStatus.LINK_CLOSED_BY_DEVICE
            )
        )
        runCurrent()

        val result = opening.await()
        assertTrue("open() 은 실패로 끝나야 한다", result.isFailure)
        assertTrue(
            "원인은 LinkClosed 여야 한다 (실제: ${result.exceptionOrNull()})",
            result.exceptionOrNull() is PBRemoteBearerError.LinkClosed
        )
        assertFalse("좀비 베어러: isOpen 이 true 로 남았다", bearer.isOpen)
        assertTrue(bearer.state.value is BearerEvent.Closed)

        // 닫힌 채널이 열린 베어러처럼 보이지 않는지 — send 는 즉시 거부돼야 한다.
        val sendResult = runCatching {
            bearer.send(pdu = byteArrayOf(0x00, 0x05), type = PduType.PROVISIONING_PDU)
        }
        assertTrue(sendResult.isFailure)
    }

    @Test
    fun `정상 경로에서는 ACTIVE Link Report 로 open 이 완료된다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { linkStatus(RemoteProvisioningLinkState.LINK_OPENING) }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )

        val opening = async { bearer.open() }
        runCurrent()
        transport.receive(linkReport(RemoteProvisioningLinkState.LINK_ACTIVE))
        runCurrent()
        opening.await()

        assertTrue(bearer.isOpen)
        assertEquals(1, transport.sentAcknowledged.size)
        assertTrue(transport.sentAcknowledged.first() is RemoteProvisioningLinkOpen)
    }

    // ── (b) send 중 Link Report 도착 (감사 P1-2) ───────────────────────────────

    /**
     * `send()` 는 PDU Outbound Report 를 기다리며 최대 15초 suspend 한다. 그 사이 서버가 링크를
     * 닫으면 대기 중인 deferred 를 **예외로 완료**해야 한다.
     *
     * `CompletableDeferred.cancel()` 로 깨우면 `await()` 가 평범한 `CancellationException` 을
     * 던지고, 그것은 `withTimeoutOrNull` 이 잡지 않아 **호출자 코루틴이 통째로 취소된다** —
     * 앱 입장에서는 provisioning flow 가 원인 없이 사라진다. 그래서 `completeExceptionally` 다.
     */
    @Test
    fun `send 대기 중 링크가 죽으면 타임아웃이 아니라 즉시 원인과 함께 실패한다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { message ->
            when (message) {
                is RemoteProvisioningLinkOpen -> linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE)
                is RemoteProvisioningLinkClose -> linkStatus(RemoteProvisioningLinkState.IDLE)
                else -> null
            }
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { bearer.open() }
        runCurrent()
        opening.await()
        assertTrue(bearer.isOpen)

        val startedAt = testScheduler.currentTime
        val sending = async {
            runCatching {
                bearer.send(pdu = byteArrayOf(0x00, 0x05), type = PduType.PROVISIONING_PDU)
            }
        }
        runCurrent()
        assertEquals(1, transport.sentUnacknowledged.size)

        // 서버가 PDU 전달 중 링크를 잃었다.
        transport.receive(
            linkReport(
                state = RemoteProvisioningLinkState.IDLE,
                status = RemoteProvisioningMessageStatus.LINK_CLOSED_AS_CANNOT_SEND_PDU
            )
        )
        runCurrent()

        val result = sending.await()
        assertTrue("send() 는 실패해야 한다", result.isFailure)
        assertTrue(
            "원인은 LinkClosed 여야 한다 (실제: ${result.exceptionOrNull()})",
            result.exceptionOrNull() is PBRemoteBearerError.LinkClosed
        )
        assertTrue(
            "15초 타임아웃을 기다리지 않고 즉시 끝나야 한다 (경과 ${testScheduler.currentTime - startedAt} ms)",
            testScheduler.currentTime - startedAt < PBRemoteBearer.DEFAULT_OUTBOUND_PDU_TIMEOUT.inWholeMilliseconds
        )
        // 재전송을 시도하지 않았다.
        assertEquals(1, transport.sentUnacknowledged.size)
        assertFalse(bearer.isOpen)
    }

    @Test
    fun `Outbound Report 가 오면 send 가 정상 종료되고 PDU 번호가 1부터 증가한다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE) }
        var next: UByte = 0u
        transport.onUnacknowledgedSent = { message ->
            if (message is RemoteProvisioningPDUSend) {
                next = message.outboundPduNumber
                transport.receive(RemoteProvisioningPDUOutboundReport(outboundPduNumber = next))
            }
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { bearer.open() }
        runCurrent()
        opening.await()

        val first = async { bearer.send(byteArrayOf(0x00, 0x05), PduType.PROVISIONING_PDU) }
        runCurrent()
        first.await()
        assertEquals(1u.toUByte(), next)

        val second = async { bearer.send(byteArrayOf(0x02, 0x00), PduType.PROVISIONING_PDU) }
        runCurrent()
        second.await()
        assertEquals(2u.toUByte(), next)
        assertEquals(2, transport.sentUnacknowledged.size)
    }

    /**
     * `send()` 반환과 `ProvisioningManager` 의 `pdus.first { … }` 구독 사이의 창에 PDU Report 가
     * 도착해도 유실되면 안 된다. `MutableSharedFlow(replay = 0)` 였다면 여기서 드롭됐고
     * 프로비저닝은 15초 재시도까지 정지했다 — [Channel] 백킹의 존재 이유.
     */
    @Test
    fun `send 완료와 pdus 구독 사이에 도착한 PDU Report 는 버퍼링된다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE) }
        transport.onUnacknowledgedSent = { message ->
            if (message is RemoteProvisioningPDUSend) {
                // 서버가 Outbound Report 와 디바이스 응답을 연달아 보낸다.
                transport.receive(
                    RemoteProvisioningPDUOutboundReport(
                        outboundPduNumber = message.outboundPduNumber
                    )
                )
                transport.receive(
                    RemoteProvisioningPDUReport(
                        inboundPduNumber = 1u,
                        provisioningPdu = byteArrayOf(0x01, 0x0A)
                    )
                )
            }
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { bearer.open() }
        runCurrent()
        opening.await()

        val sending = async { bearer.send(byteArrayOf(0x00, 0x05), PduType.PROVISIONING_PDU) }
        runCurrent()
        sending.await()

        // ProvisioningManager 는 send() 가 반환한 **뒤에야** 구독한다.
        val received = async { withTimeoutOrNull(5.seconds) { bearer.pdus.first() } }
        runCurrent()
        val pdu = received.await()
        assertNotNull("send() 이후에 구독해도 PDU Report 가 남아 있어야 한다", pdu)
        assertEquals(PduType.PROVISIONING_PDU, pdu!!.type)
        assertEquals(0x0A.toByte(), pdu.data[1])
    }

    @Test
    fun `중복 PDU Report 는 Inbound PDU Count 로 억제된다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE) }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { bearer.open() }
        runCurrent()
        opening.await()

        transport.receive(RemoteProvisioningPDUReport(1u, byteArrayOf(0x01, 0xAA.toByte())))
        transport.receive(RemoteProvisioningPDUReport(1u, byteArrayOf(0x01, 0xBB.toByte())))
        transport.receive(RemoteProvisioningPDUReport(2u, byteArrayOf(0x01, 0xCC.toByte())))
        runCurrent()

        val collected = mutableListOf<Byte>()
        val reader = async {
            withTimeoutOrNull(1.seconds) {
                repeat(2) { collected.add(bearer.pdus.first().data[1]) }
            }
        }
        runCurrent()
        reader.await()
        assertEquals(listOf(0xAA.toByte(), 0xCC.toByte()), collected)
    }

    // ── (c) 재시도 중 취소/링크 종료 ───────────────────────────────────────────

    /**
     * 첫 시도가 타임아웃돼 재전송에 들어간 뒤 링크가 죽는 경우. 두 번째 시도가 다시 15초를
     * 헛돌면 안 되고, `OutboundPduTimeout` 이 아니라 실제 원인이 나와야 한다.
     */
    @Test
    fun `재시도 중 링크가 죽으면 두 번째 대기가 헛돌지 않는다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { message ->
            when (message) {
                is RemoteProvisioningLinkOpen -> linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE)
                is RemoteProvisioningLinkClose -> linkStatus(RemoteProvisioningLinkState.IDLE)
                else -> null
            }
        }
        val secondAttempt = CompletableDeferred<Unit>()
        transport.onUnacknowledgedSent = { message ->
            if (message is RemoteProvisioningPDUSend && transport.sentUnacknowledged.size == 2) {
                secondAttempt.complete(Unit)
            }
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { bearer.open() }
        runCurrent()
        opening.await()

        val sending = async {
            runCatching { bearer.send(byteArrayOf(0x00, 0x05), PduType.PROVISIONING_PDU) }
        }
        runCurrent()
        val startedAt = testScheduler.currentTime
        // 첫 시도는 Outbound Report 없이 15초 타임아웃 → 재전송.
        advanceTimeBy(PBRemoteBearer.DEFAULT_OUTBOUND_PDU_TIMEOUT + 1.seconds)
        runCurrent()
        assertTrue("재전송이 일어나야 한다", secondAttempt.isCompleted)
        assertEquals(
            "같은 Outbound PDU 번호로 재전송해야 한다",
            (transport.sentUnacknowledged[0] as RemoteProvisioningPDUSend).outboundPduNumber,
            (transport.sentUnacknowledged[1] as RemoteProvisioningPDUSend).outboundPduNumber
        )

        // 재전송을 기다리는 사이 서버가 링크를 잃는다.
        transport.receive(
            linkReport(
                state = RemoteProvisioningLinkState.IDLE,
                status = RemoteProvisioningMessageStatus.LINK_CLOSED_AS_CANNOT_SEND_PDU
            )
        )
        runCurrent()

        val result = sending.await()
        assertTrue(result.isFailure)
        assertTrue(
            "두 번째 대기도 15초 헛돌면 안 된다 (실제: ${result.exceptionOrNull()})",
            result.exceptionOrNull() is PBRemoteBearerError.LinkClosed
        )
        assertTrue(
            "두 번째 타임아웃까지 기다리지 않아야 한다 (경과 ${testScheduler.currentTime - startedAt} ms)",
            testScheduler.currentTime - startedAt <
                    2 * PBRemoteBearer.DEFAULT_OUTBOUND_PDU_TIMEOUT.inWholeMilliseconds
        )
        assertEquals("재전송은 한 번뿐", 2, transport.sentUnacknowledged.size)
        assertFalse(bearer.isOpen)
    }

    /**
     * 두 번 다 Outbound Report 가 없으면 [PBRemoteBearerError.OutboundPduTimeout] 이고,
     * MshPRT 정합상 링크는 `reason = Fail` 로 닫아야 한다.
     */
    @Test
    fun `두 번 다 무응답이면 OutboundPduTimeout 이고 Fail 로 닫는다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { message ->
            when (message) {
                is RemoteProvisioningLinkOpen -> linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE)
                is RemoteProvisioningLinkClose -> linkStatus(RemoteProvisioningLinkState.IDLE)
                else -> null
            }
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { bearer.open() }
        runCurrent()
        opening.await()

        val sending = async {
            runCatching { bearer.send(byteArrayOf(0x00, 0x05), PduType.PROVISIONING_PDU) }
        }
        advanceUntilIdle()
        val result = sending.await()

        assertTrue(
            "실제: ${result.exceptionOrNull()}",
            result.exceptionOrNull() is PBRemoteBearerError.OutboundPduTimeout
        )
        assertEquals(2, transport.sentUnacknowledged.size)
        val close = transport.sentAcknowledged
            .filterIsInstance<RemoteProvisioningLinkClose>()
            .lastOrNull()
        assertNotNull("실패 후 Link Close 를 보내야 한다", close)
        assertEquals(RemoteProvisioningLinkCloseReason.FAIL, close!!.reason)
        assertFalse(bearer.isOpen)
    }

    @Test
    fun `close 는 reason SUCCESS 로 링크를 닫고 재호출은 무해하다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE) }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { bearer.open() }
        runCurrent()
        opening.await()

        bearer.close()
        runCurrent()
        val close = transport.sentAcknowledged.filterIsInstance<RemoteProvisioningLinkClose>()
        assertEquals(1, close.size)
        assertEquals(RemoteProvisioningLinkCloseReason.SUCCESS, close.first().reason)
        assertFalse(bearer.isOpen)

        // 두 번째 close 는 아무것도 보내지 않는다 (finish 멱등).
        bearer.close()
        runCurrent()
        assertEquals(
            1,
            transport.sentAcknowledged.filterIsInstance<RemoteProvisioningLinkClose>().size
        )
    }

    /**
     * 감사 P2-N2. `open()` 이 LINK_ACTIVE 를 기다리는 동안 호출자가 취소되면(화면 이탈 등)
     * `finish()` 를 한 번도 안 거치므로, 정리 경로가 없으면:
     *  - `isOpening` 이 true 로 남아 **다음 `open()` 이 조용히 no-op**,
     *  - observer/supervisor 코루틴 2개가 leak,
     *  - **Link Close 를 안 보내 서버 링크가 Link Open Timeout(최대 60 s) 동안 점유**되어
     *    다른 client 의 Link Open 이 `LINK_CANNOT_OPEN` 으로 거절된다.
     */
    @Test
    fun `open 이 취소되면 Link Close 를 보내고 재-open 이 가능하다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { message ->
            when (message) {
                is RemoteProvisioningLinkOpen -> linkStatus(RemoteProvisioningLinkState.LINK_OPENING)
                is RemoteProvisioningLinkClose -> linkStatus(RemoteProvisioningLinkState.IDLE)
                else -> null
            }
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )

        val opening = async { bearer.open() }
        runCurrent()
        // 아직 LINK_ACTIVE Link Report 를 기다리는 중 — Link Open 만 나갔다.
        assertEquals(1, transport.sentAcknowledged.count { it is RemoteProvisioningLinkOpen })
        assertTrue(transport.sentAcknowledged.none { it is RemoteProvisioningLinkClose })
        assertFalse(bearer.isOpen)

        opening.cancel()
        advanceUntilIdle()

        val close = transport.sentAcknowledged.filterIsInstance<RemoteProvisioningLinkClose>()
        assertEquals("취소 후 서버 링크를 반드시 놓아줘야 한다", 1, close.size)
        assertEquals(RemoteProvisioningLinkCloseReason.FAIL, close.first().reason)
        assertFalse(bearer.isOpen)
        assertTrue(bearer.state.value is BearerEvent.Closed)

        // isOpening 이 남지 않아야 재-open 이 no-op 으로 삼켜지지 않는다.
        val reopening = async { bearer.open() }
        runCurrent()
        assertEquals(
            "재-open 이 Link Open 을 다시 보내야 한다",
            2,
            transport.sentAcknowledged.count { it is RemoteProvisioningLinkOpen }
        )
        reopening.cancel()
        advanceUntilIdle()
    }

    /**
     * 감사 P2-N1 의 표면 증상 가드: close 로 끝난 세션의 teardown 이 **다음 세션**을 건드리면
     * 재-open 직후 베어러가 곧바로 닫힌다. (근본 fix 는 `finish()` 가 `closeSignal` 을
     * `observer` 와 대칭으로 lock 안에서 캡처+null 하는 것이라 by-construction 이다.)
     */
    @Test
    fun `close 뒤 재-open 한 세션은 이전 세션 teardown 에 영향받지 않는다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE) }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )

        val first = async { bearer.open() }
        runCurrent()
        first.await()
        assertTrue(bearer.isOpen)

        bearer.close()
        runCurrent()
        assertFalse(bearer.isOpen)

        val second = async { bearer.open() }
        runCurrent()
        second.await()
        assertTrue("재-open 한 세션이 살아 있어야 한다", bearer.isOpen)

        // 새 세션의 채널도 살아 있다 (이전 세션의 close 가 넘어오지 않았다).
        transport.receive(RemoteProvisioningPDUReport(1u, byteArrayOf(0x01, 0x77)))
        runCurrent()
        val pdu = async { withTimeoutOrNull(1.seconds) { bearer.pdus.first() } }
        runCurrent()
        assertNotNull("새 세션의 pdus 채널이 닫혀 있다", pdu.await())
    }

    @Test
    fun `close 에 FAIL 을 명시하면 그대로 전송된다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { linkStatus(RemoteProvisioningLinkState.LINK_ACTIVE) }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { bearer.open() }
        runCurrent()
        opening.await()

        bearer.close(reason = RemoteProvisioningLinkCloseReason.FAIL)
        runCurrent()
        assertEquals(
            RemoteProvisioningLinkCloseReason.FAIL,
            transport.sentAcknowledged.filterIsInstance<RemoteProvisioningLinkClose>()
                .first().reason
        )
    }

    @Test
    fun `Link Open 이 거부되면 즉시 LinkCannotOpen 으로 실패한다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = {
            RemoteProvisioningLinkStatus(
                status = RemoteProvisioningMessageStatus.LINK_CANNOT_OPEN,
                linkState = RemoteProvisioningLinkState.IDLE
            )
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { runCatching { bearer.open() } }
        runCurrent()
        val result = opening.await()

        val error = result.exceptionOrNull()
        assertTrue("실제: $error", error is PBRemoteBearerError.LinkCannotOpen)
        assertEquals(
            RemoteProvisioningMessageStatus.LINK_CANNOT_OPEN,
            (error as PBRemoteBearerError.LinkCannotOpen).status
        )
        assertFalse(bearer.isOpen)
        // 열리지도 않은 링크에 Link Close 를 보내지 않는다.
        assertTrue(transport.sentAcknowledged.none { it is RemoteProvisioningLinkClose })
    }

    @Test
    fun `LINK_ACTIVE 가 끝내 오지 않으면 LinkOpenTimeout 이고 Fail 로 닫는다`() = runTest {
        val transport = FakeMeshMessageTransport(server = server)
        transport.responder = { message ->
            when (message) {
                is RemoteProvisioningLinkOpen -> linkStatus(RemoteProvisioningLinkState.LINK_OPENING)
                is RemoteProvisioningLinkClose -> linkStatus(RemoteProvisioningLinkState.IDLE)
                else -> null
            }
        }
        val bearer = PBRemoteBearer(
            transport = transport,
            server = server,
            uuid = uuid,
            ioDispatcher = StandardTestDispatcher(testScheduler),
        )
        val opening = async { runCatching { bearer.open() } }
        advanceUntilIdle()
        val result = opening.await()

        assertTrue(
            "실제: ${result.exceptionOrNull()}",
            result.exceptionOrNull() is PBRemoteBearerError.LinkOpenTimeout
        )
        val close = transport.sentAcknowledged.filterIsInstance<RemoteProvisioningLinkClose>()
        assertEquals(1, close.size)
        assertEquals(RemoteProvisioningLinkCloseReason.FAIL, close.first().reason)
        assertFalse(bearer.isOpen)
    }
}
