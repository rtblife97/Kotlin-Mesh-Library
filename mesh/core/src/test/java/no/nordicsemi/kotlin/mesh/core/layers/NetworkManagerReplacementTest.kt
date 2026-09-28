package no.nordicsemi.kotlin.mesh.core.layers

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.MeshBearer
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.NetworkEvent
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPduType
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkPduDecoder
import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotSame
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * 네트워크를 다시 불러와도(load/import 가 NetworkManager 를 새로 만든다) 수신 스택은 하나다.
 * (simdo-fork, 2026-09-28 — 실기기에서 옛 스택이 살아남아 수신 PDU 를 두 번 처리하던 결함)
 *
 * 잠그는 불변 조건:
 *  1. 몇 번을 다시 불러오든, bearer 재할당이 몇 번·몇 스레드에서 오든 bearer 구독자는 1개다.
 *  2. 다시 불러온 뒤 수신 PDU 하나는 한 스택에서 한 번만 처리되고, 앱이 bearer 를 다시 물려 주지
 *     않아도 새 스택이 받는다(수신 공백 없음).
 *  3. 수신 메시지 이벤트(networkEvents)는 현재 스택에서 나온다 — 한 번씩.
 *  4. 옛 스택으로 나간 요청이 응답을 기다리는 동안에는 옛 스택 수신을 닫지 않는다(응답 유실 금지).
 */
@OptIn(ExperimentalUuidApi::class)
class NetworkManagerReplacementTest {

    private class StubBearer : MeshBearer {
        val pduFlow = MutableSharedFlow<Pdu>(extraBufferCapacity = 64)
        override val pdus: Flow<Pdu> = pduFlow
        override val state: StateFlow<BearerEvent>
            get() = throw UnsupportedOperationException()
        override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.NetworkPdu)
        override val isOpen: Boolean = true
        override suspend fun open() = Unit
        override suspend fun close() = Unit
        override suspend fun send(pdu: ByteArray, type: PduType) = Unit
    }

    private class StubStorage : Storage {
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = Unit
    }

    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) = Unit
        private var seq: UInt = 0u
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = seq++
        override suspend fun storeNextSequenceNumber(uuid: Uuid, address: UnicastAddress, sequenceNumber: UInt) = Unit
        override suspend fun resetSequenceNumber(uuid: Uuid, address: UnicastAddress) = Unit
        override suspend fun lastSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(uuid: Uuid, source: UnicastAddress, lastSeqAuth: ULong) = Unit
        override suspend fun previousSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(uuid: Uuid, source: UnicastAddress, seqAuth: ULong) = Unit
        override suspend fun storeLocalProvisioner(uuid: Uuid, localProvisionerUuid: Uuid) = Unit
        override suspend fun localProvisioner(uuid: Uuid): String? = null
    }

    /** Heartbeat control message — app key 없이 복호화되는 경로(NetworkLayerMessageCacheTest 와 동일). */
    private class HeartbeatControlPdu(
        override val networkKey: NetworkKey,
        override val ivIndex: UInt,
        override val source: MeshAddress,
        override val destination: MeshAddress,
    ) : LowerTransportPdu {
        override val type = LowerTransportPduType.CONTROL_MESSAGE
        override val upperTransportPdu: ByteArray = byteArrayOf(0x05, 0x00, 0x00)
        override val transportPdu: ByteArray = byteArrayOf(0x0A) + upperTransportPdu
    }

    private val cdb: ByteArray =
        javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()

    /** 앱 순서 재현: bearer 는 Hilt 생성 시점(네트워크 로드 전)에 한 번만 물린다. */
    private fun managerWithBearer(bearer: MeshBearer) = MeshNetworkManager(
        storage = StubStorage(),
        secureProperties = StubSecureProperties(),
        ioDispatcher = Dispatchers.Default,
    ).apply { meshBearer = bearer }

    private fun wirePdu(network: MeshNetwork, sequence: UInt): Pdu {
        val pdu = HeartbeatControlPdu(
            networkKey = network.networkKeys.first(),
            ivIndex = network.ivIndex.transmitIvIndex,
            source = MeshAddress.create(0x0002),
            destination = MeshAddress.create(0xC000),
        )
        val bytes = NetworkPduDecoder.encode(
            lowerTransportPdu = pdu,
            pduType = PduType.NETWORK_PDU,
            sequence = sequence,
            ttl = 5u,
        ).pdu
        return Pdu(data = bytes, type = PduType.NETWORK_PDU)
    }

    /**
     * 구독자 수가 [expected] 로 수렴하는지. 취소된 collector 의 구독 해제는 그 코루틴이 dispatcher 에서
     * 취소를 마칠 때 반영되므로 즉시 읽지 않고 기다렸다가, 잠시 뒤에도 그대로인지 다시 본다.
     */
    private suspend fun StubBearer.settledSubscribers(expected: Int): Int {
        withTimeoutOrNull(2_000) { while (pduFlow.subscriptionCount.value != expected) delay(10) }
        delay(100)
        return pduFlow.subscriptionCount.value
    }

    private suspend fun awaitCount(expected: Int, read: () -> Int): Int {
        withTimeoutOrNull(2_000) { while (read() < expected) delay(10) }
        delay(100) // 추가 처리(이중 처리)가 들어올 여유
        return read()
    }

    @Test
    fun `reload keeps exactly one bearer subscriber and the new stack receives without a rewire`() =
        runBlocking {
            val bearer = StubBearer()
            val mgr = managerWithBearer(bearer)

            mgr.import(cdb)
            val first = mgr.networkManager!!
            mgr.import(cdb) // 앱: activate load → attachNetwork import (2026-09-28 실기기 순서)
            val second = mgr.networkManager!!
            assertNotSame(first, second)
            assertEquals("bearer 구독자는 현재 스택 하나", 1, bearer.settledSubscribers(1))
            assertTrue("옛 스택 수신 종료", first.isRxClosed)

            // 앱이 meshBearer 를 다시 넣지 않았는데도 새 스택이 받는다.
            bearer.pduFlow.emit(wirePdu(mgr.network!!, sequence = 0x10u))
            val handledByNew = awaitCount(1) { second.networkLayer.networkMessageCache.rawPduCount }
            assertEquals("새 스택이 1번 처리", 1, handledByNew)
            assertEquals("옛 스택은 처리 안 함", 0, first.networkLayer.networkMessageCache.rawPduCount)
        }

    @Test
    fun `concurrent same-bearer rewires never leave an extra collector`() = runBlocking {
        val bearer = StubBearer()
        val mgr = managerWithBearer(bearer)
        mgr.import(cdb)

        // activate() 의 재할당과 NetworkUpdated 핸들러의 재할당이 같은 ms 에 다른 스레드에서 온다.
        val pool = Executors.newFixedThreadPool(8)
        try {
            repeat(50) {
                val start = CountDownLatch(1)
                val done = CountDownLatch(8)
                repeat(8) {
                    pool.execute {
                        start.await()
                        mgr.meshBearer = bearer
                        done.countDown()
                    }
                }
                start.countDown()
                done.await()
                assertEquals("재할당 경합 뒤에도 구독자 1", 1, bearer.settledSubscribers(1))
            }
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `received messages reach networkEvents once, from the current stack`() = runBlocking {
        val bearer = StubBearer()
        val mgr = managerWithBearer(bearer)
        mgr.import(cdb)
        val first = mgr.networkManager!!
        mgr.import(cdb)
        val current = mgr.networkManager!!

        val events = mutableListOf<NetworkEvent.MeshMessageReceived>()
        val collector = launch {
            mgr.networkEvents.filterIsInstance<NetworkEvent.MeshMessageReceived>()
                .collect { events += it }
        }
        delay(50)
        val message = ReceivedMessage(
            source = MeshAddress.create(0x0002),
            destination = MeshAddress.create(0x0001),
            message = FakeMessage(0x8204u),
            sequence = 7u, ivIndex = 0u, ttl = 5u,
        )
        current.emitIncomingMeshMessageForTest(message)
        first.emitIncomingMeshMessageForTest(message.copy(sequence = 8u)) // 은퇴한 스택: 전달 안 됨
        withTimeoutOrNull(1_000) { while (events.isEmpty()) delay(10) }
        delay(100)
        collector.cancel()
        assertEquals(listOf(7u), events.map { it.sequence })
    }

    @Test
    fun `old stack keeps RX while a request it sent is still waiting for its response`() =
        runBlocking {
            val bearer = StubBearer()
            val mgr = managerWithBearer(bearer)
            mgr.import(cdb)
            val first = mgr.networkManager!!

            first.enterInFlight() // 옛 스택으로 나간 요청이 응답 대기 중
            mgr.import(cdb)
            val second = mgr.networkManager!!
            assertEquals("drain 중: 옛+새 구독", 2, bearer.settledSubscribers(2))

            // 대기 중 응답이 오면 옛 스택도 받는다(종전과 같음 — 요청이 타임아웃나지 않는다).
            bearer.pduFlow.emit(wirePdu(mgr.network!!, sequence = 0x20u))
            assertEquals(1, awaitCount(1) { first.networkLayer.networkMessageCache.rawPduCount })

            first.exitInFlight() // 응답 수신 → 요청 종료
            withTimeout(2_000) { while (!first.isRxClosed) delay(10) }
            assertEquals("요청이 끝나면 옛 스택 수신 종료", 1, bearer.settledSubscribers(1))
            assertSame(second, mgr.networkManager)
        }

    @Test
    fun `sending through a retired stack reopens its RX until that send completes`() = runBlocking {
        val bearer = StubBearer()
        val mgr = managerWithBearer(bearer)
        mgr.import(cdb)
        val first = mgr.networkManager!!
        mgr.import(cdb)
        assertTrue(first.isRxClosed)

        // 교체 직전에 스택을 잡아 둔 호출자가 송신 → 응답을 받을 수 있어야 한다.
        assertEquals(1, bearer.settledSubscribers(1))
        first.enterInFlight()
        assertEquals(2, bearer.settledSubscribers(2))
        first.exitInFlight()
        assertEquals(1, bearer.settledSubscribers(1))
        assertTrue(first.isRxClosed)
    }

    private class FakeMessage(override val opCode: UInt) : MeshMessage {
        override val parameters: ByteArray? = null
    }
}
