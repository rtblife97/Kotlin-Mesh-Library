package no.nordicsemi.kotlin.mesh.core.layers

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.MeshBearer
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.PersistenceDiagnostics
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.access.AccessPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.AccessMessage
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkPduDecoder
import no.nordicsemi.kotlin.mesh.core.layers.uppertransport.UpperTransportPdu
import no.nordicsemi.kotlin.mesh.core.messages.ConfigMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppBind
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppStatus
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.SigModelId
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-15) — 수신 경로에서 영속화 분리 (합침 저장) 의 불변.
 *
 *  1. 망 상태를 바꾸는 수신 메시지 N 개 → 저장 1회 (조용한 시간 뒤). 계속 바뀌어도 상한 안에는 저장한다.
 *  2. 명시 저장([MeshNetworkManager.save])은 미뤄 둔 저장을 지금 쓰고 끝까지 기다린다 (flush 지점).
 *  3. 수신 경로는 저장을 기다리지 않는다 — 실제 NetKey·DevKey 로 암호화한 Status 를 수신 경로(링크 pdus → NetworkLayer → LowerTransport →
 *     AccessLayer)로 흘리고, 저장소가 느려도(1.5 s) 연속 요청의 왕복이 저장 시간에 묶이지 않는다. 종전 코드에서는 두 번째 응답부터 앞 저장이
 *     끝날 때까지 하위 전송 잠금 안에서 줄을 섰다 (2026-10-15 진단).
 *  4. 해제([MeshNetworkManager.clear]) 뒤에 늦게 끝난 옛 저장이 파일을 되살리지 않는다.
 *
 * RPR 수신이 저장을 유발하지 않는 판정은 [no.nordicsemi.kotlin.mesh.core.layers.access.AccessLayerNetworkStateEmitTest] 가 잠근다 (판정 함수
 * 그대로 — 이 변경은 판정 뒤의 "저장 방식" 만 바꾼다).
 */
@OptIn(ExperimentalUuidApi::class)
class CoalescedNetworkSaveTest {

    private val provisionerAddress = 0x0001
    private val nodeX = 0x0007

    // ── 1. 합침 ───────────────────────────────────────────────────────────────

    @Test
    fun `N dirty marks within the quiet window - one save`() = runBlocking {
        val storage = RecordingStorage()
        val mgr = manager(storage).apply { saveDebounce = 200.milliseconds; saveMaxDelay = 5.seconds }
        val before = storage.saves.get()
        repeat(20) { mgr.networkManager!!.emitNetworkManagerEvent(NetworkManagerEvent.OnNetworkChanged); delay(5) }
        assertTrue("표시만 했으니 아직 저장 전", mgr.hasUnsavedChanges)
        assertEquals("조용한 시간 전에는 쓰지 않는다", before, storage.saves.get())
        waitUntil(2_000) { !mgr.hasUnsavedChanges && storage.saves.get() > before }
        delay(400)
        assertEquals("20 번 표시 → 저장 1회", before + 1, storage.saves.get())
    }

    @Test
    fun `continuous changes - saved within the max delay`() = runBlocking {
        val storage = RecordingStorage()
        val mgr = manager(storage).apply { saveDebounce = 300.milliseconds; saveMaxDelay = 500.milliseconds }
        val before = storage.saves.get()
        val start = System.nanoTime()
        var firstSaveAtMs = -1L
        // 100 ms 마다 계속 바뀐다 — 조용한 시간이 오지 않는다.
        while ((System.nanoTime() - start) / 1_000_000 < 1_200) {
            mgr.networkManager!!.emitNetworkManagerEvent(NetworkManagerEvent.OnNetworkChanged)
            if (firstSaveAtMs < 0 && storage.saves.get() > before) firstSaveAtMs = (System.nanoTime() - start) / 1_000_000
            delay(100)
        }
        assertTrue("상한(500 ms) 근처에서 저장해야 한다: 첫 저장 $firstSaveAtMs ms", firstSaveAtMs in 400..900)
    }

    // ── 2. flush 지점 ─────────────────────────────────────────────────────────

    @Test
    fun `explicit save flushes now and the deferred save does not repeat it`() = runBlocking {
        val storage = RecordingStorage()
        val mgr = manager(storage).apply { saveDebounce = 300.milliseconds; saveMaxDelay = 5.seconds }
        val before = storage.saves.get()
        mgr.networkManager!!.emitNetworkManagerEvent(NetworkManagerEvent.OnNetworkChanged)
        mgr.save()
        assertEquals("명시 저장은 반환 전에 쓴다", before + 1, storage.saves.get())
        assertFalse(mgr.hasUnsavedChanges)
        delay(600)
        assertEquals("미뤄 둔 저장은 할 일이 없다", before + 1, storage.saves.get())
    }

    @Test
    fun `flushPendingSave writes only when something is pending`() = runBlocking {
        val storage = RecordingStorage()
        val mgr = manager(storage).apply { saveDebounce = 10.seconds; saveMaxDelay = 10.seconds }
        val before = storage.saves.get()
        assertFalse("바뀐 것이 없으면 쓰지 않는다", mgr.flushPendingSave())
        assertEquals(before, storage.saves.get())
        mgr.networkManager!!.emitNetworkManagerEvent(NetworkManagerEvent.OnNetworkChanged)
        assertTrue(mgr.flushPendingSave())
        assertEquals(before + 1, storage.saves.get())
        assertFalse(mgr.hasUnsavedChanges)
    }

    @Test
    fun `failed deferred save keeps the change pending`() = runBlocking {
        val storage = RecordingStorage(failNext = 1)
        val mgr = manager(storage).apply { saveDebounce = 100.milliseconds; saveMaxDelay = 5.seconds }
        val before = storage.saves.get()
        mgr.networkManager!!.emitNetworkManagerEvent(NetworkManagerEvent.OnNetworkChanged)
        waitUntil(3_000) { storage.saves.get() > before }
        assertEquals("실패 뒤 다시 시도해 결국 쓴다", before + 1, storage.saves.get())
        assertFalse(mgr.hasUnsavedChanges)
    }

    // ── 3. 수신 경로가 저장을 기다리지 않음 (실제 암호화 PDU) ─────────────────────────

    @Test
    fun `slow storage does not stretch config round trips`() = runBlocking {
        val storage = RecordingStorage(saveDelayMs = 1_500)
        val mgr = manager(storage).apply { saveDebounce = 2.seconds; saveMaxDelay = 10.seconds }
        val network = mgr.network!!
        assertTrue(mgr.rehydrateLocalNodeHandlers())
        mgr.meshBearer = LinkFake()
        val onOff = SigModelId(0x1000u)
        val x = network.node(nodeX.toUShort())!!
        val savesBefore = storage.saves.get()
        val mark = PersistenceDiagnostics.mark()

        val rtts = (0 until 3).map { i ->
            val status = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, 0u, UnicastAddress(nodeX.toUShort()), onOff)
            val wire = response(network, nodeX, status, sequence = (0x100 + i).toUInt())
            lateinit var link: LinkFake
            link = LinkFake { link.inject(wire) }
            mgr.registerBearer(nodeX.toUShort(), link)
            val t0 = System.nanoTime()
            val r = withTimeoutOrNull(5_000) {
                mgr.send(ConfigModelAppBind(0u, UnicastAddress(nodeX.toUShort()), onOff), x, initialTtl = null)
            }
            val rtt = (System.nanoTime() - t0) / 1_000_000
            mgr.unregisterBearer(nodeX.toUShort())
            link.close()
            assertTrue("응답 $i 를 받아야 한다: $r", r is ConfigModelAppStatus)
            rtt
        }

        // 종전: 첫 Status 가 1.5 s 저장을 시작하고, 두 번째·세 번째 Status 는 하위 전송 잠금 안에서 그 저장을 기다렸다 (≥ 1.5 s).
        assertTrue("연속 왕복이 저장 시간에 묶이면 안 된다: $rtts ms", rtts.all { it < 700 })
        assertEquals("주고받는 동안에는 쓰지 않는다 (합침 대기)", savesBefore, storage.saves.get())
        assertTrue("상태가 바뀌었으니 저장 대기 중", mgr.hasUnsavedChanges)

        val window = PersistenceDiagnostics.since(mark) { it == nodeX }
        assertEquals("수신 대기 표본 = 응답 3건", 3, window.rxWaitMs.size)
        assertTrue("수신 경로 대기가 저장 시간을 포함하면 안 된다: ${window.rxWaitMs}", window.rxWaitMs.all { it < 500.0 })

        // flush 지점(조명 설정 완료 = 명시 save) 에서는 기다려서 쓴다.
        mgr.save()
        assertEquals(savesBefore + 1, storage.saves.get())
        assertEquals(1L, PersistenceDiagnostics.since(mark).saves)
    }

    // ── 4. 해제 뒤 되살아나지 않음 ─────────────────────────────────────────────────

    @Test
    fun `clear wins over an in-flight deferred save`() = runBlocking {
        val storage = RecordingStorage(saveDelayMs = 400)
        val mgr = manager(storage).apply { saveDebounce = 10.milliseconds; saveMaxDelay = 10.milliseconds }
        mgr.networkManager!!.emitNetworkManagerEvent(NetworkManagerEvent.OnNetworkChanged)
        waitUntil(2_000) { storage.inFlight.get() > 0 }
        assertTrue("옛 저장이 쓰는 중이어야 한다", storage.inFlight.get() > 0)
        mgr.clear()
        delay(600)
        assertEquals("마지막으로 쓴 것은 빈 파일", 0, storage.last.get()!!.size)
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    private class RecordingStorage(
        private val saveDelayMs: Long = 0,
        failNext: Int = 0,
    ) : Storage {
        val saves = AtomicLong(0)
        val inFlight = AtomicInteger(0)
        val last = AtomicReference<ByteArray?>(null)
        private val failures = AtomicInteger(failNext)
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) {
            inFlight.incrementAndGet()
            try {
                if (saveDelayMs > 0) delay(saveDelayMs)
                if (failures.getAndDecrement() > 0) throw java.io.IOException("디스크 실패 흉내")
                last.set(network)
                if (network.isNotEmpty()) saves.incrementAndGet()
            } finally {
                inFlight.decrementAndGet()
            }
        }
    }

    /** 첫 송신 때 [reply] 를 다른 코루틴에서 돌리는 링크 (lib 수신 경로로 응답을 흘린다). */
    private class LinkFake(private val reply: (suspend () -> Unit)? = null) : MeshBearer {
        private val flow = MutableSharedFlow<Pdu>(extraBufferCapacity = 64)
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        private val sent = AtomicInteger()
        override val pdus: Flow<Pdu> = flow
        override val state: StateFlow<BearerEvent> get() = throw UnsupportedOperationException()
        override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.NetworkPdu, PduTypes.MeshBeacon, PduTypes.ProxyConfiguration)
        override val isOpen: Boolean = true
        override suspend fun open() = Unit
        override suspend fun close() = scope.cancel()
        override suspend fun send(pdu: ByteArray, type: PduType) {
            if (sent.getAndIncrement() == 0 && reply != null) scope.launch { reply.invoke() }
        }

        suspend fun inject(bytes: ByteArray) = flow.emit(Pdu(bytes, PduType.NETWORK_PDU))
    }

    /** 노드 → 프로비저너, 노드 DevKey·NetKey 로 암호화한 비분할 Access 메시지의 wire 바이트. */
    private fun response(network: MeshNetwork, from: Int, status: ConfigModelAppStatus, sequence: UInt): ByteArray {
        val node = network.node(from.toUShort())!!
        val netKey = network.networkKeys.first { it.index.toInt() == 0 }
        val access = AccessPdu.init(
            message = status,
            source = from.toUShort(),
            destination = MeshAddress.create(provisionerAddress),
            userInitiated = false,
        )
        val keySet = DeviceKeySet.init(netKey, node)!!
        val upper = UpperTransportPdu.init(access, keySet, sequence, network.ivIndex)
        return NetworkPduDecoder.encode(
            lowerTransportPdu = AccessMessage(upper, netKey),
            pduType = PduType.NETWORK_PDU,
            sequence = sequence,
            ttl = 5u,
        ).pdu
    }

    private suspend fun waitUntil(ms: Long, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + ms
        while (!condition() && System.currentTimeMillis() < deadline) delay(10)
    }

    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) = Unit
        private val seq = AtomicInteger(0)
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = seq.getAndIncrement().toUInt()
        override suspend fun storeNextSequenceNumber(uuid: Uuid, address: UnicastAddress, sequenceNumber: UInt) = Unit
        override suspend fun resetSequenceNumber(uuid: Uuid, address: UnicastAddress) = Unit
        override suspend fun lastSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(uuid: Uuid, source: UnicastAddress, lastSeqAuth: ULong) = Unit
        override suspend fun previousSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(uuid: Uuid, source: UnicastAddress, seqAuth: ULong) = Unit
        override suspend fun storeLocalProvisioner(uuid: Uuid, localProvisionerUuid: Uuid) = Unit
        override suspend fun localProvisioner(uuid: Uuid): String? = null
    }

    private val cdb: ByteArray = javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()

    private fun manager(storage: Storage): MeshNetworkManager = MeshNetworkManager(
        storage = storage,
        secureProperties = StubSecureProperties(),
        ioDispatcher = Dispatchers.Default,
    ).also { runBlocking { it.import(cdb) } }
}
