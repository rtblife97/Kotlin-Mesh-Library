package no.nordicsemi.kotlin.mesh.core.layers.network

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.ExperimentalTime
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-21) — IV 상태 전이 시각(transitionDate)은 실제 전이 때만 바뀐다.
 *
 * Mesh Profile 1.0.1 §3.10.5: 각 IV 상태(IV, IV Update 플래그)는 최소 96 h 유지된다. 이 lib 은 "현재 상태에 들어선 시각"
 * 으로 그 규칙을 판정한다([NetworkBeaconPdu.canOverWrite]). 종전에는 비콘 복호기가 IvIndex 를 지금 시각으로 만들고
 * 같은 상태 비콘에도 그 시각을 저장해, 전이 시각이 "마지막 비콘 시각" 이 됐다. 그러면 망이 X → X+1 로 넘어갈 때
 * 폰·게이트웨이가 96 h 규칙에 걸려 비콘을 버리고, 노드가 정상 X+1 로 가면 약 96 h 동안 복호를 못 한다.
 * iOS 원본(NetworkLayer.swift `handle(networkBeacon:)`)은 IV 와 전이 시각을 따로 두고, 상태가 바뀔 때만 시각을 쓴다.
 *
 * 실제 NetKey 로 인증한 Secure Network beacon 을 [NetworkLayer.handle] 에 넣어 lib 경로 그대로 판정한다.
 * 시간 경과는 저장된 전이 시각을 과거로 옮겨 흉내 낸다(lib 시계는 주입할 수 없다).
 */
@OptIn(ExperimentalUuidApi::class, ExperimentalTime::class)
class IvTransitionDateTest {

    private class StubStorage : Storage {
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = Unit
    }

    /** IV 한 칸만 기억하는 저장소. 저장값이 없으면 앱 저장소(MeshSecurePropertiesStorage)처럼 먼 과거를 준다. */
    private class IvRecordingStorage(var stored: IvIndex?) : SecurePropertiesStorage {
        var sequenceResets = 0
        override suspend fun ivIndex(uuid: Uuid): IvIndex =
            stored ?: IvIndex(transitionDate = Instant.DISTANT_PAST)
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) { stored = ivIndex }
        private var seq: UInt = 0u
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = seq++
        override suspend fun storeNextSequenceNumber(uuid: Uuid, address: UnicastAddress, sequenceNumber: UInt) = Unit
        override suspend fun resetSequenceNumber(uuid: Uuid, address: UnicastAddress) { sequenceResets++ }
        override suspend fun lastSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(uuid: Uuid, source: UnicastAddress, lastSeqAuth: ULong) = Unit
        override suspend fun previousSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(uuid: Uuid, source: UnicastAddress, seqAuth: ULong) = Unit
        override suspend fun storeLocalProvisioner(uuid: Uuid, localProvisionerUuid: Uuid) = Unit
        override suspend fun localProvisioner(uuid: Uuid): String? = null

        /** 저장된 전이 시각을 [by] 만큼 과거로 — 그만큼 시간이 흐른 것과 같다. */
        fun age(by: Duration) {
            val s = stored!!
            stored = s.copy(transitionDate = s.transitionDate - by)
        }
    }

    private class Fixture(val manager: MeshNetworkManager, val props: IvRecordingStorage) {
        val network: MeshNetwork get() = manager.network!!
        val layer get() = manager.networkManager!!.networkLayer
        val stored: IvIndex get() = props.stored!!

        fun receive(index: UInt, updateActive: Boolean) = runBlocking {
            val pdu = secureNetworkBeacon(network, index, updateActive)
            layer.handle(incomingPdu = pdu, type = PduType.MESH_BEACON)
        }
    }

    private fun fixture(stored: IvIndex?): Fixture {
        val props = IvRecordingStorage(stored)
        val mgr = MeshNetworkManager(
            storage = StubStorage(),
            secureProperties = props,
            ioDispatcher = Dispatchers.Default,
        )
        val json = javaClass.classLoader.getResourceAsStream("cdb_json.json")!!.readAllBytes()
        runBlocking { mgr.import(json) }
        // 전제: 로컬 프로비저너에 노드가 있어야 SEQ 재설정 경로가 돈다.
        assertNotNull(mgr.network!!.localProvisioner?.node)
        return Fixture(mgr, props)
    }

    private fun assertState(f: Fixture, index: UInt, updateActive: Boolean) {
        assertEquals("stored index", index, f.stored.index)
        assertEquals("stored IV Update flag", updateActive, f.stored.isIvUpdateActive)
        assertEquals("in-memory index", index, f.network.ivIndex.index)
        assertEquals("in-memory IV Update flag", updateActive, f.network.ivIndex.isIvUpdateActive)
    }

    private fun assertRecent(at: Instant, before: Instant, after: Instant) =
        assertTrue("transition date $at must be in [$before, $after]", at in before..after)

    @Test
    fun `같은 IV 비콘을 거듭 받아도 전이 시각은 그대로다`() {
        val entered = Clock.System.now() - 1000.hours
        val f = fixture(IvIndex(5u, false, entered))

        repeat(3) { f.receive(5u, false) }

        assertState(f, 5u, false)
        assertEquals("stored transition date", entered, f.stored.transitionDate)
        assertEquals("in-memory transition date", entered, f.network.ivIndex.transitionDate)
        assertEquals("same state must not reset SEQ", 0, f.props.sequenceResets)
    }

    @Test
    fun `전이 뒤 96 h 가 지나면 +1 진행 중을, 다시 96 h 뒤 정상을 받는다`() {
        // 현장 상황: 97 h 전에 IV 5 정상에 들어섰고, 그 뒤로도 같은 비콘을 계속 받았다.
        val f = fixture(IvIndex(5u, false, Clock.System.now() - 97.hours))
        repeat(2) { f.receive(5u, false) }

        // 망이 IV Update 시작 → (6, 진행 중).
        val t1 = Clock.System.now()
        f.receive(6u, true)
        val t2 = Clock.System.now()
        assertState(f, 6u, true)
        val enteredInProgress = f.stored.transitionDate
        assertRecent(enteredInProgress, t1, t2)
        assertEquals("in-progress keeps transmitting with IV 5: no SEQ reset", 0, f.props.sequenceResets)

        // 진행 중 비콘 반복 → 전이 시각 그대로.
        repeat(2) { f.receive(6u, true) }
        assertEquals(enteredInProgress, f.stored.transitionDate)

        // 96 h 경과 → (6, 정상).
        f.props.age(by = 97.hours)
        val t3 = Clock.System.now()
        f.receive(6u, false)
        val t4 = Clock.System.now()
        assertState(f, 6u, false)
        assertRecent(f.stored.transitionDate, t3, t4)
        assertEquals("transmit IV 5 → 6 resets SEQ once", 1, f.props.sequenceResets)

        // 정상 비콘 반복 → 시각·SEQ 그대로.
        val enteredNormal = f.stored.transitionDate
        repeat(2) { f.receive(6u, false) }
        assertEquals(enteredNormal, f.stored.transitionDate)
        assertEquals(1, f.props.sequenceResets)
    }

    @Test
    fun `직전 전이 96 h 안의 +1 은 거부한다`() {
        val entered = Clock.System.now() - 95.hours
        val f = fixture(IvIndex(5u, false, entered))
        f.receive(5u, false)

        f.receive(6u, true)
        assertState(f, 5u, false)
        assertEquals("rejected beacon must not touch the date", entered, f.stored.transitionDate)

        // 진행 중을 건너뛴 (6, 정상) 은 두 상태 = 192 h 가 필요하다 — 100 h 에서는 거부.
        f.props.age(by = 5.hours)
        f.receive(6u, false)
        assertState(f, 5u, false)

        // 96 h 를 넘긴 뒤의 (6, 진행 중) 은 받는다.
        f.receive(6u, true)
        assertState(f, 6u, true)
        assertEquals(0, f.props.sequenceResets)
    }

    @Test
    fun `+2 이상 IV Recovery 는 종전대로 받는다`() {
        // 1 시간 전에 IV 5 에 들어섰어도 +2 는 복구로 받는다 (2026-09-30 포크 수정).
        val f = fixture(IvIndex(5u, false, Clock.System.now() - 1.hours))
        val t1 = Clock.System.now()
        f.receive(7u, false)
        val t2 = Clock.System.now()
        assertState(f, 7u, false)
        assertRecent(f.stored.transitionDate, t1, t2)
        assertTrue("recovery flag", f.network.ivIndex.ivRecoveryFlag)
        assertEquals("transmit IV 5 → 7 resets SEQ", 1, f.props.sequenceResets)

        // 복구 뒤 같은 비콘 반복 → 전이 시각 그대로.
        val recoveredAt = f.stored.transitionDate
        f.receive(7u, false)
        assertEquals(recoveredAt, f.stored.transitionDate)

        // 뒤로 가는 IV, +43 이상은 거부.
        f.receive(6u, false)
        assertState(f, 7u, false)
        f.receive(7u + 43u, false)
        assertState(f, 7u, false)
        assertEquals(1, f.props.sequenceResets)
    }

    @Test
    fun `첫 실행 - 저장값이 없으면 같은 IV 비콘 뒤에도 +1 을 바로 받는다`() {
        val f = fixture(stored = null)
        repeat(2) { f.receive(0u, false) }
        assertState(f, 0u, false)
        assertEquals("unknown transition stays unknown", Instant.DISTANT_PAST, f.stored.transitionDate)

        f.receive(1u, true)
        assertState(f, 1u, true)
    }

    @Test
    fun `첫 실행 - 운영 중인 망의 IV 는 복구로 받고 그때를 전이 시각으로 둔다`() {
        val f = fixture(stored = null)
        val t1 = Clock.System.now()
        f.receive(5u, false)
        val t2 = Clock.System.now()
        assertState(f, 5u, false)
        assertRecent(f.stored.transitionDate, t1, t2)
    }

    private companion object {
        /** Secure Network beacon (Mesh Profile 1.0.1 §3.9.3): 0x01 | Flags | Network ID(8) | IV Index(4) | Auth(8). */
        fun secureNetworkBeacon(network: MeshNetwork, index: UInt, updateActive: Boolean): ByteArray {
            val key = network.primaryNetworkKey!!
            val flags = if (updateActive) 0x02 else 0x00
            val body = byteArrayOf(flags.toByte()) + key.networkId + byteArrayOf(
                (index shr 24).toByte(), (index shr 16).toByte(), (index shr 8).toByte(), index.toByte(),
            )
            val auth = aesCmac(key.derivatives.beaconKey, body).copyOfRange(0, 8)
            val pdu = byteArrayOf(0x01) + body + auth
            // 자가 점검: lib 복호기가 이 비콘을 인증하고 같은 값을 읽는다.
            val decoded = SecureNetworkBeaconDecoder.decode(pdu, key)
            assertNotNull("beacon must authenticate", decoded)
            assertEquals(index, decoded!!.ivIndex.index)
            assertEquals(updateActive, decoded.ivIndex.isIvUpdateActive)
            return pdu
        }

        /** AES-CMAC (RFC 4493). lib 의 CMAC 은 crypto 모듈 private 이라 시험에서 따로 계산한다. */
        fun aesCmac(key: ByteArray, msg: ByteArray): ByteArray {
            val aes = Cipher.getInstance("AES/ECB/NoPadding")
                .apply { init(Cipher.ENCRYPT_MODE, SecretKeySpec(key, "AES")) }
            fun dbl(b: ByteArray): ByteArray {
                val out = ByteArray(16)
                var carry = 0
                for (i in 15 downTo 0) {
                    val v = b[i].toInt() and 0xFF
                    out[i] = ((v shl 1) or carry).toByte()
                    carry = v ushr 7
                }
                if (b[0].toInt() and 0x80 != 0) out[15] = (out[15].toInt() xor 0x87).toByte()
                return out
            }
            val k1 = dbl(aes.doFinal(ByteArray(16)))
            val k2 = dbl(k1)
            val n = maxOf(1, (msg.size + 15) / 16)
            val complete = msg.isNotEmpty() && msg.size % 16 == 0
            val start = (n - 1) * 16
            val last = ByteArray(16) { i ->
                val m = when {
                    start + i < msg.size -> msg[start + i].toInt()
                    start + i == msg.size -> 0x80
                    else -> 0
                }
                (m xor (if (complete) k1[i] else k2[i]).toInt()).toByte()
            }
            var x = ByteArray(16)
            for (blk in 0 until n - 1) {
                x = aes.doFinal(ByteArray(16) { i -> (x[i].toInt() xor msg[blk * 16 + i].toInt()).toByte() })
            }
            return aes.doFinal(ByteArray(16) { i -> (x[i].toInt() xor last[i].toInt()).toByte() })
        }
    }
}
