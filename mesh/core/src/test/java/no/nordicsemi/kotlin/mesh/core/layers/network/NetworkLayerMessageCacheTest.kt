package no.nordicsemi.kotlin.mesh.core.layers.network

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPduType
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Test
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * [NetworkLayer.handle] 을 실제 암호화된 Network PDU 로 구동해 Network Message Cache 2단계를 가드한다
 * (simdo-patch, 2026-09-09).
 *
 * - stage 1(raw bytes): 같은 wire 바이트의 **새 인스턴스** 2회 → 두 번째 drop.
 * - stage 2(SRC+IV+SEQ): relay 사본(TTL-1 → nonce 가 달라 wire 바이트 전부 다름)은 stage 1 을
 *   통과하지만 stage 2 에서 drop. 종전 코드에는 이 단계 자체가 없었다(group 목적지는
 *   LowerTransportLayer 의 replay 검사도 면제라 앱까지 중복 전달).
 * - 다른 SEQ 는 통과.
 *
 * 판정은 캐시 카운터로 한다. Control 메시지 경로는 항상 null 을 반환하므로 `handle()` 의 반환값은
 * 판별력이 없고, 대신 [NetworkPduDecoder.decode] 로 두 PDU 모두 정상 복호화됨을 독립 검증해
 * "decode 실패" 와 "캐시 drop" 을 분리한다.
 */
@OptIn(ExperimentalUuidApi::class)
class NetworkLayerMessageCacheTest {

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

    /** Heartbeat control message (opcode 0x0A, InitTTL + Features) — app key 없이 복호화 가능한 경로. */
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

    private fun freshManager(): MeshNetworkManager {
        val mgr = MeshNetworkManager(
            storage = StubStorage(),
            secureProperties = StubSecureProperties(),
            ioDispatcher = Dispatchers.Default,
        )
        val jsonBytes =
            this.javaClass.classLoader.getResourceAsStream("cdb_json.json")!!.readAllBytes()
        runBlocking { mgr.import(jsonBytes) }
        return mgr
    }

    /** 노드 0x0002 → group 0xC000 Heartbeat 를 주어진 SEQ/TTL 로 암호화한 wire 바이트. */
    private fun wirePdu(network: MeshNetwork, sequence: UInt, ttl: UByte): ByteArray {
        val pdu = HeartbeatControlPdu(
            networkKey = network.networkKeys.first(),
            ivIndex = network.ivIndex.transmitIvIndex,
            source = MeshAddress.create(0x0002),
            destination = MeshAddress.create(0xC000),
        )
        return NetworkPduDecoder.encode(
            lowerTransportPdu = pdu,
            pduType = PduType.NETWORK_PDU,
            sequence = sequence,
            ttl = ttl,
        ).pdu
    }

    @Test
    fun `byte-identical repeat is dropped at stage 1 and relay copy at stage 2`() = runBlocking {
        val mgr = freshManager()
        val nm = mgr.networkManager!!
        val network = mgr.network!!
        val layer = nm.networkLayer
        val cache = layer.networkMessageCache

        val original = wirePdu(network, sequence = 0x001234u, ttl = 5u)
        val relayCopy = wirePdu(network, sequence = 0x001234u, ttl = 4u)
        val next = wirePdu(network, sequence = 0x001235u, ttl = 5u)

        // 전제: relay 사본은 wire 바이트가 다르고, 셋 다 복호화된다.
        assertFalse("relay copy must differ on the wire", original.contentEquals(relayCopy))
        for (bytes in listOf(original, relayCopy, next)) {
            assertNotNull("decodable", NetworkPduDecoder.decode(bytes, PduType.NETWORK_PDU, network))
        }
        assertEquals(0, cache.rawPduCount)
        assertEquals(0, cache.sequenceCount)

        // 1) 원본 수신.
        layer.handle(incomingPdu = original.copyOf(), type = PduType.NETWORK_PDU)
        assertEquals("stage 1 recorded", 1, cache.rawPduCount)
        assertEquals("stage 2 recorded", 1, cache.sequenceCount)

        // 2) 같은 바이트의 새 인스턴스(NetworkTransmit 반복) → stage 1 drop. 종전 identity 키에서는
        //    miss 였고 매번 새 항목이 추가됐다(카운터가 2 가 됐을 것).
        layer.handle(incomingPdu = original.copyOf(), type = PduType.NETWORK_PDU)
        assertEquals("stage 1 hit, no growth", 1, cache.rawPduCount)
        assertEquals("never reached stage 2 again", 1, cache.sequenceCount)

        // 3) relay 사본(TTL 4) → stage 1 통과(새 바이트), stage 2 drop(같은 SRC/IV/SEQ).
        layer.handle(incomingPdu = relayCopy.copyOf(), type = PduType.NETWORK_PDU)
        assertEquals("new wire bytes recorded at stage 1", 2, cache.rawPduCount)
        assertEquals("stage 2 hit: same src/seq not re-recorded", 1, cache.sequenceCount)

        // 4) 다음 SEQ → 통과.
        layer.handle(incomingPdu = next.copyOf(), type = PduType.NETWORK_PDU)
        assertEquals(3, cache.rawPduCount)
        assertEquals("distinct seq passes stage 2", 2, cache.sequenceCount)
    }

    @Test
    fun `mesh beacons bypass the cache`() = runBlocking {
        val mgr = freshManager()
        val cache = mgr.networkManager!!.networkLayer.networkMessageCache
        // Secure Network Beacon 은 proxy 재연결마다 반복 수신되므로 캐시 대상이 아니다(기존 동작 보존).
        val garbageBeacon = ByteArray(22) { 0x01 }
        mgr.networkManager!!.networkLayer.handle(garbageBeacon.copyOf(), PduType.MESH_BEACON)
        mgr.networkManager!!.networkLayer.handle(garbageBeacon.copyOf(), PduType.MESH_BEACON)
        assertEquals(0, cache.rawPduCount)
        assertEquals(0, cache.sequenceCount)
    }
}
