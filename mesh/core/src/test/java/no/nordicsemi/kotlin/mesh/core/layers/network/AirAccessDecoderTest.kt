package no.nordicsemi.kotlin.mesh.core.layers.network

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.AccessKeySet
import no.nordicsemi.kotlin.mesh.core.layers.DeviceKeySet
import no.nordicsemi.kotlin.mesh.core.layers.KeySet
import no.nordicsemi.kotlin.mesh.core.layers.access.AccessPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.AccessMessage
import no.nordicsemi.kotlin.mesh.core.layers.uppertransport.UpperTransportPdu
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-17, 작업자 앱 공중 수신) — [AirAccessDecoder].
 * 실제 CDB 키로 lib 인코더가 만든 태그 보고(벤더 0x0059/0x02 = wire C2 59 00, 그룹 목적지)가 접근 페이로드까지 풀리고,
 * 난독화 헤더가 같은 src·SEQ·TTL 을 주며, MIC 가 깨졌거나 DevKey 메시지면 null 인지 본다.
 */
@OptIn(ExperimentalUuidApi::class)
class AirAccessDecoderTest {

    private object TagReport : UnacknowledgedMeshMessage {
        override val opCode: UInt = 0xC25900u
        override val parameters: ByteArray = byteArrayOf(0x8C.toByte(), 0x20, 0x7C, 0x1F)
    }

    @Test
    fun `남의 그룹 목적지 벤더 보고가 접근 페이로드까지 풀린다`() {
        val network = manager().network!!
        val appKey = network.applicationKeys.first()
        val pdu = encode(network, TagReport, AccessKeySet(appKey), from = 0x0035, to = 0xC000, sequence = 1234u, ttl = 37u)

        val h = AirAccessDecoder.peekHeader(pdu, network)!!
        assertEquals(0x0035, h.source)
        assertEquals(1234L, h.sequence)
        assertEquals(37, h.ttl)
        assertFalse(h.ctl)
        assertTrue(h.nid in AirAccessDecoder.networkIds(network))

        val a = AirAccessDecoder.decodeAccess(pdu, network)!!
        assertEquals(0x0035, a.source)
        assertEquals(0xC000, a.destination)
        assertEquals(1234L, a.sequence)
        assertEquals(37, a.ttl)
        assertArrayEquals(byteArrayOf(0xC2.toByte(), 0x59, 0x00) + TagReport.parameters, a.accessPdu)
    }

    @Test
    fun `깨진 MIC 와 DevKey 메시지는 null`() {
        val network = manager().network!!
        val appKey = network.applicationKeys.first()
        val pdu = encode(network, TagReport, AccessKeySet(appKey), from = 0x0035, to = 0xC000, sequence = 9u, ttl = 5u)
        val broken = pdu.copyOf().also { it[it.size - 1] = (it[it.size - 1].toInt() xor 0x01).toByte() }
        assertNull(AirAccessDecoder.decodeAccess(broken, network))
        assertNull(AirAccessDecoder.decodeAccess(ByteArray(5), network))

        val node = network.nodes.first { it.deviceKey != null }
        val dev = DeviceKeySet.init(appKey.boundNetworkKey!!, node)
        assertNotNull(dev)
        val devPdu = encode(network, TagReport, dev!!, from = node.primaryUnicastAddress.address.toInt(), to = 0x0001, sequence = 10u, ttl = 5u)
        assertNull(AirAccessDecoder.decodeAccess(devPdu, network))
    }

    private fun encode(network: MeshNetwork, msg: UnacknowledgedMeshMessage, keySet: KeySet, from: Int, to: Int, sequence: UInt, ttl: UByte): ByteArray {
        val access = AccessPdu.init(message = msg, source = from.toUShort(), destination = MeshAddress.create(to), userInitiated = false)
        val upper = UpperTransportPdu.init(access, keySet, sequence, network.ivIndex)
        return NetworkPduDecoder.encode(
            lowerTransportPdu = AccessMessage(upper, keySet.networkKey),
            pduType = PduType.NETWORK_PDU,
            sequence = sequence,
            ttl = ttl,
        ).pdu
    }

    private class StubStorage : Storage {
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = Unit
    }

    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) = Unit
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = 0u
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

    private fun manager(): MeshNetworkManager = MeshNetworkManager(
        storage = StubStorage(),
        secureProperties = StubSecureProperties(),
        ioDispatcher = Dispatchers.Default,
    ).also { runBlocking { it.import(cdb) } }
}
