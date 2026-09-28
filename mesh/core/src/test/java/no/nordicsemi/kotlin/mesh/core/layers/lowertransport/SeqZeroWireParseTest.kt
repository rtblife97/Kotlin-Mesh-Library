package no.nordicsemi.kotlin.mesh.core.layers.lowertransport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkPdu
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertEquals
import org.junit.Test
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * SeqZero 13비트 wire 해석 (Mesh Profile 1.0.1 §3.5.2.3.1 Segment Acknowledgment: OBO(1)|SeqZero(13)|RFU(2)|BlockAck(32),
 * §3.5.2.2 Segmented Access: SZMIC(1)|SeqZero(13)|SegO(5)|SegN(5)).
 *
 * 잠그는 불변 조건: 수신 PDU 의 SeqZero 는 보낸 값 그대로 읽힌다 — 하위 6비트가 32 이상이어도(octet 2 의 최상위 비트가 1).
 * 실기기 2026-09-28 14:46:21: 조명이 seqZero 125 에 보낸 ACK(octet 1..2 = 0x01 0xF4)를 16381 로 읽어 버렸다.
 */
@OptIn(ExperimentalUuidApi::class)
class SeqZeroWireParseTest {

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

    private val key: NetworkKey = run {
        val mgr = MeshNetworkManager(StubStorage(), StubSecureProperties(), ioDispatcher = Dispatchers.Default)
        val json = javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()
        runBlocking { mgr.import(json) }
        mgr.network!!.networkKeys.first()
    }

    /** 하위 6비트 < 32 와 ≥ 32 를 모두 포함 (125 = 실기기 값). */
    private val seqZeros = listOf(0, 2, 31, 32, 63, 125, 129, 135, 4096 + 33, 8191)

    private fun networkPdu(transportPdu: ByteArray, ctl: Boolean) = NetworkPdu(
        pdu = ByteArray(0),
        key = key,
        ivIndex = 0u,
        ivi = 0,
        nid = 0,
        type = if (ctl) LowerTransportPduType.CONTROL_MESSAGE else LowerTransportPduType.ACCESS_MESSAGE,
        ttl = 7u,
        sequence = 0x001000u,
        source = MeshAddress.create(0x0307),
        destination = MeshAddress.create(0x0001),
        transportPdu = transportPdu,
    )

    private fun ackBytes(seqZero: Int, block: Int = 0x3): ByteArray = byteArrayOf(
        0x00,
        ((seqZero shr 6) and 0x7F).toByte(),
        ((seqZero and 0x3F) shl 2).toByte(),
        (block ushr 24).toByte(), (block ushr 16).toByte(), (block ushr 8).toByte(), block.toByte(),
    )

    @Test
    fun `segment ACK seqZero is read as sent - device ACK of 14-46-21`() {
        // 실기기 바이트 그대로: seqZero 125 → octet1 0x01, octet2 0xF4
        val real = ackBytes(125)
        assertEquals(0x01.toByte(), real[1])
        assertEquals(0xF4.toByte(), real[2])
        assertEquals(125, SegmentAcknowledgementMessage.init(networkPdu(real, ctl = true)).sequenceZero.toInt())

        for (sz in seqZeros) {
            val ack = SegmentAcknowledgementMessage.init(networkPdu(ackBytes(sz), ctl = true))
            assertEquals("seqZero=$sz", sz, ack.sequenceZero.toInt())
            assertEquals(0x3u, ack.ackedSegments)
        }
    }

    @Test
    fun `segment ACK encode then parse round-trips`() {
        for (sz in seqZeros) {
            val sent = SegmentAcknowledgementMessage(
                source = MeshAddress.create(0x0001),
                destination = MeshAddress.create(0x0307),
                networkKey = key,
                ivIndex = 0u,
                upperTransportPdu = byteArrayOf(0, 0, 0, 3),
                sequenceZero = sz.toUShort(),
                ackedSegments = 3u,
            )
            val parsed = SegmentAcknowledgementMessage.init(networkPdu(sent.transportPdu, ctl = true))
            assertEquals("seqZero=$sz", sz, parsed.sequenceZero.toInt())
        }
    }

    @Test
    fun `segmented access and network PDU seqZero are read as sent`() {
        for (sz in seqZeros) {
            val segO = 1
            val segN = 1
            val bytes = byteArrayOf(
                0x80.toByte(), // SEG=1 AKF=0
                ((sz shr 6) and 0x7F).toByte(), // SZMIC=0
                (((sz and 0x3F) shl 2) or (segO shr 3)).toByte(),
                (((segO and 0x07) shl 5) or segN).toByte(),
                0x11, 0x22, 0x33,
            )
            val pdu = networkPdu(bytes, ctl = false)
            assertEquals("NetworkPdu seqZero=$sz", sz, pdu.sequenceZero?.toInt())
            val seg = SegmentedAccessMessage.init(pdu)!!
            assertEquals("SegmentedAccess seqZero=$sz", sz, seg.sequenceZero.toInt())
            assertEquals(segO, seg.segmentOffset.toInt())
            assertEquals(segN, seg.lastSegmentNumber.toInt())
        }
    }
}
