package no.nordicsemi.kotlin.mesh.core.layers.network

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.access.AccessPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.AccessMessage
import no.nordicsemi.kotlin.mesh.core.layers.AccessKeySet
import no.nordicsemi.kotlin.mesh.core.layers.uppertransport.UpperTransportPdu
import no.nordicsemi.kotlin.mesh.core.messages.ConfigMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppStatus
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.SigModelId
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-17, 게이트웨이 동글 공중 측정 경로 A) — [NetworkRxDiagnostics.decodeForDiagnostics].
 * 실제 NetKey·AppKey 로 암호화한 **남의 목적지** PDU 가 헤더(src·dst·seq·ttl)로 풀리고, 한 바이트만 바뀌어도(MIC) null 인지 본다.
 */
@OptIn(ExperimentalUuidApi::class)
class NetworkRxDiagnosticsDecodeTest {

    @Test
    fun `남의 목적지 PDU 도 헤더가 풀리고 깨진 PDU 는 null`() {
        val network = manager().network!!
        val pdu = appKeyStatus(network, from = 0x0007, to = 0x0105, sequence = 77u, ttl = 4u)
        val r = NetworkRxDiagnostics.decodeForDiagnostics(pdu, network)!!
        assertEquals(0x0007, r.source)
        assertEquals(0x0105, r.destination)
        assertEquals(77L, r.sequence)
        assertEquals(4, r.ttl)
        assertNull(r.heartbeatInitTtl)
        val broken = pdu.copyOf().also { it[it.size - 1] = (it[it.size - 1].toInt() xor 0x01).toByte() }
        assertNull(NetworkRxDiagnostics.decodeForDiagnostics(broken, network))
        assertNull(NetworkRxDiagnostics.decodeForDiagnostics(ByteArray(5), network))
    }

    private fun appKeyStatus(network: MeshNetwork, from: Int, to: Int, sequence: UInt, ttl: UByte): ByteArray {
        val appKey = network.applicationKeys.first()
        val status = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, 0u, UnicastAddress(from.toUShort()), SigModelId(0x1000u))
        val access = AccessPdu.init(message = status, source = from.toUShort(), destination = MeshAddress.create(to), userInitiated = false)
        val upper = UpperTransportPdu.init(access, AccessKeySet(appKey), sequence, network.ivIndex)
        return NetworkPduDecoder.encode(
            lowerTransportPdu = AccessMessage(upper, appKey.boundNetworkKey),
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
        private val seq = java.util.concurrent.atomic.AtomicInteger(0)
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

    private fun manager(): MeshNetworkManager = MeshNetworkManager(
        storage = StubStorage(),
        secureProperties = StubSecureProperties(),
        ioDispatcher = Dispatchers.Default,
    ).also { runBlocking { it.import(cdb) } }
}
