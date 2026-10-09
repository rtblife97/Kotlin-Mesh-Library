package no.nordicsemi.kotlin.mesh.core.layers.uppertransport

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPduType
import no.nordicsemi.kotlin.mesh.core.layers.network.NetworkPduDecoder
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-09) — 수신 Heartbeat 가 [MeshNetworkManager.heartbeats] 로 나오는지.
 *
 * 실제 NetKey 로 암호화한 Heartbeat Network PDU 를 NetworkLayer 에 넣는다. 목적지는 sink 그룹 0xC0F0 이고
 * 로컬 노드에 Heartbeat Subscription 이 없다 — 구독과 무관하게 나와야 한다는 불변 조건.
 */
@OptIn(ExperimentalUuidApi::class)
class HeartbeatSurfaceTest {

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

    /** InitTTL 40 (0x28), Features = Relay|Proxy (0x0003, big-endian). */
    private class HeartbeatControlPdu(
        override val networkKey: NetworkKey,
        override val ivIndex: UInt,
        override val source: MeshAddress,
        override val destination: MeshAddress,
    ) : LowerTransportPdu {
        override val type = LowerTransportPduType.CONTROL_MESSAGE
        override val upperTransportPdu: ByteArray = byteArrayOf(0x28, 0x00, 0x03)
        override val transportPdu: ByteArray = byteArrayOf(0x0A) + upperTransportPdu
    }

    @Test
    fun `heartbeat to sink group is emitted regardless of subscription`() = runBlocking {
        val mgr = MeshNetworkManager(
            storage = StubStorage(),
            secureProperties = StubSecureProperties(),
            ioDispatcher = Dispatchers.Default,
        )
        val json = javaClass.classLoader.getResourceAsStream("cdb_json.json")!!.readAllBytes()
        mgr.import(json)
        val network = mgr.network!!
        assertTrue(
            "전제: 로컬 노드에 일치하는 구독이 없다",
            network.localProvisioner?.node?.heartbeatSubscription == null,
        )

        val wire = NetworkPduDecoder.encode(
            lowerTransportPdu = HeartbeatControlPdu(
                networkKey = network.networkKeys.first(),
                ivIndex = network.ivIndex.transmitIvIndex,
                source = MeshAddress.create(0x0002),
                destination = MeshAddress.create(0xC0F0),
            ),
            pduType = PduType.NETWORK_PDU,
            sequence = 0x000100u,
            ttl = 37u,
        ).pdu

        val received = async(start = CoroutineStart.UNDISPATCHED) {
            withTimeout(5_000) { mgr.heartbeats.first() }
        }
        mgr.networkManager!!.networkLayer.handle(incomingPdu = wire, type = PduType.NETWORK_PDU)
        val hb = received.await()

        assertEquals(0x0002.toUShort(), hb.source)
        assertEquals(0xC0F0.toUShort(), hb.destination)
        assertEquals(40.toUByte(), hb.initialTtl)
        assertEquals(37.toUByte(), hb.receivedTtl)
        assertEquals("InitTTL - RecvTTL + 1", 4.toUByte(), hb.hops)
        assertEquals(0x0003.toUShort(), hb.features)
        assertTrue(hb.isRelayActive && hb.isProxyActive && !hb.isFriendActive && !hb.isLowPowerActive)
    }
}
