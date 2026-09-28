package no.nordicsemi.kotlin.mesh.core.layers.access

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
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
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.ReceivedMessage
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigAppKeyAdd
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigAppKeyStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigDefaultTtlGet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigDefaultTtlStatus
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.messages.BaseMeshMessage
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * 응답이 송신 함수 반환보다 먼저 도착해도 받는다 (simdo-fork, 2026-09-28).
 *
 * 실기기 2026-09-28 14:46:22 — 2분할 ConfigAppKeyAdd 의 Status 가 22.320 에 수신·복호(로그 "ConfigAppKeyStatus received")
 * 됐는데 호출자는 15 s 타임아웃. 원인: 응답 구독이 `upperTransportLayer.send` 반환 뒤에 등록됐고, 분할 송신은 마지막 분할
 * 뒤에도 분할 간격만큼 기다렸다 반환한다. 노드가 그 안에 응답하면 replay 없는 SharedFlow 에서 사라졌다.
 *
 * 잠그는 불변 조건: 요청이 bearer 로 나간 뒤 도착한 응답은 — 송신 함수가 아직 돌아오지 않았어도 — 그 요청의 응답으로
 * 전달된다. 가짜 bearer 는 첫 PDU(분할 0)가 나가는 순간 응답을 다른 코루틴에서 흘린다(실제 수신 경로도 bearer
 * collector 의 별도 코루틴에서 emit 한다).
 */
@OptIn(ExperimentalUuidApi::class)
class ResponseBeforeSendReturnsTest {

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

    /** 노드 흉내: 요청의 첫 PDU 가 나가는 순간(송신 함수 반환 전) 응답 하나를 수신 경로로 흘린다. */
    private class ReplyingBearer(private val reply: suspend () -> Unit) : MeshBearer {
        val sent = AtomicInteger()
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        override val pdus: Flow<Pdu> = MutableSharedFlow()
        override val state: StateFlow<BearerEvent>
            get() = throw UnsupportedOperationException()
        override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.NetworkPdu)
        override val isOpen: Boolean = true
        override suspend fun open() = Unit
        override suspend fun close() = scope.cancel()
        override suspend fun send(pdu: ByteArray, type: PduType) {
            if (sent.getAndIncrement() == 0) scope.launch { reply() }
        }
    }

    private val cdb: ByteArray =
        javaClass.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()

    private fun manager(): MeshNetworkManager = MeshNetworkManager(
        storage = StubStorage(),
        secureProperties = StubSecureProperties(),
        ioDispatcher = Dispatchers.Default,
    ).also { runBlocking { it.import(cdb) } }

    private fun received(mgr: MeshNetworkManager, from: Int, message: BaseMeshMessage) = ReceivedMessage(
        source = MeshAddress.create(from),
        destination = mgr.network!!.localProvisioner!!.node!!.primaryUnicastAddress,
        message = message,
        sequence = 1u,
        ivIndex = 0u,
        ttl = 7u,
    )

    @Test
    fun `segmented config request - status arriving before send returns is delivered`() = runBlocking {
        val mgr = manager()
        val network = mgr.network!!
        val node = network.nodes.first { it.primaryUnicastAddress.address.toInt() == 0x0004 }
        val appKey = network.applicationKeys.first()
        val bearer = ReplyingBearer {
            mgr.networkManager!!.emitIncomingMeshMessageForTest(
                received(mgr, 0x0004, ConfigAppKeyStatus(applicationKey = appKey)),
            )
        }
        // 1홉 직접(실기기와 같음): 목적지에 bearer 를 등록 — 연결된 프록시 없이도 목적지 노드 키로 보낸다.
        mgr.registerBearer(0x0004u, bearer)

        // ConfigAppKeyAdd = 1+3+16 옥텟 access → 분할 2개 (실기기와 같은 형태).
        val response = withTimeoutOrNull(5_000) { mgr.send(ConfigAppKeyAdd(key = appKey), node, initialTtl = null) }

        assertTrue("분할 송신이어야 한다 (보낸 PDU ${bearer.sent.get()})", bearer.sent.get() >= 2)
        assertTrue("응답을 놓쳤다: $response", response is ConfigAppKeyStatus)
    }

    @Test
    fun `unsegmented config request - early status is delivered`() = runBlocking {
        val mgr = manager()
        val node = mgr.network!!.nodes.first { it.primaryUnicastAddress.address.toInt() == 0x0004 }
        val bearer = ReplyingBearer {
            mgr.networkManager!!.emitIncomingMeshMessageForTest(received(mgr, 0x0004, ConfigDefaultTtlStatus(ttl = 7u)))
        }
        // 1홉 직접(실기기와 같음): 목적지에 bearer 를 등록 — 연결된 프록시 없이도 목적지 노드 키로 보낸다.
        mgr.registerBearer(0x0004u, bearer)

        val response = withTimeoutOrNull(5_000) { mgr.send(ConfigDefaultTtlGet(), node, initialTtl = null) }

        assertTrue("응답을 놓쳤다: $response", response is ConfigDefaultTtlStatus)
    }
}
