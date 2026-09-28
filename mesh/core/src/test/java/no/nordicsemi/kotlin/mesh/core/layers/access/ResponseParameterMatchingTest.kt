package no.nordicsemi.kotlin.mesh.core.layers.access

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
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
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.layers.ReceivedMessage
import no.nordicsemi.kotlin.mesh.core.messages.BaseMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.ConfigMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppBind
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelAppStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelSubscriptionAdd
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelSubscriptionDeleteAll
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigModelSubscriptionStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNodeIdentitySet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigNodeIdentityStatus
import no.nordicsemi.kotlin.mesh.core.model.NodeIdentityState
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import no.nordicsemi.kotlin.mesh.core.model.SigModelId
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-09-29) — 응답을 요청과 **정확히** 짝짓는다 (실기기 2026-09-28 17:57:11.259).
 *
 * 불변:
 *  - 요청 파라미터를 되돌려 주는 Status 는 파라미터까지 맞아야 그 요청의 응답이다. 다른 파라미터의 Status(앞 요청의 늦은 응답)는
 *    그 요청의 응답(성공이든 실패든)으로 인정되지 않는다.
 *  - 호출자가 기다리기를 포기한 요청은 더 재전송되지 않는다.
 *
 * 가짜 bearer 는 lib 가 실제로 내보내는 PDU 를 센다(재전송 포함). 응답은 실제 수신 경로처럼 다른 코루틴에서 흘린다.
 */
@OptIn(ExperimentalUuidApi::class)
class ResponseParameterMatchingTest {

    private val elem = UnicastAddress(0x0004u)
    private val onOff = SigModelId(0x1000u)

    // ── 순수 규칙 ──────────────────────────────────────────────────────────────

    @Test
    fun `Model App Status for another AppKey is not the response to this bind`() {
        val bindApp1 = ConfigModelAppBind(applicationKeyIndex = 1u, elementAddress = elem, modelId = onOff)
        val lateApp0 = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, applicationKeyIndex = 0u, elementAddress = elem, modelId = onOff)
        val ownApp1Fail = ConfigModelAppStatus(ConfigMessageStatus.INVALID_APP_KEY_INDEX, applicationKeyIndex = 1u, elementAddress = elem, modelId = onOff)
        val otherModel = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, applicationKeyIndex = 1u, elementAddress = elem, modelId = SigModelId(0x1002u))
        val otherElement = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, applicationKeyIndex = 1u, elementAddress = UnicastAddress(0x0005u), modelId = onOff)

        assertFalse(responseMatchesRequest(bindApp1, lateApp0))
        assertFalse(responseMatchesRequest(bindApp1, otherModel))
        assertFalse(responseMatchesRequest(bindApp1, otherElement))
        assertTrue("실패 Status 도 자기 요청 파라미터면 그 요청의 응답", responseMatchesRequest(bindApp1, ownApp1Fail))
    }

    @Test
    fun `Subscription Status matches on group address, Delete All ignores it`() {
        val add = ConfigModelSubscriptionAdd(address = 0xC001u, elementAddress = elem, modelIdentifier = 0x1000u, companyIdentifier = null)
        val statusOther = ConfigModelSubscriptionStatus(ConfigMessageStatus.SUCCESS, 0xC002u, elem, 0x1000u, null)
        val statusOwn = ConfigModelSubscriptionStatus(ConfigMessageStatus.SUCCESS, 0xC001u, elem, 0x1000u, null)
        assertFalse(responseMatchesRequest(add, statusOther))
        assertTrue(responseMatchesRequest(add, statusOwn))

        val deleteAll = ConfigModelSubscriptionDeleteAll(elementAddress = elem, modelIdentifier = 0x1000u, companyIdentifier = null)
        val unassigned = ConfigModelSubscriptionStatus(ConfigMessageStatus.SUCCESS, 0x0000u, elem, 0x1000u, null)
        assertTrue("Delete All 의 Status 주소는 unassigned — 주소를 보지 않는다", responseMatchesRequest(deleteAll, unassigned))
    }

    @Test
    fun `Node Identity Status must be for the same subnet`() {
        val set = ConfigNodeIdentitySet(networkKeyIndex = 0u, identityState = NodeIdentityState.RUNNING)
        val other = ConfigNodeIdentityStatus(ConfigMessageStatus.SUCCESS, networkKeyIndex = 1u, identity = NodeIdentityState.RUNNING)
        val own = ConfigNodeIdentityStatus(ConfigMessageStatus.SUCCESS, networkKeyIndex = 0u, identity = NodeIdentityState.RUNNING)
        assertFalse(responseMatchesRequest(set, other))
        assertTrue(responseMatchesRequest(set, own))
    }

    // ── 송신 경로 (awaitMeshMessageResponse) ─────────────────────────────────────

    @Test
    fun `late Status of the previous bind is not delivered as this bind's response`() = runBlocking {
        val mgr = manager()
        val node = mgr.network!!.nodes.first { it.primaryUnicastAddress.address.toInt() == 0x0004 }
        val own = ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, applicationKeyIndex = 1u, elementAddress = elem, modelId = onOff)
        val bearer = CountingBearer {
            // 앞 요청(bind app0)의 늦은 응답이 먼저, 그다음 이 요청의 응답.
            emit(mgr, ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, applicationKeyIndex = 0u, elementAddress = elem, modelId = onOff))
            delay(50)
            emit(mgr, own)
        }
        mgr.registerBearer(0x0004u, bearer)

        val r = withTimeoutOrNull(5_000) {
            mgr.send(ConfigModelAppBind(applicationKeyIndex = 1u, elementAddress = elem, modelId = onOff), node, initialTtl = null)
        }
        assertSame("이 요청의 Status 만 응답", own, r)
    }

    @Test
    fun `only a mismatching Status arrives - the request gets no response instead of a wrong success`() = runBlocking {
        val mgr = manager()
        val node = mgr.network!!.nodes.first { it.primaryUnicastAddress.address.toInt() == 0x0004 }
        val bearer = CountingBearer {
            emit(mgr, ConfigModelAppStatus(ConfigMessageStatus.SUCCESS, applicationKeyIndex = 0u, elementAddress = elem, modelId = onOff))
        }
        mgr.registerBearer(0x0004u, bearer)

        val r = withTimeoutOrNull(1_500) {
            mgr.send(ConfigModelAppBind(applicationKeyIndex = 1u, elementAddress = elem, modelId = onOff), node, initialTtl = null)
        }
        assertNull("다른 파라미터의 Status 로 성공 처리하면 안 된다", r)
    }

    @Test
    fun `a request the caller gave up on is not retransmitted any more`() = runBlocking {
        // 대조: 기다리는 동안에는 lib 가 재전송한다 (가짜가 재전송을 실제로 관찰할 수 있음을 확인).
        val watched = manager()
        val watchedNode = watched.network!!.nodes.first { it.primaryUnicastAddress.address.toInt() == 0x0004 }
        val silentA = CountingBearer { }
        watched.registerBearer(0x0004u, silentA)
        val pending = async(Dispatchers.Default) {
            withTimeoutOrNull(20_000) {
                watched.send(ConfigModelAppBind(applicationKeyIndex = 1u, elementAddress = elem, modelId = onOff), watchedNode, initialTtl = null)
            }
        }
        waitUntil(8_000) { silentA.sent.get() >= 2 }
        assertTrue("기다리는 동안 재전송 관찰 (보낸 PDU ${silentA.sent.get()})", silentA.sent.get() >= 2)
        pending.cancel()

        // 포기: 첫 송신 뒤 곧바로 기다리기를 멈춘다(앱 15 s 포기와 같은 취소).
        val mgr = manager()
        val node = mgr.network!!.nodes.first { it.primaryUnicastAddress.address.toInt() == 0x0004 }
        val silent = CountingBearer { }
        mgr.registerBearer(0x0004u, silent)
        val r = withTimeoutOrNull(300) {
            mgr.send(ConfigModelAppBind(applicationKeyIndex = 1u, elementAddress = elem, modelId = onOff), node, initialTtl = null)
        }
        assertNull(r)
        val afterGiveUp = silent.sent.get()
        delay(8_000)
        assertTrue("포기한 요청 재전송 0 (포기 때 ${afterGiveUp}, 지금 ${silent.sent.get()})", silent.sent.get() == afterGiveUp)
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    private suspend fun waitUntil(ms: Long, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + ms
        while (!condition() && System.currentTimeMillis() < deadline) delay(50)
    }

    private suspend fun emit(mgr: MeshNetworkManager, message: BaseMeshMessage) {
        mgr.networkManager!!.emitIncomingMeshMessageForTest(
            ReceivedMessage(
                source = MeshAddress.create(0x0004),
                destination = mgr.network!!.localProvisioner!!.node!!.primaryUnicastAddress,
                message = message,
                sequence = 1u,
                ivIndex = 0u,
                ttl = 7u,
            ),
        )
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

    /** 보낸 PDU 를 세고, 첫 PDU 가 나가는 순간 [reply] 를 다른 코루틴에서 돌린다 (실제 수신 경로도 별도 코루틴). */
    private class CountingBearer(private val reply: suspend () -> Unit) : MeshBearer {
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
}
