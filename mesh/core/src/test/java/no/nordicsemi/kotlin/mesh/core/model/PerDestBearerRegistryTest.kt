package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import no.nordicsemi.kotlin.mesh.core.layers.access.CannotRelay
import no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration.ConfigCompositionDataGet
import kotlin.time.Duration.Companion.milliseconds
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.MeshBearer
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.ExperimentalUuidApi

/**
 * P6 M=2 회귀 가드 (simdo-fork, 2026-06-07 — config 병렬화 per-dest bearer registry).
 *
 * ## 배경
 *
 * config 병렬화(P6)는 N 노드를 N 개 1-hop GATT 로 **동시** config 한다. 종전 lib 는 `NetworkManager`
 * 에 [NetworkManager.bearer] **단일 슬롯**뿐이라 한 시점에 한 채널만 가능했다(=직렬 config). P6 는
 * dst unicast → 그 노드 bearer 의 **registry**([NetworkManager.registerBearer])를 도입한다:
 *  - TX([NetworkLayer.send])는 `networkPdu.destination.address` → [NetworkManager.bearerFor] 로 라우팅.
 *  - 미등록 dst(group/proxy/평상시 측위)는 default [NetworkManager.bearer] 로 떨어진다(회귀 0).
 *  - RX 는 src/dest-keyed 라 dst 라우팅 불필요 → registered bearer 마다 collector 를 fan-in.
 *
 * 본 테스트는 실제 [MeshNetworkManager] 를 import 해 살아있는 `NetworkManager` 위에서 registry
 * 라우팅 불변을 가드한다(stub MeshBearer 들을 등록/해제하고 [NetworkManager.bearerFor] 결과를 검증).
 */
@OptIn(ExperimentalUuidApi::class)
class PerDestBearerRegistryTest {

    /** pdus flow + send 캡처만 하는 최소 MeshBearer stub. */
    private class StubBearer(val tag: String) : MeshBearer {
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

    // 본 테스트 자족(self-contained) storage stub — 공유 [TestPropertiesStorage] 는 현재 interface 와
    // 시그니처가 어긋나(suspend mismatch) 컴파일되지 않으므로 여기 최신 시그니처로 직접 둔다.
    private class StubStorage : Storage {
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = Unit
    }

    @OptIn(kotlin.uuid.ExperimentalUuidApi::class)
    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: kotlin.uuid.Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: kotlin.uuid.Uuid, ivIndex: IvIndex) = Unit
        private var seq: UInt = 0u
        override suspend fun nextSequenceNumber(uuid: kotlin.uuid.Uuid, address: UnicastAddress): UInt = seq++
        override suspend fun storeNextSequenceNumber(uuid: kotlin.uuid.Uuid, address: UnicastAddress, sequenceNumber: UInt) = Unit
        override suspend fun resetSequenceNumber(uuid: kotlin.uuid.Uuid, address: UnicastAddress) = Unit
        override suspend fun lastSeqAuthValue(uuid: kotlin.uuid.Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(uuid: kotlin.uuid.Uuid, source: UnicastAddress, lastSeqAuth: ULong) = Unit
        override suspend fun previousSeqAuthValue(uuid: kotlin.uuid.Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(uuid: kotlin.uuid.Uuid, source: UnicastAddress, seqAuth: ULong) = Unit
        override suspend fun storeLocalProvisioner(uuid: kotlin.uuid.Uuid, localProvisionerUuid: kotlin.uuid.Uuid) = Unit
        override suspend fun localProvisioner(uuid: kotlin.uuid.Uuid): String? = null
    }

    private fun freshNetworkManager(): MeshNetworkManager {
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

    private val nodeA: UShort = 0x0002u
    private val nodeB: UShort = 0x0003u
    private val groupAddr: UShort = 0xC000u

    /**
     * 미등록 dst·group 은 default bearer 로, 등록된 dst 는 각자 자기 bearer 로 라우팅된다.
     * (= [NetworkManager.bearerFor] 의 `_bearers[dst] ?: bearer` 불변 — TX 채널 분리의 핵심.)
     */
    @Test
    fun `bearerFor 는 등록 dst 는 그 bearer, 미등록은 default 로 라우팅한다`() {
        val mgr = freshNetworkManager()
        val nm = mgr.networkManager!!
        val default = StubBearer("default")
        val bearerForA = StubBearer("A")
        val bearerForB = StubBearer("B")

        mgr.meshBearer = default // default 슬롯

        // 등록 전: 모든 dst 가 default.
        assertSame("등록 전 nodeA 는 default", default, nm.bearerFor(nodeA))
        assertSame("등록 전 nodeB 는 default", default, nm.bearerFor(nodeB))

        // nodeA, nodeB 각각 등록.
        assertTrue(mgr.registerBearer(nodeA, bearerForA))
        assertTrue(mgr.registerBearer(nodeB, bearerForB))

        // 등록된 dst 는 각자 자기 bearer, 미등록(group)·default 슬롯은 default.
        assertSame("nodeA → A", bearerForA, nm.bearerFor(nodeA))
        assertSame("nodeB → B", bearerForB, nm.bearerFor(nodeB))
        assertSame("group(미등록) → default", default, nm.bearerFor(groupAddr))

        // nodeA 해제 후: nodeA 는 default 로 복귀, nodeB 는 유지(독립성).
        mgr.unregisterBearer(nodeA)
        assertSame("해제 후 nodeA → default", default, nm.bearerFor(nodeA))
        assertSame("nodeB 는 유지", bearerForB, nm.bearerFor(nodeB))
    }

    /**
     * registry 가 비어 있으면 default [NetworkManager.bearer] 만 쓰인다(평상시 측위/group 제어 회귀 0).
     * default 가 null 이면 bearerFor 도 null(종전 단일 슬롯 동작 보존).
     */
    @Test
    fun `registry 비면 default 슬롯과 동일 동작 - 비-config 경로 회귀 0`() {
        val mgr = freshNetworkManager()
        val nm = mgr.networkManager!!

        // default null → 모든 dst null (종전 단일 슬롯 null 과 동일).
        assertNull("default null 이면 bearerFor 도 null", nm.bearerFor(nodeA))

        val default = StubBearer("default")
        mgr.meshBearer = default
        assertSame("registry 비면 모든 dst 가 default", default, nm.bearerFor(nodeA))
        assertSame(default, nm.bearerFor(groupAddr))
        assertTrue("registry 비어있음", nm.bearers.isEmpty())
    }

    /**
     * 등록/해제가 [NetworkManager.bearers] 맵을 정확히 반영하고 멱등하다(누수 0).
     */
    @Test
    fun `register unregister 는 bearers 맵을 정확히 반영하고 멱등하다`() {
        val mgr = freshNetworkManager()
        val nm = mgr.networkManager!!
        val a = StubBearer("A")
        val b = StubBearer("B")

        mgr.registerBearer(nodeA, a)
        mgr.registerBearer(nodeB, b)
        assertEquals(2, nm.bearers.size)
        assertSame(a, nm.bearers[nodeA])

        // 멱등: 같은 dst 재등록(다른 bearer) → 교체, 크기 유지.
        val a2 = StubBearer("A2")
        mgr.registerBearer(nodeA, a2)
        assertEquals("같은 dst 재등록은 교체(크기 불변)", 2, nm.bearers.size)
        assertSame(a2, nm.bearers[nodeA])

        // 해제 후 크기 감소, 재해제 멱등(no-op).
        mgr.unregisterBearer(nodeA)
        assertEquals(1, nm.bearers.size)
        mgr.unregisterBearer(nodeA)
        assertEquals("재해제 멱등", 1, nm.bearers.size)
        assertFalse(nm.bearers.containsKey(nodeA))
    }

    /**
     * networkManager 미초기화(network 미로드) 시 registerBearer 는 no-op(false) — 안전 실패.
     */
    @Test
    fun `network 미로드면 registerBearer 는 false`() {
        val mgr = MeshNetworkManager(
            storage = StubStorage(),
            secureProperties = StubSecureProperties(),
            ioDispatcher = Dispatchers.Default,
        )
        // import 안 함 → networkManager null.
        assertNull(mgr.networkManager)
        assertFalse("network 미로드면 false", mgr.registerBearer(nodeA, StubBearer("A")))
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // P6 M=2 device 검증 실패 회귀 가드 (simdo-fork, 2026-06-07).
    //
    // 증상: per-dest bearer 가 registry 에 등록되고 1-hop GATT 도 연결됐는데, config send 가
    //   "No GATT Proxy connected or no common Network Keys" → CannotRelay 로 거부됐다.
    // root: MeshNetworkManager.send(AcknowledgedConfigMessage) 의 send-전 proxy-key 가드가
    //   **단일 글로벌** proxyFilter.proxy 만 봤다(registry-unaware). per-dest bearer 로만 등록하고
    //   default 슬롯(=Main proxy)을 건드리지 않으면 proxyFilter.proxy 는 그 노드를 모름 → 거부.
    // fix: 가드를 registry-aware 로 — dst 에 registered bearer 가 있으면(=1-hop 직접 도달) default
    //   proxy 가 아니라 **목적지 노드 자신**이 netkey 를 아는지로 판정([NetworkManager.hasRegisteredBearer]).

    private val remoteNode: UShort = 0x0002u // cdb fixture: Bedroom Light Switch, netKey 0, 원격(=비-localProvisioner).

    /**
     * 핵심 회귀: per-dest bearer 등록 + proxyFilter.proxy == null 인데도 config send 가
     * **CannotRelay 를 던지지 않는다**(가드가 노드 자신의 netkey 를 인정 → 통과). 가드 통과 후 응답
     * 대기에서 timeout 으로 빠지는 것은 정상(StubBearer 는 송신 no-op) — CannotRelay 만 안 나오면 된다.
     */
    @Test
    fun `registered bearer 면 proxy 가 null 이어도 config send 가 CannotRelay 안 던진다`() {
        val mgr = freshNetworkManager()
        // default 슬롯은 비워둔다(=Main proxy 없음). 종전이라면 CannotRelay 가 날 상황.
        assertNull("proxyFilter.proxy 는 null(연결된 proxy 없음)", mgr.proxyFilter.proxy)

        mgr.registerBearer(remoteNode, StubBearer("perDest-0x0002"))

        runBlocking {
            try {
                withTimeout(300.milliseconds) {
                    mgr.send(
                        message = ConfigCompositionDataGet(page = 0xFFu),
                        destination = remoteNode,
                    )
                }
                // 응답 없이 반환되어도 OK — 가드는 통과했다.
            } catch (e: CannotRelay) {
                throw AssertionError(
                    "registered bearer 면 가드를 통과해야 하는데 CannotRelay 가 났다(=registry-unaware 회귀)", e
                )
            } catch (e: TimeoutCancellationException) {
                // 가드 통과 후 응답 대기 timeout — 기대 동작.
            }
        }
    }

    /**
     * 음성 대조군: registry 가 비어 있고(=per-dest bearer 미등록) proxyFilter.proxy 도 null 이면
     * 종전대로 CannotRelay 를 던진다(가드의 안전 동작 보존 — 채널이 정말 없을 때 빠른 실패).
     */
    @Test
    fun `미등록 dst + proxy null 이면 종전대로 CannotRelay 를 던진다`() {
        val mgr = freshNetworkManager()
        assertNull(mgr.proxyFilter.proxy)
        // 등록 안 함.

        runBlocking {
            var threwCannotRelay = false
            try {
                withTimeout(300.milliseconds) {
                    mgr.send(
                        message = ConfigCompositionDataGet(page = 0xFFu),
                        destination = remoteNode,
                    )
                }
            } catch (e: CannotRelay) {
                threwCannotRelay = true
            } catch (e: TimeoutCancellationException) {
                // 가드를 통과해 버린 것 → 회귀.
            }
            assertTrue(
                "미등록 dst + proxy null 이면 CannotRelay 가 나야 한다(가드 안전 동작 보존)",
                threwCannotRelay
            )
        }
    }

    /**
     * M=2 동시: 서로 다른 두 dst 에 각각 per-dest bearer 등록 → **둘 다** 가드를 독립적으로 통과한다
     * (한 노드의 등록이 다른 노드 가드에 영향 0 — 동시 config 의 핵심 불변).
     */
    @Test
    fun `M=2 동시 — 서로 다른 두 dst 각각 등록되면 둘 다 가드 통과한다`() {
        val mgr = freshNetworkManager()
        assertNull(mgr.proxyFilter.proxy)

        val dst1: UShort = 0x0002u // Bedroom Light Switch
        val dst2: UShort = 0x0004u // Bedroom Light — 둘 다 netKey 0, 원격.
        mgr.registerBearer(dst1, StubBearer("perDest-1"))
        mgr.registerBearer(dst2, StubBearer("perDest-2"))

        for (dst in listOf(dst1, dst2)) {
            runBlocking {
                try {
                    withTimeout(300.milliseconds) {
                        mgr.send(
                            message = ConfigCompositionDataGet(page = 0xFFu),
                            destination = dst,
                        )
                    }
                } catch (e: CannotRelay) {
                    throw AssertionError("dst 0x${dst.toString(16)} 가드가 CannotRelay 로 막힘(동시성 회귀)", e)
                } catch (e: TimeoutCancellationException) {
                    // 통과 후 timeout — OK.
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // busy-set 누수 봉인 회귀 가드 (simdo-fork, 2026-06-08 — C-F2). 코드리뷰 R1.
    //
    // 종전 NetworkManager.send 5변형은 `accessLayer.send(...).also { outgoingMessages.remove(dst) }`
    // 였다. `.also` 는 **정상 return 시만** 실행되므로, accessLayer.send 가 ack timeout(또는 outer
    // cancel)으로 throw 하면 dst 가 outgoingMessages 에 영구 잔류 → 같은 dst 재시도 send 가 Busy 로
    // 거부된다(M=7 에서 timeout 빈도↑로 누수 누적). fix: `.also` → try/finally 로 throw/cancel/정상
    // 모두 remove 보장.
    //
    // 본 테스트는 StubBearer(send=no-op) 로 ack 가 절대 오지 않게 해 send 를 timeout/cancel 시킨 뒤,
    // **같은 dst 재시도가 Busy 를 던지지 않는지**(=busy-set 이 비워졌는지) 검증한다.
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * timeout/cancel 로 throw 된 첫 send 가 busy-set 을 비우는지 — 같은 dst 두 번째 send 가 Busy 안 난다.
     * (fix 전: 첫 send 가 throw 하면 `.also` 미실행 → dst 잔류 → 두 번째가 Busy. fix 후: finally 가 제거.)
     */
    @Test
    fun `timeout 으로 throw 된 send 후 같은 dst 재시도가 Busy 안 난다(누수 봉인)`() {
        val mgr = freshNetworkManager()
        // per-dest bearer(send=no-op) 등록 → 가드 통과하되 ack 는 영원히 안 옴 → 응답 대기 throw.
        mgr.registerBearer(remoteNode, StubBearer("noop-0x0002"))

        fun sendOnce(): Throwable? = runBlocking {
            runCatching {
                withTimeout(200.milliseconds) {
                    mgr.send(
                        message = ConfigCompositionDataGet(page = 0xFFu),
                        destination = remoteNode,
                    )
                }
            }.exceptionOrNull()
        }

        // 1차: ack 없이 timeout/cancel 로 throw — finally 가 busy-set 에서 dst 제거해야 한다.
        val first = sendOnce()
        assertTrue(
            "1차 send 는 ack 부재로 throw 돼야 한다(timeout/cancel). 실제: $first",
            first is TimeoutCancellationException || first is kotlinx.coroutines.CancellationException,
        )

        // 2차: 같은 dst 재시도 — busy-set 이 비었으면 Busy 가 아니라 다시 timeout 으로 빠진다.
        val second = sendOnce()
        assertFalse(
            "2차 send 가 Busy 면 busy-set 누수다(finally 미적용). 실제: $second",
            second is no.nordicsemi.kotlin.mesh.core.layers.access.Busy,
        )
        assertTrue(
            "2차 send 도 ack 부재로 timeout/cancel 이어야 한다(누수 0 → 정상 재진입). 실제: $second",
            second is TimeoutCancellationException || second is kotlinx.coroutines.CancellationException,
        )
    }
}
