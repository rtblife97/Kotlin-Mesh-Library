package no.nordicsemi.kotlin.mesh.core.layers.access

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.serialization.MeshNetworkSerializer
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.NetworkConfiguration
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicReference

/**
 * Fork-3 회귀 가드 (simdo-fork, 2026-06-05 — mode2 병렬 config CDB mutation race).
 *
 * ## 배경
 *
 * 병렬 config (mode2) 에서 N 노드의 `Config*Status` 응답이 멀티스레드(`Dispatchers.Default`)로
 * 동시에 수신되면, lib 의 `handleResponses` 가 같은 [MeshNetwork] 의 `_nodes` 및 각 노드의
 * `_netKeys`/`_appKeys`/`_subscribe`/`_groups` (전부 일반 `mutableListOf` = ArrayList) 에
 * 동시에 write 한다. 동시에 autosave 경로의 `export()`=`serialize()` 가 같은 컬렉션을 순회하면:
 *
 *  - **ConcurrentModificationException** — 순회 중 구조 변경.
 *  - **lost update** — 두 `ArrayList.add` 가 같은 슬롯에 race → 최종 size < 기대.
 *
 * Fork-3 는 lib `NetworkManager.cdbMutex` (`AccessLayer.onMeshMessageReceived` RX 직렬화 +
 * `MeshNetworkManager.save()`/`withCdbLock` traversal 직렬화) 로 이 둘을 같은 lock 위에 직렬화한다.
 *
 * ## 본 테스트가 검증하는 것
 *
 * 본 테스트는 [AccessLayer]/`NetworkManager` 전체 harness (timer 구동·bearer) 없이, race 의
 * **본질**(다중 writer 의 CDB 컬렉션 동시 mutation + 동시 serialize 순회)을 직접 재현한다.
 * `handleResponses` 가 호출하는 것과 동일한 `internal` mutator (`addNetKey`/`addAppKey`,
 * `meshNetwork.add(node)`) 와 동일한 `MeshNetworkSerializer.serialize` 를 사용한다.
 *
 * `cdbMutex` 를 끼우지 않으면([noLock])  CME 또는 lost-update 가 재현되고, 끼우면([withLock])
 * 둘 다 사라진다. Fork-3 의 lock 적용 지점(AccessLayer RX + serialize)이 같은 단일 Mutex 를
 * 공유한다는 불변을 가드한다.
 */
@OptIn(kotlin.uuid.ExperimentalUuidApi::class)
class CdbMutationRaceTest {

    private companion object {
        const val WRITER_NODES = 64        // 동시 add 될 노드 수
        const val KEYS_PER_NODE = 32       // 노드당 동시 addNetKey/addAppKey 수
        const val SERIALIZE_ITERATIONS = 400
        const val PROVISION_NODES = 64     // P4: 동시 "provision add" 될 노드 수
    }

    /** handleResponses 와 동일한 internal mutator 로 노드에 키를 적재 (병렬). */
    private suspend fun mutateNode(node: Node, lock: Mutex?) = coroutineScope {
        (0 until KEYS_PER_NODE).map { i ->
            launch(Dispatchers.Default) {
                // 서로 다른 index → 매 add 가 실제 구조 변경(=lost update 가능).
                val run = suspend {
                    node.addNetKey(index = i.toUShort())
                    node.addAppKey(index = i.toUShort())
                }
                if (lock != null) lock.withLock { run() } else run()
            }
        }.forEach { it.join() }
    }

    /**
     * lock=null → fix 없음(재현). lock!=null → Fork-3 cdbMutex 모사(직렬화).
     *
     * @return 잡힌 throwable (CME 등) 또는 null. 정상 종료 시 lost-update 검증을 위해 network 도 본다.
     */
    private fun runRace(lock: Mutex?): Throwable? {
        val network = MeshNetwork(name = "Race Network").apply {
            // Node(name,address,elements) ctor 는 NodeKey(index=0u) 를 갖는다 → 네트워크에도
            // index 0 의 NetworkKey 가 있어야 add(node) 가 통과한다.
            add(name = "Primary Network Key", index = 0u)
        }
        val failure = AtomicReference<Throwable?>(null)

        runBlocking {
            try {
                coroutineScope {
                    // (A) writer: 노드 add + 노드별 키 mutation, 전부 Dispatchers.Default 병렬.
                    val writers = (0 until WRITER_NODES).map { n ->
                        async(Dispatchers.Default) {
                            val node = Node(
                                name = "Node $n",
                                address = (1 + n),
                                elements = 1,
                            )
                            // _nodes 컬렉션 구조 변경 (serialize 순회와 충돌 지점).
                            if (lock != null) lock.withLock { network.add(node = node) }
                            else network.add(node = node)
                            mutateNode(node = node, lock = lock)
                            node
                        }
                    }

                    // (B) reader: serialize 트래버설 반복 (autosave/export 경로 모사).
                    val readers = (0 until 4).map {
                        async(Dispatchers.Default) {
                            repeat(SERIALIZE_ITERATIONS) {
                                runCatching {
                                    val ser = {
                                        MeshNetworkSerializer.serialize(
                                            network = network,
                                            configuration = NetworkConfiguration.Full,
                                        )
                                    }
                                    if (lock != null) lock.withLock { ser() } else ser()
                                }.onFailure { failure.compareAndSet(null, it) }
                            }
                        }
                    }

                    (writers + readers).awaitAll()
                }
            } catch (t: Throwable) {
                failure.compareAndSet(null, t)
            }
        }

        // lost-update 검사: 모든 노드의 netKeys/appKeys 가 정확히 KEYS_PER_NODE 여야 한다.
        if (failure.get() == null) {
            network.nodes.forEach { node ->
                if (node.netKeys.size != KEYS_PER_NODE || node.appKeys.size != KEYS_PER_NODE) {
                    failure.compareAndSet(
                        null,
                        AssertionError(
                            "lost update: ${node.name} netKeys=${node.netKeys.size} " +
                                "appKeys=${node.appKeys.size} (expected $KEYS_PER_NODE)",
                        ),
                    )
                }
            }
            // _nodes 자체도 lost-update 검사.
            if (network.nodes.size != WRITER_NODES) {
                failure.compareAndSet(
                    null,
                    AssertionError("lost update: nodes=${network.nodes.size} expected $WRITER_NODES"),
                )
            }
        }
        return failure.get()
    }

    /**
     * 수정 전 재현 가드(문서화): cdbMutex 없이 동시 mutation + serialize 면 CME 또는 lost-update 가
     * 발생함을 보인다. race 는 타이밍 의존이라 단발에 항상 터지진 않으므로 여러 번 반복해 적어도
     * 1회 결함을 관측한다. (Fork-3 lock 의 필요성을 입증하는 음성 대조군.)
     */
    @Test
    fun `cdbMutex 없으면 동시 CDB mutation 과 serialize 가 CME 또는 lost-update 를 일으킨다`() {
        var observed: Throwable? = null
        repeat(10) {
            if (observed == null) observed = runRace(lock = null)
        }
        assertTrue(
            "cdbMutex 없는 동시 mutation+serialize 는 CME/lost-update 를 일으켜야 한다 " +
                "(관측 실패 시 race window 가 좁아진 것 — 반복 수를 늘려라). 실제: $observed",
            observed != null,
        )
    }

    /**
     * 수정 후: 같은 단일 Mutex(cdbMutex 모사) 로 mutation 과 serialize 를 직렬화하면 CME 도
     * lost-update 도 없다. 여러 라운드 반복해도 안정적으로 PASS.
     */
    @Test
    fun `cdbMutex 로 직렬화하면 CME 도 lost-update 도 없다`() {
        val cdbMutex = Mutex()
        repeat(10) { round ->
            val failure = runRace(lock = cdbMutex)
            assertEquals(
                "round=$round 에서 cdbMutex 직렬화에도 결함 발생: $failure",
                null,
                failure,
            )
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // P4 (simdo-fork, 2026-06-06) — 병렬 provisioning add race.
    // ProvisioningManager.kt:266 의 `meshNetwork.withCdbLock { remove(uuid); add(node) }` 가
    // config-RX 와 같은 lock(MeshNetwork.cdbMutex, P4 hoist)으로 직렬화되는지 가드한다. 위 테스트는
    // 로컬 Mutex 로 cdbMutex 를 *모사*했지만, 본 테스트는 **실제 network.withCdbLock / cdbMutex** 를
    // 직접 행사한다(hoisted lock 자체의 회귀 가드).
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * ProvisioningManager 의 add 경로를 모사 — N 노드를 동시에 remove(uuid)+add(node) 한다.
     * `useRealCdbLock=true` 면 실제 `network.withCdbLock` 으로 감싸고(=fix), false 면 raw(=재현).
     * reader 는 serialize 트래버설로 동시 순회한다.
     */
    private fun runProvisionRace(useRealCdbLock: Boolean): Throwable? {
        val network = MeshNetwork(name = "Provision Race Network").apply {
            add(name = "Primary Network Key", index = 0u)
        }
        val failure = AtomicReference<Throwable?>(null)

        runBlocking {
            try {
                coroutineScope {
                    val writers = (0 until PROVISION_NODES).map { n ->
                        async(Dispatchers.Default) {
                            val node = Node(name = "Node $n", address = (1 + n), elements = 1)
                            // ProvisioningManager 동형: remove(uuid) 후 add(node).
                            val provisionAdd = suspend {
                                network.remove(uuid = node.uuid)
                                network.add(node = node)
                            }
                            if (useRealCdbLock) network.withCdbLock { provisionAdd() }
                            else provisionAdd()
                        }
                    }
                    val readers = (0 until 4).map {
                        async(Dispatchers.Default) {
                            repeat(SERIALIZE_ITERATIONS) {
                                runCatching {
                                    val ser = {
                                        MeshNetworkSerializer.serialize(
                                            network = network,
                                            configuration = NetworkConfiguration.Full,
                                        )
                                    }
                                    // reader 도 같은 lock 으로 직렬화(config-RX serialize 모사).
                                    if (useRealCdbLock) network.withCdbLock { ser() } else ser()
                                }.onFailure { failure.compareAndSet(null, it) }
                            }
                        }
                    }
                    (writers + readers).awaitAll()
                }
            } catch (t: Throwable) {
                failure.compareAndSet(null, t)
            }
        }

        // lost-update: 동시 add 된 노드가 전부 살아 있어야 한다(각 주소 unique).
        if (failure.get() == null && network.nodes.size != PROVISION_NODES) {
            failure.compareAndSet(
                null,
                AssertionError("provision lost update: nodes=${network.nodes.size} expected $PROVISION_NODES"),
            )
        }
        return failure.get()
    }

    /**
     * P4 재현(음성 대조군): 실제 cdbMutex 없이 N 노드 동시 provision-add + serialize → CME/lost-update.
     * mutation-check 역할도 겸한다 — `withCdbLock` 을 무력화하면(=useRealCdbLock=false) 결함이 산다.
     */
    @Test
    fun `P4 - cdbMutex 없이 동시 provisioning add 면 CME 또는 lost-update`() {
        var observed: Throwable? = null
        repeat(10) {
            if (observed == null) observed = runProvisionRace(useRealCdbLock = false)
        }
        assertTrue(
            "lock 없는 동시 provisioning add+serialize 는 CME/lost-update 를 일으켜야 한다. 실제: $observed",
            observed != null,
        )
    }

    /**
     * P4 fix: 실제 `network.withCdbLock`(= MeshNetwork.cdbMutex, config-RX 가 위임하는 그 lock)으로
     * provisioning add 와 serialize 를 직렬화하면 여러 라운드 반복해도 CME/lost-update 0.
     */
    @Test
    fun `P4 - 실제 withCdbLock 으로 동시 provisioning add 직렬화하면 결함 0`() {
        repeat(10) { round ->
            val failure = runProvisionRace(useRealCdbLock = true)
            assertEquals("round=$round 에서 withCdbLock 직렬화에도 결함: $failure", null, failure)
        }
    }

    /**
     * 직렬(1건 in-flight) 경로 동작 보존 회귀 가드 — 현재 ship 되는 직렬 config 에선 lock 이
     * 항상 즉시 획득되고 결과가 lock 없는 단일 스레드 mutation 과 동일해야 한다.
     */
    @Test
    fun `직렬 단일 writer 경로는 lock 유무와 무관하게 동일 결과(동작 보존)`() {
        fun buildSerially(lock: Mutex?): MeshNetwork {
            val network = MeshNetwork(name = "Serial Network").apply {
                add(name = "Primary Network Key", index = 0u)
            }
            runBlocking {
                repeat(4) { n ->
                    val node = Node(name = "Node $n", address = 1 + n, elements = 1)
                    val add = { network.add(node = node) }
                    if (lock != null) lock.withLock { add() } else add()
                    repeat(KEYS_PER_NODE) { i ->
                        val m = {
                            node.addNetKey(index = i.toUShort())
                            node.addAppKey(index = i.toUShort())
                        }
                        if (lock != null) lock.withLock { m() } else m()
                    }
                }
            }
            return network
        }

        val withoutLock = buildSerially(lock = null)
        val withLock = buildSerially(lock = Mutex())

        assertEquals(withoutLock.nodes.size, withLock.nodes.size)
        withoutLock.nodes.zip(withLock.nodes).forEach { (a, b) ->
            assertEquals("netKeys 동일", a.netKeys.size, b.netKeys.size)
            assertEquals("appKeys 동일", a.appKeys.size, b.appKeys.size)
        }
        assertEquals(4, withLock.nodes.size)
        withLock.nodes.forEach {
            assertEquals(KEYS_PER_NODE, it.netKeys.size)
            assertEquals(KEYS_PER_NODE, it.appKeys.size)
        }
    }
}
