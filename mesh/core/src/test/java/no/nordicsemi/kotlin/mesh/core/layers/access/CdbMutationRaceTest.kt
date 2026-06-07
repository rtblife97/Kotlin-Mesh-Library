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
import org.junit.Assume.assumeTrue
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
        // dual-lock 갭 (2026-06-08, B-F1/F2/F3/F5)
        const val DUAL_WRITER_NODES = 64
        const val DUAL_SERIALIZE_ITERATIONS = 600
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
     * 수정 전 재현 가드(음성 대조군, 문서화): cdbMutex 없이 동시 mutation + serialize 면 CME 또는
     * lost-update 가 발생함을 **best-effort** 로 보인다(Fork-3 lock 의 필요성 입증).
     *
     * ## ★CI 결정성 — race 는 비결정적(Heisenbug)이라 hard 단언 금지
     *
     * 결함 발생은 타이밍·코어수·JIT 의존이라 단발에 항상 터지진 않는다. dev 머신에선 거의 매번
     * 관측되나 CI(다른 환경)에선 그 반복 윈도우에 안 날 수 있어 hard 게이트로는 본질적으로 flaky.
     * 정책: 여러 라운드 best-effort → 한 번이라도 결함 관측 시 통과, 끝까지 안 나면 [assumeTrue]
     * 로 skip(fail 아님). lock 의 실제 효력은 결정적 positive 테스트
     * (`cdbMutex 로 직렬화하면 CME 도 lost-update 도 없다`)가 hard 게이트로 검증한다.
     */
    @Test
    fun `cdbMutex 없으면 동시 CDB mutation 과 serialize 가 CME 또는 lost-update 를 일으킨다(best-effort)`() {
        var observed: Throwable? = null
        repeat(20) {
            if (observed == null) observed = runRace(lock = null)
        }
        assumeTrue(
            "cdbMutex 없는 동시 mutation+serialize race window 가 이 환경에서 안 열림(결함 미관측) — " +
                "CI 결정성 위해 skip. lock 효력 검증은 positive 직렬화 테스트가 담당. 실제: $observed",
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
     * dual-lock fix(2026-06-08, B-F1/F2/F3/F5) **이후** 갱신된 불변 — `_nodes` 만 다루는
     * provision-add(remove+add)+serialize 는 **cdbMutex 없이도** CME/lost-update 0 이다.
     *
     * ## 왜 이 테스트가 "재현 음성 대조군"에서 "불변 가드"로 바뀌었나
     *
     * 종전(dual-lock fix 전)엔 [MeshNetwork.add]/[remove] 가 insert/remove 한 줄만 nodesMonitor 로
     * 감쌌고 serialize 는 backing `_nodes` 를 raw 순회했다 → cdbMutex 가 없으면 `_nodes` 컨테이너에서
     * CME/lost-update 가 났다(그래서 이 테스트가 `useRealCdbLock=false` 로 재현했다).
     *
     * dual-lock fix 가 `_nodes` 컨테이너 무결성의 **단일 guardian = nodesMonitor** 로 수렴시킨 뒤로는,
     * add/remove(precheck 포함)·serialize 가 전부 nodesMonitor 아래라 **cdbMutex 유무와 무관하게**
     * `_nodes` 레벨 결함이 0 이다. 즉 cdbMutex 는 더 이상 `_nodes` 컨테이너를 지키는 lock 이 아니라,
     * 여러 CDB collection 에 걸친 **고수준 트랜잭션 ordering**(예 _appKeys/_subscribe 동시 mutation —
     * `cdbMutex 없으면 ...` 테스트가 그쪽을 여전히 재현) 전용이다.
     *
     * mutation-check 는 dual-lock 섹션의 `withNodesLock 우회한 raw _nodes 순회는 CME` 가 담당한다
     * (nodesMonitor 가드를 우회하면 backing list 가 여전히 CME-prone 임을 입증).
     */
    @Test
    fun `dual-lock 이후 - _nodes-only provision race 는 cdbMutex 없이도 결함 0`() {
        repeat(10) { round ->
            val failure = runProvisionRace(useRealCdbLock = false)
            assertEquals(
                "round=$round — dual-lock fix 후 _nodes add/remove/serialize 는 nodesMonitor 단일 " +
                    "guardian 으로 cdbMutex 없이도 CME/lost-update 0 이어야 한다. 실제: $failure",
                null,
                failure,
            )
        }
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

    // ─────────────────────────────────────────────────────────────────────────────
    // dual-lock 갭 (simdo-fork, 2026-06-08 — B-F1/F2/F3/F5). 코드리뷰 R1.
    //
    // `_nodes` 는 nodesMonitor(synchronized) + cdbMutex(suspend) 두 disjoint lock 으로 보호됐으나
    // 상호배제하지 못했다. 위 Fork-3/P4 테스트는 **writer 와 serialize reader 둘 다 같은 cdbMutex 를
    // 잡아** de-facto 직렬화돼 갭을 마스킹했다. 실제 노출 경로(node-delete UX / force-remove / 직접
    // export)는 cdbMutex 를 잡지 않은 채 nodesMonitor-only [add]/[remove] 가 cdbMutex-없는 serialize
    // 트래버설과 동시에 도는데, 이 둘이 서로 block 안 해 CME 가 재발한다.
    //
    // fix: `_nodes` 컨테이너 무결성의 단일 guardian = nodesMonitor 로 수렴. [add]/[remove]/node lookup/
    // isAddress*/serialize 전부 nodesMonitor 아래. 본 테스트는 **어느 쪽도 cdbMutex 를 잡지 않은 채**
    // 동시 mutation + serialize 를 돌려 그 수렴을 가드한다(위 테스트가 못 덮는 cross-lock 경로).
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * dual-lock 갭 positive 가드 — **cdbMutex 없이** nodesMonitor-only [add]/[remove] 가
     * cdbMutex-없는 [MeshNetworkSerializer.serialize] 트래버설과 동시에 돌아도 CME 0.
     *
     * (production 노출 경로 모사: node-delete UX/force-remove 는 cdbMutex 를 안 잡고 [remove] 만 호출,
     * autosave/export serialize 도 동시 진행. fix 전이면 serialize 가 backing `_nodes` 를 raw 순회하다
     * 동시 구조 변이와 CME → fix 후 serialize 가 withNodesLock 으로 감싸 0.)
     */
    @Test
    fun `dual-lock - cdbMutex 없는 add_remove 와 cdbMutex 없는 serialize 동시도 CME 0`() {
        val failure = AtomicReference<Throwable?>(null)

        repeat(5) { round ->
            if (failure.get() != null) return@repeat
            val network = MeshNetwork(name = "Dual Lock Race $round").apply {
                add(name = "Primary Network Key", index = 0u)
            }
            runBlocking {
                try {
                    coroutineScope {
                        // writer: cdbMutex 미보유 — nodesMonitor-only add/remove (force-delete 동형).
                        val writers = (0 until DUAL_WRITER_NODES).map { n ->
                            async(Dispatchers.Default) {
                                val node = Node(name = "Node $n", address = 1 + n, elements = 1)
                                network.add(node = node)
                                if (n % 2 == 0) network.remove(uuid = node.uuid)
                            }
                        }
                        // reader: cdbMutex 미보유 — serialize 트래버설(autosave/export 동형).
                        val readers = (0 until 4).map {
                            async(Dispatchers.Default) {
                                repeat(DUAL_SERIALIZE_ITERATIONS) {
                                    runCatching {
                                        MeshNetworkSerializer.serialize(
                                            network = network,
                                            configuration = NetworkConfiguration.Full,
                                        )
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
        }

        assertEquals(
            "nodesMonitor 단일 guardian — cdbMutex 없는 add/remove + cdbMutex 없는 serialize 는 CME 0 " +
                "이어야 한다(serialize 가 withNodesLock 으로 감싸짐). 실제: ${failure.get()}",
            null,
            failure.get(),
        )
    }

    /**
     * 음성 대조군(mutation-check) — withNodesLock 를 우회해 backing `_nodes` 를 **직접** raw 순회하면
     * (= fix 전 serialize 가 backing list 를 raw iterate 하던 동작), 동시 구조 변이와 CME 가 난다.
     * 이로써 serialize 의 withNodesLock 감싸기가 load-bearing 임을 **best-effort** 로 입증한다.
     *
     * ## ★CI 결정성 — race 는 비결정적(Heisenbug)이라 hard 단언 금지
     *
     * CME 발생은 타이밍·코어수·JIT 의존이라 CI 환경에선 그 반복 윈도우에 안 날 수 있다. 따라서
     * 여러 라운드 best-effort 시도 → 한 번이라도 CME 관측 시 통과, 끝까지 안 나면 [assumeTrue] 로
     * skip(fail 아님). withNodesLock 감싸기의 실제 효력은 결정적 positive 테스트
     * (`dual-lock - cdbMutex 없는 add_remove 와 cdbMutex 없는 serialize 동시도 CME 0`)가 담당한다.
     */
    @Test
    fun `dual-lock - withNodesLock 우회한 raw _nodes 순회는 CME 를 일으킨다(필요성 입증, best-effort)`() {
        var observed: Throwable? = null
        repeat(20) {
            if (observed != null) return@repeat
            val network = MeshNetwork(name = "Dual Lock Negative").apply {
                add(name = "Primary Network Key", index = 0u)
            }
            runBlocking {
                runCatching {
                    coroutineScope {
                        val writers = (0 until DUAL_WRITER_NODES).map { n ->
                            async(Dispatchers.Default) {
                                network.add(node = Node(name = "Node $n", address = 1 + n, elements = 1))
                            }
                        }
                        val readers = (0 until 4).map {
                            async(Dispatchers.Default) {
                                repeat(DUAL_SERIALIZE_ITERATIONS) {
                                    runCatching {
                                        // backing list 직접 raw 순회 = fix 전 serialize 의 _nodes 트래버설.
                                        network._nodes.forEach { node -> node.uuid.hashCode() }
                                    }.onFailure { if (observed == null) observed = it }
                                }
                            }
                        }
                        (writers + readers).awaitAll()
                    }
                }.onFailure { if (observed == null) observed = it }
            }
        }
        // 관측되면 가드 필요성 입증(통과). 안 나면 CI 환경차로 race window 미개방 → skip(fail 아님).
        assumeTrue(
            "withNodesLock 우회 raw _nodes 순회 race window 가 이 환경에서 안 열림(CME 미관측) — " +
                "CI 결정성 위해 skip. withNodesLock 효력 검증은 positive CME-0 테스트가 담당. 실제: $observed",
            observed is java.util.ConcurrentModificationException,
        )
    }

    /**
     * find-then-remove 원자성 가드 — 같은 uuid 의 [remove] 를 다중 스레드가 동시에 호출해도 결함 0이고,
     * node lookup([MeshNetwork.node])도 동시에 안전하다(전부 nodesMonitor 아래 _nodes 직접 접근).
     */
    @Test
    fun `dual-lock - 동시 remove(uuid)+node lookup 이 원자적이고 CME 0`() {
        val failure = AtomicReference<Throwable?>(null)
        repeat(5) { round ->
            if (failure.get() != null) return@repeat
            val network = MeshNetwork(name = "Atomic Remove $round").apply {
                add(name = "Primary Network Key", index = 0u)
            }
            val nodes = (0 until DUAL_WRITER_NODES).map { n ->
                Node(name = "Node $n", address = 1 + n, elements = 1).also { network.add(node = it) }
            }
            runBlocking {
                try {
                    coroutineScope {
                        // 각 노드마다 2 스레드가 같은 uuid 로 동시 remove → find-then-remove 비원자면 race.
                        val removers = nodes.flatMap { node ->
                            (0 until 2).map {
                                async(Dispatchers.Default) {
                                    runCatching { network.remove(uuid = node.uuid) }
                                        .onFailure { failure.compareAndSet(null, it) }
                                }
                            }
                        }
                        // 동시 lookup — nodesMonitor 아래 _nodes 직접 find.
                        val lookups = (0 until 4).map {
                            async(Dispatchers.Default) {
                                repeat(2000) {
                                    runCatching { nodes.forEach { network.node(uuid = it.uuid) } }
                                        .onFailure { failure.compareAndSet(null, it) }
                                }
                            }
                        }
                        (removers + lookups).awaitAll()
                    }
                } catch (t: Throwable) {
                    failure.compareAndSet(null, t)
                }
            }
            // 모든 노드가 정확히 제거됐는지(local provisioner 제외, 또는 본 테스트는 provisioner 없음 → 0).
            if (failure.get() == null && network.nodes.isNotEmpty()) {
                failure.compareAndSet(
                    null,
                    AssertionError("동시 중복 remove 후 잔존 노드: ${network.nodes.size} (기대 0)"),
                )
            }
        }
        assertEquals(
            "동시 remove(uuid)+lookup 은 원자적·CME 0 이어야 한다. 실제: ${failure.get()}",
            null,
            failure.get(),
        )
    }
}
