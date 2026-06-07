package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import java.util.concurrent.atomic.AtomicReference

/**
 * P6 M=2 회귀 가드 (simdo-fork, 2026-06-07 — config 병렬화 nodes-getter race).
 *
 * ## 배경
 *
 * config 병렬화(P6)는 N 노드를 동시에 config/provision 한다. 동시 완료 시 [MeshNetwork.add]/[remove]
 * 가 `_nodes` 를 구조적으로 변이하는 동안, 앱 consumer(예 `MeshNodeManager.startNetworkCollector`)가
 * [MeshNetwork.nodes] getter 를 iterate 하면 **ConcurrentModificationException** 이 난다
 * ([defect_meshnetwork_nodes_unsynchronized_cme_crash]). [cdbMutex] 는 suspend Mutex 라 일반 property
 * getter 에서 잡을 수 없으므로, getter 는 non-suspend `nodesMonitor` 아래에서 방어적 스냅샷을 반환한다.
 *
 * 본 테스트는 그 불변을 가드한다: 동시 add/remove + getter iterate 가 결함 0 이어야 한다. (getter 가
 * raw `_nodes` 를 노출하던 종전 구현이면 본 테스트가 CME 로 떨어진다 — fix 의 필요성 입증.)
 */
@OptIn(kotlin.uuid.ExperimentalUuidApi::class)
class NodesGetterConcurrencyTest {

    private companion object {
        const val WRITER_NODES = 64
        const val READER_ITERATIONS = 2000
    }

    /**
     * P6 production 경로 모사 — writer(config-RX/provisioning add) 는 `withCdbLock` 으로 직렬화되지만,
     * **앱 consumer(reader) 는 cdbMutex 를 모른 채** [MeshNetwork.nodes] getter 를 iterate 한다
     * (MeshNodeManager.startNetworkCollector onEach { nodes.map {} }). getter 가 라이브 `_nodes` 를
     * 노출하면 writer 의 구조 변경과 충돌해 CME → 본 fix 의 방어적 스냅샷이 그 race 를 봉인한다.
     *
     * (reader 가 cdbMutex 를 잡지 않는다는 점이 핵심 — 앱 레이어는 lib 의 lock 을 알 수 없다.)
     */
    @Test
    fun `withCdbLock writer 와 lock 없는 nodes getter reader 가 CME 0`() {
        val failure = AtomicReference<Throwable?>(null)

        repeat(5) { round ->
            if (failure.get() != null) return@repeat
            val network = MeshNetwork(name = "Nodes Getter Race $round").apply {
                add(name = "Primary Network Key", index = 0u)
            }

            runBlocking {
                try {
                    coroutineScope {
                        // writer: config-RX/provisioning 동형 — add/remove 를 withCdbLock 으로 직렬화.
                        val writers = (0 until WRITER_NODES).map { n ->
                            async(Dispatchers.Default) {
                                val node = Node(name = "Node $n", address = 1 + n, elements = 1)
                                network.withCdbLock { network.add(node = node) }
                                if (n % 3 == 0) network.withCdbLock { network.remove(uuid = node.uuid) }
                            }
                        }
                        // reader: 앱 consumer — cdbMutex 미보유로 nodes getter 만 iterate.
                        val readers = (0 until 4).map {
                            async(Dispatchers.Default) {
                                repeat(READER_ITERATIONS) {
                                    runCatching {
                                        // getter 스냅샷을 iterate — 라이브 리스트면 여기서 CME.
                                        network.nodes.forEach { node -> node.name.length }
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

        assertNull(
            "withCdbLock writer + lock 없는 nodes getter reader 는 CME 0 이어야 한다(방어적 스냅샷). 실제: ${failure.get()}",
            failure.get(),
        )
    }

    /**
     * 음성 대조군 — **Phase 3(2026-06-08) 이후 컴파일 타임으로 승격**.
     *
     * 종전엔 라이브 backing `_nodes` 를 런타임에 직접 iterate 해 CME 를 best-effort 로 관측함으로써
     * 방어적 스냅샷의 필요성을 입증했다(Heisenbug 라 assumeTrue skip 정책). Phase 3 에서 `_nodes` 를
     * `internal`→`private` 로 봉인하면서 **그 raw 경로가 test 소스셋에서도 컴파일 불가**가 됐다 —
     * 즉 "가드 우회"가 더 이상 런타임 race 가 아니라 **타입 시스템이 거부하는 컴파일 에러**다.
     * 이로써 스냅샷이 load-bearing 임을 비결정적 race 관측이 아니라 **결정적 컴파일 강제**로 입증한다.
     *
     * 본 테스트는 그 사실을 문서화하는 결정적 plumbing 가드다: consumer 가 쓸 수 있는 유일한 nodes
     * 접근면이 방어적 스냅샷([MeshNetwork.nodes])뿐임을 확인한다. (raw `_nodes` 를 적으면 이 파일이
     * 컴파일되지 않으므로, 회귀가 PR 단계에서 컴파일러에 의해 차단된다.)
     */
    @Test
    fun `_nodes 는 private 봉인 - consumer 접근면은 방어적 스냅샷뿐(컴파일 타임 강제)`() {
        val network = MeshNetwork(name = "Sealed Nodes").apply {
            add(name = "Primary Network Key", index = 0u)
            add(node = Node(name = "Node 0", address = 1, elements = 1))
        }
        // 유일하게 컴파일되는 nodes 접근면 = 방어적 스냅샷 getter. (`network._nodes` 를 적으면 컴파일 불가.)
        val snapshot = network.nodes
        assertEquals(1, snapshot.size)
        // 스냅샷은 라이브 뷰가 아니다 → 후속 add 가 기존 스냅샷을 변이하지 못함(CME 면역의 근거).
        network.add(node = Node(name = "Node 1", address = 2, elements = 1))
        assertEquals("방어적 스냅샷은 호출 시점 고정(라이브 _nodes 미노출)", 1, snapshot.size)
    }

    /**
     * getter 가 방어적 복사를 반환하는지(스냅샷 = 호출 시점 고정). 스냅샷을 받은 뒤 추가 add 가
     * 일어나도 그 스냅샷 크기는 변하지 않아야 한다(라이브 뷰면 변함). 동작 보존 회귀 가드.
     */
    @Test
    fun `nodes getter 는 호출 시점 방어적 스냅샷을 반환한다`() {
        val network = MeshNetwork(name = "Snapshot").apply {
            add(name = "Primary Network Key", index = 0u)
            add(node = Node(name = "Node 0", address = 1, elements = 1))
            add(node = Node(name = "Node 1", address = 2, elements = 1))
        }
        val snapshot = network.nodes
        assertEquals(2, snapshot.size)
        // 스냅샷을 받은 뒤 add — 스냅샷은 불변(라이브 뷰가 아님).
        network.add(node = Node(name = "Node 2", address = 3, elements = 1))
        assertEquals("스냅샷은 호출 시점에 고정되어야 한다(라이브 뷰 금지)", 2, snapshot.size)
        // 새 getter 호출은 최신 상태 반영.
        assertEquals(3, network.nodes.size)
    }
}
