package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
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
     * 음성 대조군(문서화) — 라이브 `_nodes` 를 직접 iterate(=종전 getter 동작)하면, 동시 구조 변경과
     * 충돌해 CME 가 발생함을 보인다. 이로써 getter 의 방어적 스냅샷이 load-bearing 임을 입증한다.
     * (race 는 타이밍 의존이라 반복해 적어도 1회 관측.)
     */
    @Test
    fun `라이브 _nodes 직접 iterate 는 CME 를 일으킨다 - 스냅샷의 필요성 입증`() {
        var observed: Throwable? = null
        repeat(10) {
            if (observed != null) return@repeat
            val network = MeshNetwork(name = "Live Nodes Race").apply {
                add(name = "Primary Network Key", index = 0u)
            }
            runBlocking {
                runCatching {
                    coroutineScope {
                        val writers = (0 until WRITER_NODES).map { n ->
                            async(Dispatchers.Default) {
                                network.add(node = Node(name = "Node $n", address = 1 + n, elements = 1))
                            }
                        }
                        val readers = (0 until 4).map {
                            async(Dispatchers.Default) {
                                repeat(READER_ITERATIONS) {
                                    runCatching {
                                        // 라이브 backing list 직접 iterate(종전 getter == _nodes).
                                        network._nodes.forEach { node -> node.name.length }
                                    }.onFailure { if (observed == null) observed = it }
                                }
                            }
                        }
                        (writers + readers).awaitAll()
                    }
                }.onFailure { if (observed == null) observed = it }
            }
        }
        assertTrue(
            "라이브 _nodes 직접 iterate 는 동시 구조 변경과 CME 를 일으켜야 한다(스냅샷 필요성 입증). 실제: $observed",
            observed is java.util.ConcurrentModificationException,
        )
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
