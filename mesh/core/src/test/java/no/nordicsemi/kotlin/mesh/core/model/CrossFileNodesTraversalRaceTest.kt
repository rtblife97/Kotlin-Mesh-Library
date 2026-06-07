package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertNull
import org.junit.Test
import java.util.concurrent.atomic.AtomicReference
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * Phase 3 회귀 가드 (simdo-fork, 2026-06-08 — `_nodes` private 봉인의 load-bearing 입증).
 *
 * ## 배경
 *
 * Phase 3 는 [MeshNetwork] 의 backing `_nodes` 를 `internal`→`private` 로 봉인해, `_nodes` 컨테이너
 * 무결성의 단일 guardian([MeshNetwork] 의 `nodesMonitor`)을 **타입 시스템(컴파일러)으로 강제**한다.
 * 그 과정에서 종전 `MeshNetwork` 밖(다른 파일)에서 raw `_nodes` 를 순회하던 3종 cross-file accessor 를
 * guarded 경로로 교체했다:
 *
 *  - [Group.nodes] / [Group.elements] / `Group.isUsed` — `network?._nodes` → `network?.nodes`(방어적 스냅샷)
 *  - [Scene.nodes] / [Scene.elements] — 동일
 *  - `Provisioner.node` — `network?._nodes?.find` → `network?.node(uuid)`([MeshNetwork] guarded lookup)
 *
 * 봉인이 단순히 버그를 Group/Scene/Provisioner 로 옮긴 게 아니라 **실제로 race 를 닫았음**을 입증한다:
 * 동시 [MeshNetwork.add]/[MeshNetwork.remove] 가 `_nodes` 를 구조적으로 변이하는 동안 cross-file
 * accessor 를 반복 호출해도 ConcurrentModificationException(CME) 0 이어야 한다. (교체 전 라이브 `_nodes`
 * 를 노출하던 구현이면 이 traversal 들이 writer 의 구조 변경과 충돌해 CME 로 떨어진다.)
 *
 * race 는 비결정적이지만 이 테스트는 **결정적 positive 가드**다 — guarded 스냅샷은 writer 와 무관하게
 * 항상 CME 0 이므로(스냅샷 = 호출 시점 고정 복사), 타이밍에 의존하지 않는다. 따라서 hard 게이트.
 */
@OptIn(ExperimentalUuidApi::class)
class CrossFileNodesTraversalRaceTest {

    private companion object {
        const val WRITER_NODES = 64
        const val READER_ITERATIONS = 2000
        const val ROUNDS = 5
    }

    /**
     * Group/Scene 의 cross-file traversal(이제 `network.nodes` 스냅샷 경유)이 동시 add/remove 와
     * CME 0 임을 보인다. writer 는 production 동형으로 `withCdbLock` 직렬화하지만, **reader 는 lock 을
     * 모른 채** Group/Scene accessor 만 호출한다(앱 consumer = lib 의 lock 을 알 수 없음).
     */
    @Test
    fun `Group_Scene cross-file traversal 이 동시 add_remove 와 CME 0`() {
        val failure = AtomicReference<Throwable?>(null)

        repeat(ROUNDS) { round ->
            if (failure.get() != null) return@repeat
            val network = MeshNetwork(name = "Cross-File Race $round").apply {
                add(name = "Primary Network Key", index = 0u)
            }
            val group = Group(_name = "Group $round", address = GroupAddress(0xC000u))
            network.add(group = group)
            val scene = network.add(name = "Scene $round", number = 1u)

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
                        // reader: 앱 consumer — cdbMutex 미보유로 cross-file accessor 만 호출.
                        // (guarded 스냅샷 경유면 CME 0, 라이브 `_nodes` 를 노출하던 종전이면 여기서 CME.)
                        val readers = (0 until 4).map {
                            async(Dispatchers.Default) {
                                repeat(READER_ITERATIONS) {
                                    runCatching {
                                        group.isUsed
                                        group.nodes().size
                                        group.elements().size
                                        scene.nodes().size
                                        scene.elements().size
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
            "Group/Scene cross-file traversal(network.nodes 스냅샷 경유)은 동시 add/remove 와 CME 0 " +
                "이어야 한다(`_nodes` private 봉인의 load-bearing 입증). 실제: ${failure.get()}",
            failure.get(),
        )
    }

    /**
     * `Provisioner.node`(이제 [MeshNetwork.node] guarded lookup 경유)가 동시 add/remove 와 CME 0.
     * provisioner uuid 가 어떤 노드와도 매칭 안 돼 null 이 반환되더라도, lookup traversal 자체가
     * nodesMonitor 아래 `_nodes` find 를 타므로 writer 의 구조 변경과 충돌하지 않아야 한다.
     */
    @Test
    fun `Provisioner_node lookup 이 동시 add_remove 와 CME 0`() {
        val failure = AtomicReference<Throwable?>(null)

        repeat(ROUNDS) { round ->
            if (failure.get() != null) return@repeat
            val network = MeshNetwork(name = "Provisioner Lookup Race $round").apply {
                add(name = "Primary Network Key", index = 0u)
            }
            val provisioner = Provisioner(Uuid.random()).apply {
                // add(provisioner) 가 자기 주소를 할당하려면 unicast 범위가 필요. (provisioner self-node 가
                // _nodes 에 추가되며, 그 uuid 매칭으로 provisioner.node 가 실제 노드를 찾는 경로도 탄다.)
                allocate(UnicastAddress(1000u)..UnicastAddress(1100u))
            }
            network.add(provisioner = provisioner)

            runBlocking {
                try {
                    coroutineScope {
                        val writers = (0 until WRITER_NODES).map { n ->
                            async(Dispatchers.Default) {
                                val node = Node(name = "Node $n", address = 1 + n, elements = 1)
                                network.withCdbLock { network.add(node = node) }
                                if (n % 3 == 0) network.withCdbLock { network.remove(uuid = node.uuid) }
                            }
                        }
                        val readers = (0 until 4).map {
                            async(Dispatchers.Default) {
                                repeat(READER_ITERATIONS) {
                                    runCatching {
                                        // guarded lookup — nodesMonitor 아래 _nodes find.
                                        provisioner.node
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
            "Provisioner.node(MeshNetwork.node guarded lookup 경유)는 동시 add/remove 와 CME 0 " +
                "이어야 한다(`_nodes` private 봉인의 load-bearing 입증). 실제: ${failure.get()}",
            failure.get(),
        )
    }
}
