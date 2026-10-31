package no.nordicsemi.kotlin.mesh.core.model

import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-31) — 마지막 저장 뒤 바뀐 노드 기록 (노드 단위 증분 저장용).
 *
 * 저장소가 저널을 쓸 수 있으면([no.nordicsemi.kotlin.mesh.core.JournalStorage]) 저장 1회는 망 전체가 아니라 껍데기(키·프로비저너·
 * 그룹·장면·제외 목록, 노드 없음) + 여기 적힌 노드만 쓴다. 노드 내부를 바꾸는 라이브러리 경로는 모두 [MeshNetwork.nodeChanged] 를
 * 부른다 (노드·엘리먼트·모델의 timestamp 갱신 자리 전부, 노드 추가, 장치 키 설정 응답의 출처 노드). 지움은 [nodeRemoved].
 * 껍데기는 저장마다 통째로 쓰므로 따로 적지 않는다.
 *
 * 재생 규칙 (저널 → 망, [no.nordicsemi.kotlin.mesh.core.model.serialization.CdbJournal]): 한 묶음 안에서 지움을 먼저, 그다음
 * [nodes] 를 처음 건드린 순서대로 — 있던 노드는 제자리에서 바꾸고 없던 노드는 끝에 붙인다. 메모리 망도 새 노드를 끝에 붙이므로
 * 순서가 같다 (지웠다 같은 UUID 로 다시 넣으면 둘 다 적혀 끝으로 간다).
 *
 * [full] = 저장 파일이 이 망을 대표하지 못한다 (새로 만들거나 들여온 망, 저널 없는 저장소에서 올린 망) → 다음 저장은 전체 스냅숏.
 * 저장소에서 저널로 올린 망만 [markClean] 으로 시작한다.
 */
@OptIn(ExperimentalUuidApi::class)
internal class NetworkChangeLog {

    /** 한 번에 가져간 변경. 저장이 실패하면 [restore] 로 되돌린다. */
    data class Changes(val full: Boolean, val nodes: List<Uuid>, val removed: Set<Uuid>) {
        val isEmpty: Boolean get() = !full && nodes.isEmpty() && removed.isEmpty()
    }

    private val lock = Any()
    private var full = true
    private val nodes = LinkedHashSet<Uuid>()
    private val removed = LinkedHashSet<Uuid>()

    fun nodeChanged(uuid: Uuid) {
        synchronized(lock) { nodes.add(uuid) }
    }

    fun nodeRemoved(uuid: Uuid) {
        synchronized(lock) {
            nodes.remove(uuid)
            removed.add(uuid)
        }
    }

    fun requireFull() {
        synchronized(lock) { full = true }
    }

    /** 저장 파일이 지금 망과 같다 (저널에서 막 올렸다). */
    fun markClean() {
        synchronized(lock) {
            full = false
            nodes.clear()
            removed.clear()
        }
    }

    fun take(): Changes = synchronized(lock) {
        Changes(full, nodes.toList(), LinkedHashSet(removed)).also {
            full = false
            nodes.clear()
            removed.clear()
        }
    }

    /**
     * 저장 실패 — 가져간 변경을 되돌린다. 순서: 실패한 묶음의 노드가 먼저 (메모리 망에 먼저 붙었다), 그 뒤 새로 적힌 노드.
     * 그 사이 지워진 노드는 실패한 묶음 쪽에서 빼고 (다시 넣었으면 새로 적힌 쪽 순서로 간다).
     */
    fun restore(changes: Changes) {
        synchronized(lock) {
            if (changes.full) full = true
            val merged = LinkedHashSet<Uuid>()
            changes.nodes.forEach { if (it !in removed) merged.add(it) }
            merged.addAll(nodes)
            nodes.clear()
            nodes.addAll(merged)
            removed.addAll(changes.removed)
        }
    }
}
