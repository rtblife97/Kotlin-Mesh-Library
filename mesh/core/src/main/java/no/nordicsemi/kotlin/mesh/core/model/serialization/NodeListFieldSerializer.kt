package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import no.nordicsemi.kotlin.mesh.core.model.Node

/**
 * simdo-fork (2026-10-26, #80) — `MeshNetwork._nodes`(JSON 키 `nodes`) 필드 직렬화기.
 *
 * 평소에는 기본 리스트 직렬화와 똑같다 (읽기·쓰기 모두 [ListSerializer] 에 넘긴다). 다른 점은 하나:
 * [withoutNodes] 블록 안에서는 노드 목록 대신 빈 배열을 낸다. 망 전체 저장을 "노드 목록 스냅샷 + 나머지(껍데기)" 와
 * "노드 하나씩" 으로 나눠 인코딩할 때, 껍데기는 생성된 MeshNetwork 직렬화기 그대로(키 순서·기본값·다른 필드 전부 같게)
 * 만들고 `nodes` 자리만 나중에 노드별 결과로 갈아 끼우기 위해서다 ([MeshNetworkSerializer]).
 *
 * 스위치는 스레드 지역 값이다 — 껍데기 인코딩은 한 스레드에서 동기로 끝나므로(중간에 suspend 없음) 다른 스레드의
 * 일반 직렬화에 새지 않는다.
 */
internal object NodeListFieldSerializer : KSerializer<MutableList<Node>> {

    private val delegate = ListSerializer(Node.serializer())

    private val skipNodes = ThreadLocal<Boolean>()

    override val descriptor: SerialDescriptor = delegate.descriptor

    override fun serialize(encoder: Encoder, value: MutableList<Node>) {
        delegate.serialize(encoder, if (skipNodes.get() == true) emptyList() else value)
    }

    override fun deserialize(decoder: Decoder): MutableList<Node> =
        delegate.deserialize(decoder).toMutableList()

    /** [block] 안의 직렬화는 `nodes` 를 빈 배열로 낸다. 중첩 호출은 안쪽이 끝나도 바깥 상태를 되돌린다. */
    fun <T> withoutNodes(block: () -> T): T {
        val previous = skipNodes.get()
        skipNodes.set(true)
        try {
            return block()
        } finally {
            skipNodes.set(previous)
        }
    }
}
