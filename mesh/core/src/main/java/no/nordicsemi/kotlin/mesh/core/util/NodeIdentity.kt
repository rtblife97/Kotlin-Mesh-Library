@file:Suppress("unused")

package no.nordicsemi.kotlin.mesh.core.util

import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.data.toHexString
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.crypto.Crypto
import java.util.IdentityHashMap

/**
 * The Node Identity contains information from Node Identity or Private Node Identity beacon.
 *
 * This can be used to identify a node in the network.
 *
 * @property hash    Function of the included random number and identity information.
 * @property random  64-bit random number.
 */
sealed interface NodeIdentity {
    val hash: ByteArray
    val random: ByteArray

    /**
     * Returns whether the identity matches given node.
     *
     * @param node Node to be matched.
     * @return true if the identity matches, false otherwise.
     */
    fun matches(node: Node): Boolean

    /**
     * Returns the first Node that matches the identity from the given list.
     *
     * simdo-fork (2026-10-26, #80) — 같은 IdentityKey 의 AES 키 스케줄을 이 호출 안에서 재사용한다 (종전: 노드마다
     * `Cipher.getInstance`). 망 하나의 노드는 거의 모두 같은 NetKey 라 키 스케줄은 보통 한 번이다. 결과는 종전과 같다.
     *
     * @param nodes List of Nodes to be matched.
     * @return The first matching Node.
     */
    fun matches(nodes: List<Node>): Node? {
        val hashers = IdentityHashMap<ByteArray, Crypto.IdentityHasher>()
        return nodes.firstOrNull { node ->
            matchesNode(node) { key -> hashers.getOrPut(key) { Crypto.IdentityHasher(key) } }
        }
    }

    /**
     * Returns the Node Identity as a hex string with hash and random concatenated.
     *
     * @return Node Identity in hex string format.
     */
    fun toHexString(): String {
        return hash.toHexString(
            format = HexFormat {
                number.prefix = "0x"
                upperCase = true
            }
        ) + random.toHexString(
            format = HexFormat.UpperCase
        )
    }
}

/**
 * Representation of Node Identity advertisement packet.
 *
 * @property hash    Function of the included random number and identity information.
 * @property random  64-bit random number.
 */
@ConsistentCopyVisibility
data class PublicNodeIdentity internal constructor(
    override val hash: ByteArray,
    override val random: ByteArray,
) : NodeIdentity {

    override fun matches(node: Node): Boolean = matchesNode(node) { Crypto.IdentityHasher(it) }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PublicNodeIdentity

        if (!hash.contentEquals(other.hash)) return false
        if (!random.contentEquals(other.random)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = hash.contentHashCode()
        result = 31 * result + random.contentHashCode()
        return result
    }
}

/**
 * Representation of Private Node Identity advertisement packet.
 *
 * @property hash    Function of the included random number and identity information.
 * @property random  64-bit random number.
 */
@ConsistentCopyVisibility
data class PrivateNodeIdentity internal constructor(
    override val hash: ByteArray,
    override val random: ByteArray,
) : NodeIdentity {
    override fun matches(node: Node): Boolean = matchesNode(node) { Crypto.IdentityHasher(it) }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PrivateNodeIdentity

        if (!hash.contentEquals(other.hash)) return false
        if (!random.contentEquals(other.random)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = hash.contentHashCode()
        result = 31 * result + random.contentHashCode()
        return result
    }
}

/**
 * simdo-fork (2026-10-26, #80) — 해시 입력 16바이트.
 *  - Node Identity: Padding(48비트 0) || Random || Address (Mesh Protocol 1.1 §7.2.2.2.3).
 *  - Private Node Identity: Padding(40비트 0) || 0x03 || Random || Address (§7.2.2.2.4). 종전 코드는 0x03 을 빠뜨려 15바이트였고,
 *    JCA ECB/NoPadding 이 블록 크기 오류로 예외를 던져 Private Node Identity 는 한 번도 맞춰지지 않았다. 공개 테스트 벡터로는
 *    확인하지 못했다 (스펙 식 기준).
 */
private fun NodeIdentity.hashInput(node: Node): ByteArray {
    val address = node.primaryUnicastAddress.address.toByteArray()
    return when (this) {
        is PublicNodeIdentity -> ByteArray(6) + random + address
        is PrivateNodeIdentity -> ByteArray(5) + byteArrayOf(0x03) + random + address
    }
}

/** [node] 가 아는 NetKey(와 키 갱신 중이면 옛 키) 의 IdentityKey 로 해시가 맞는가. [hasherFor] = IdentityKey → 계산기. */
private inline fun NodeIdentity.matchesNode(node: Node, hasherFor: (ByteArray) -> Crypto.IdentityHasher): Boolean {
    val data = hashInput(node)
    for (key in node.networkKeys) {
        if (hash.contentEquals(hasherFor(key.derivatives.identityKey).hash(data))) return true
        key.oldDerivatives?.identityKey?.let { old ->
            if (hash.contentEquals(hasherFor(old).hash(data))) return true
        }
    }
    return false
}

/**
 * Returns the Node Identity for a given Node Identity beacon.
 *
 * @receiver ByteArray Node Identity beacon.
 * @return NodeIdentity or null if the beacon is invalid.
 */
fun ByteArray.nodeIdentity() = when (size) {
    17 if get(0) == 0x01.toByte() -> PublicNodeIdentity(
        hash = sliceArray(1 until 9),
        random = sliceArray(9 until 17)
    )

    17 if get(0) == 0x03.toByte() -> PrivateNodeIdentity(
        hash = sliceArray(1 until 9),
        random = sliceArray(9 until 17)
    )

    else -> null
}