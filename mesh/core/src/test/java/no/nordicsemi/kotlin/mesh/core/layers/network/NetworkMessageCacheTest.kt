package no.nordicsemi.kotlin.mesh.core.layers.network

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * [NetworkMessageCache] 단위 회귀 가드 (simdo-patch, 2026-09-09).
 *
 * 종전 `mutableMapOf<ByteArray, Any?>()` 는 identity 키라 (a) 같은 내용의 새 인스턴스가 영원히 miss,
 * (b) 제거 로직 부재로 무한 증가. 본 테스트는 내용 기반 키·상한·LRU 갱신·(SRC, IV, SEQ) 2단계를 고정한다.
 */
class NetworkMessageCacheTest {

    private fun bytes(vararg v: Int) = ByteArray(v.size) { v[it].toByte() }

    // ---- stage 1: raw PDU ---------------------------------------------------------------

    @Test
    fun `same content in a fresh ByteArray instance is a duplicate`() {
        val cache = NetworkMessageCache(capacity = 8)
        assertFalse("first sight passes", cache.isDuplicateRawPdu(bytes(1, 2, 3, 4)))
        // 새 인스턴스 — 종전 identity 키에서는 miss 였던 케이스.
        assertTrue("second sight (new instance) drops", cache.isDuplicateRawPdu(bytes(1, 2, 3, 4)))
        assertEquals(1, cache.rawPduCount)
    }

    @Test
    fun `different content passes`() {
        val cache = NetworkMessageCache(capacity = 8)
        assertFalse(cache.isDuplicateRawPdu(bytes(1, 2, 3, 4)))
        assertFalse("one byte differs", cache.isDuplicateRawPdu(bytes(1, 2, 3, 5)))
        assertFalse("length differs", cache.isDuplicateRawPdu(bytes(1, 2, 3)))
        assertEquals(3, cache.rawPduCount)
    }

    @Test
    fun `caller buffer mutation after insert does not corrupt the key`() {
        val cache = NetworkMessageCache(capacity = 8)
        val buffer = bytes(9, 9, 9)
        assertFalse(cache.isDuplicateRawPdu(buffer))
        buffer[0] = 0
        assertTrue("original content still cached", cache.isDuplicateRawPdu(bytes(9, 9, 9)))
        assertFalse("mutated content is new", cache.isDuplicateRawPdu(bytes(0, 9, 9)))
    }

    @Test
    fun `exceeding capacity evicts the oldest entry`() {
        val cache = NetworkMessageCache(capacity = 2)
        assertFalse(cache.isDuplicateRawPdu(bytes(1)))
        assertFalse(cache.isDuplicateRawPdu(bytes(2)))
        assertFalse(cache.isDuplicateRawPdu(bytes(3))) // evicts [1]
        assertEquals("bounded at capacity", 2, cache.rawPduCount)
        assertFalse("evicted [1] is seen as new again", cache.isDuplicateRawPdu(bytes(1)))
        assertTrue("[3] still cached", cache.isDuplicateRawPdu(bytes(3)))
        assertEquals(2, cache.rawPduCount)
    }

    @Test
    fun `a hit refreshes recency (LRU, not FIFO)`() {
        val cache = NetworkMessageCache(capacity = 2)
        assertFalse(cache.isDuplicateRawPdu(bytes(1)))
        assertFalse(cache.isDuplicateRawPdu(bytes(2)))
        assertTrue(cache.isDuplicateRawPdu(bytes(1)))  // [1] becomes most recent
        assertFalse(cache.isDuplicateRawPdu(bytes(3))) // evicts [2], not [1]
        assertTrue("[1] survived because it was hit", cache.isDuplicateRawPdu(bytes(1)))
        assertFalse("[2] was evicted", cache.isDuplicateRawPdu(bytes(2)))
    }

    @Test
    fun `stays bounded under sustained distinct traffic`() {
        val cache = NetworkMessageCache(capacity = 32)
        repeat(10_000) { i ->
            assertFalse(cache.isDuplicateRawPdu(bytes(i and 0xFF, (i shr 8) and 0xFF, (i shr 16) and 0xFF)))
        }
        assertEquals(32, cache.rawPduCount)
    }

    // ---- stage 2: SRC + IV Index + SEQ ------------------------------------------------------

    @Test
    fun `same source, iv index and sequence is a duplicate`() {
        val cache = NetworkMessageCache(capacity = 8)
        assertFalse(cache.isDuplicateSequence(source = 0x0002u, ivIndex = 0u, sequence = 100u))
        assertTrue(cache.isDuplicateSequence(source = 0x0002u, ivIndex = 0u, sequence = 100u))
        assertEquals(1, cache.sequenceCount)
    }

    @Test
    fun `sequence key discriminates on every field`() {
        val cache = NetworkMessageCache(capacity = 8)
        assertFalse(cache.isDuplicateSequence(source = 0x0002u, ivIndex = 0u, sequence = 100u))
        assertFalse("next seq", cache.isDuplicateSequence(source = 0x0002u, ivIndex = 0u, sequence = 101u))
        assertFalse("other src", cache.isDuplicateSequence(source = 0x0003u, ivIndex = 0u, sequence = 100u))
        assertFalse("next iv", cache.isDuplicateSequence(source = 0x0002u, ivIndex = 1u, sequence = 100u))
        assertEquals(4, cache.sequenceCount)
    }

    @Test
    fun `sequence stage is bounded independently of raw stage`() {
        val cache = NetworkMessageCache(capacity = 2)
        assertFalse(cache.isDuplicateSequence(0x0002u, 0u, 1u))
        assertFalse(cache.isDuplicateSequence(0x0002u, 0u, 2u))
        assertFalse(cache.isDuplicateSequence(0x0002u, 0u, 3u))
        assertEquals(2, cache.sequenceCount)
        assertEquals(0, cache.rawPduCount)
        assertFalse("seq 1 evicted", cache.isDuplicateSequence(0x0002u, 0u, 1u))
    }

    // ---- misc -------------------------------------------------------------------------------

    @Test
    fun `clear empties both stages`() {
        val cache = NetworkMessageCache(capacity = 8)
        cache.isDuplicateRawPdu(bytes(1))
        cache.isDuplicateSequence(0x0002u, 0u, 1u)
        cache.clear()
        assertEquals(0, cache.rawPduCount)
        assertEquals(0, cache.sequenceCount)
        assertFalse(cache.isDuplicateRawPdu(bytes(1)))
        assertFalse(cache.isDuplicateSequence(0x0002u, 0u, 1u))
    }

    @Test
    fun `capacity below the spec minimum is rejected`() {
        assertThrows(IllegalArgumentException::class.java) { NetworkMessageCache(capacity = 1) }
        NetworkMessageCache(capacity = NetworkMessageCache.MIN_CAPACITY) // 2 is allowed
    }

    @Test
    fun `default capacity is at least the Zephyr default`() {
        // Zephyr CONFIG_BT_MESH_MSG_CACHE_SIZE default = 32 (constrained node). Host keeps more.
        assertTrue(NetworkMessageCache().capacity >= 32)
    }
}
