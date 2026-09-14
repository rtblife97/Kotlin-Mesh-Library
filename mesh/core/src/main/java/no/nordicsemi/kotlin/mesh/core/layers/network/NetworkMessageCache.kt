package no.nordicsemi.kotlin.mesh.core.layers.network

/**
 * Network Message Cache (Mesh Protocol 1.1 / Mesh Profile 1.0.1, Section 3.4.6.4).
 *
 * simdo-patch (2026-09-09): replaces `mutableMapOf<ByteArray, Any?>()` in [NetworkLayer].
 *
 * ## Why this exists
 *
 * The iOS library keys its cache with `NSData`, which compares by content. The Kotlin port
 * kept the same shape with `ByteArray`, whose `equals`/`hashCode` are identity based, so every
 * lookup missed (no deduplication ever happened) and, having no eviction, the map grew by one
 * entry per received PDU for the life of the process. Behind a GATT Proxy the bug was masked,
 * because the Proxy Server's own Network Message Cache had already removed duplicates. On an
 * advertising bearer, every Network Transmit repetition and every relay copy reaches the host.
 *
 * ## Two-stage design (mirrors Zephyr `subsys/bluetooth/mesh/net.c`)
 *
 * 1. **Raw PDU stage** ([isDuplicateRawPdu]) — keyed by the full wire bytes, checked *before*
 *    de-obfuscation/decryption. Catches byte-identical repeats: Network Transmit
 *    (Section 4.2.19) repetitions and Proxy re-forwarding. This is what the original
 *    iOS/Kotlin code intended and what Zephyr's `check_dup()` does. It cannot catch relay
 *    copies, because a relay decrements TTL, and TTL is part of the network nonce, so the
 *    ciphertext, NetMIC, and obfuscation of a relayed PDU all differ from the original.
 *
 * 2. **Source/Sequence stage** ([isDuplicateSequence]) — keyed by `(SRC, IV Index, SEQ)`,
 *    checked *after* successful decoding. Section 3.4.6.4 lets an implementation identify a
 *    cached Network PDU by a subset of its fields rather than the whole PDU; SRC + SEQ is the
 *    customary subset, and it is what Zephyr's `msg_cache_match()` uses (`src, seq, net_idx`).
 *    It is what suppresses relay copies and our own group messages relayed back to us. SEQ is
 *    unique per source element for a given IV Index (Section 3.8.8), and every transmission —
 *    including segment retransmissions and Segment Acknowledgments — uses a fresh SEQ, so a hit
 *    is never a legitimate new message.
 *
 * Note that the Lower Transport replay check ([no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportLayer])
 * only protects unicast destinations addressed to the local node; group/virtual destinations are
 * deliberately exempt. Stage 2 is therefore the only guard against duplicate delivery of
 * group-published messages (Sensor Status, vendor reports, Heartbeat) on an advertising bearer.
 *
 * ## Capacity
 *
 * The specification requires a minimum of two entries. Zephyr's `CONFIG_BT_MESH_MSG_CACHE_SIZE`
 * defaults to 32 for RAM-constrained nodes, with the Kconfig help text warning that a value
 * that is too small "can cause unnecessary network traffic". A host (phone/desktop) is not
 * RAM-constrained, and on a busy network the window between the first copy of a message and
 * its last relay copy can contain many distinct PDUs, so [DEFAULT_CAPACITY] is set to 256 —
 * eight times the Zephyr default. Worst-case footprint is ~256 x 29 bytes for the raw stage
 * plus small keys for the sequence stage; a few tens of kilobytes.
 *
 * Eviction is least-recently-used: a hit refreshes the entry, so a message that keeps being
 * relayed stays in the cache for as long as copies keep arriving.
 *
 * This class is thread-safe. [NetworkLayer.handle] is launched once per received PDU on the
 * manager's scope, so concurrent calls are expected.
 *
 * @property capacity Maximum number of entries per stage. Must be at least 2.
 */
internal class NetworkMessageCache(val capacity: Int = DEFAULT_CAPACITY) {

    init {
        require(capacity >= MIN_CAPACITY) {
            "Network Message Cache must hold at least $MIN_CAPACITY entries (Section 3.4.6.4)"
        }
    }

    /** Content-based key for raw PDU bytes. */
    private class RawPduKey(private val bytes: ByteArray) {
        private val hash = bytes.contentHashCode()
        override fun hashCode(): Int = hash
        override fun equals(other: Any?): Boolean =
            other is RawPduKey && hash == other.hash && bytes.contentEquals(other.bytes)
    }

    /** SRC + IV Index + SEQ of a decoded Network PDU. */
    private data class SequenceKey(val source: UShort, val ivIndex: UInt, val sequence: UInt)

    /** Bounded, access-ordered set with LRU eviction. Not thread-safe; callers hold [lock]. */
    private class LruSet<K>(private val capacity: Int) {
        private val map = object : LinkedHashMap<K, Unit>(capacity, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, Unit>?): Boolean =
                size > this@LruSet.capacity
        }

        /**
         * Inserts [key] if absent.
         *
         * @return `true` if [key] was already present (and its recency was refreshed).
         */
        fun markSeen(key: K): Boolean {
            // get() with accessOrder = true moves the entry to the tail (most recently used).
            if (map[key] != null) return true
            map[key] = Unit
            return false
        }

        val size: Int get() = map.size
        fun clear() = map.clear()
    }

    private val lock = Any()
    private val rawPdus = LruSet<RawPduKey>(capacity)
    private val sequences = LruSet<SequenceKey>(capacity)

    /**
     * Stage 1: records the raw wire bytes of a received PDU.
     *
     * The array is copied, so callers may reuse their buffer.
     *
     * @return `true` if a byte-identical PDU was seen recently and this one should be dropped.
     */
    fun isDuplicateRawPdu(pdu: ByteArray): Boolean = synchronized(lock) {
        rawPdus.markSeen(RawPduKey(pdu.copyOf()))
    }

    /**
     * Stage 2: records the `(SRC, IV Index, SEQ)` of a successfully decoded Network PDU.
     *
     * @return `true` if a PDU with the same source and sequence number was seen recently and
     *         this one should be dropped.
     */
    fun isDuplicateSequence(networkPdu: NetworkPdu): Boolean =
        isDuplicateSequence(
            source = networkPdu.source.address,
            ivIndex = networkPdu.ivIndex,
            sequence = networkPdu.sequence,
        )

    /** See [isDuplicateSequence]. Exposed for tests. */
    internal fun isDuplicateSequence(source: UShort, ivIndex: UInt, sequence: UInt): Boolean =
        synchronized(lock) {
            sequences.markSeen(SequenceKey(source, ivIndex, sequence))
        }

    /** Number of raw PDU entries currently cached. Exposed for tests. */
    internal val rawPduCount: Int get() = synchronized(lock) { rawPdus.size }

    /** Number of source/sequence entries currently cached. Exposed for tests. */
    internal val sequenceCount: Int get() = synchronized(lock) { sequences.size }

    /** Drops all entries. */
    fun clear() = synchronized(lock) {
        rawPdus.clear()
        sequences.clear()
    }

    companion object {
        /** Section 3.4.6.4: "shall contain a minimum of 2 entries". */
        const val MIN_CAPACITY = 2

        /** See the class documentation for the rationale. Zephyr default is 32. */
        const val DEFAULT_CAPACITY = 256
    }
}
