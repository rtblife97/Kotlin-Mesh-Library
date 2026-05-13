package no.nordicsemi.kotlin.mesh.core

import no.nordicsemi.kotlin.mesh.core.messages.MeshMessage
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

/**
 * simdo-patch verification.
 *
 * Asserts that [NetworkEvent.MeshMessageReceived] exposes the RX metadata
 * (`sequence`, `ivIndex`, `ttl`) that simdo cloud sync requires for idempotent
 * event aggregation. Without this patch the upstream 1.0.0 data class only carries
 * source/destination/message, so `core:neomesh` cannot persist a seq watermark
 * per source address.
 *
 * The fixture exercises the public data-class shape rather than the full network
 * decoding pipeline because:
 *   - The single source of truth for the metadata is [NetworkLayer.handle] (see
 *     `NetworkLayer.kt` line ~84), which feeds the values straight from the
 *     decoded `NetworkPdu`.
 *   - Exercising the full pipeline would require provisioning keys / encrypted
 *     PDU samples, which is outside the scope of a 1.5 dev-day patch.
 *
 * If this test compiles and the equality check below succeeds the patch is intact.
 */
class NetworkEventMetadataTest {

    private class FakeMessage(override val opCode: UInt) : MeshMessage {
        // BaseMeshMessage contract — opaque payload, not used for this fixture.
        override val parameters: ByteArray? = null
        override fun toString() = "FakeMessage(opCode=0x${opCode.toString(16)})"
    }

    @Test
    fun `MeshMessageReceived exposes simdo RX metadata fields`() {
        val src = UnicastAddress(address = 0x0101)
        val dst = UnicastAddress(address = 0x0102)
        val msg = FakeMessage(opCode = 0xC2_00_59u) // vendor 3-byte opCode example

        val event = NetworkEvent.MeshMessageReceived(
            source = src.address,
            destination = dst,
            message = msg,
            sequence = 0x123456u,
            ivIndex = 0x0000_0001u,
            ttl = 0x05u,
        )

        // Patch contract: the three new fields must be addressable on the data class.
        assertEquals(0x123456u, event.sequence, "sequence must round-trip")
        assertEquals(0x0000_0001u, event.ivIndex, "ivIndex must round-trip")
        assertEquals(0x05u.toUByte(), event.ttl, "ttl must round-trip")

        // Existing fields preserved.
        assertEquals(src.address, event.source)
        assertEquals(dst, event.destination)
        assertEquals(msg, event.message)
    }

    @Test
    fun `MeshMessageReceived equality includes RX metadata`() {
        val src = UnicastAddress(address = 0x0101).address
        val dst = UnicastAddress(address = 0x0102)
        val msg = FakeMessage(opCode = 0x82_03u)

        val a = NetworkEvent.MeshMessageReceived(
            source = src, destination = dst, message = msg,
            sequence = 100u, ivIndex = 1u, ttl = 3u,
        )
        val b = NetworkEvent.MeshMessageReceived(
            source = src, destination = dst, message = msg,
            sequence = 100u, ivIndex = 1u, ttl = 3u,
        )
        // Different sequence → different value
        val c = NetworkEvent.MeshMessageReceived(
            source = src, destination = dst, message = msg,
            sequence = 101u, ivIndex = 1u, ttl = 3u,
        )

        assertEquals(a, b, "identical metadata must compare equal")
        assertNotEquals(a, c, "differing sequence must break equality (cache dedup correctness)")
    }
}
