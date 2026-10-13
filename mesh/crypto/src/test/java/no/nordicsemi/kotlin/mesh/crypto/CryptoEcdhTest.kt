package no.nordicsemi.kotlin.mesh.crypto

import org.junit.Assert
import org.junit.Test
import java.security.Security

/**
 * simdo fork 2026-10-13 — ECDH (provisioning shared secret) works with the private BouncyCastle instance, and mesh crypto
 * does not change the global provider list (removing the platform "BC" broke the default SSLContext on Android).
 */
class CryptoEcdhTest {

    @Test
    fun sharedSecretMatchesOnBothSides() {
        val a = Crypto.generateKeyPair(Algorithm.FIPS_P256_ELLIPTIC_CURVE)
        val b = Crypto.generateKeyPair(Algorithm.FIPS_P256_ELLIPTIC_CURVE)
        val sa = Crypto.calculateSharedSecret(a.private, b.public.toByteArray())
        val sb = Crypto.calculateSharedSecret(b.private, a.public.toByteArray())
        Assert.assertEquals(32, sa.size)
        Assert.assertArrayEquals(sa, sb)
    }

    @Test
    fun globalProvidersAreLeftAlone() {
        val before = Security.getProviders().map { it.name }
        val kp = Crypto.generateKeyPair(Algorithm.FIPS_P256_ELLIPTIC_CURVE)
        Crypto.calculateSharedSecret(kp.private, Crypto.generateKeyPair(Algorithm.FIPS_P256_ELLIPTIC_CURVE).public.toByteArray())
        Assert.assertEquals(before, Security.getProviders().map { it.name })
    }
}
