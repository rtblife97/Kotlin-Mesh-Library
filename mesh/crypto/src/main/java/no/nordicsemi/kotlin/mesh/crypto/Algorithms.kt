@file:Suppress("ClassName", "unused", "MemberVisibilityCanBePrivate")

package no.nordicsemi.kotlin.mesh.crypto

/**
 * Algorithm used for calculating a Device Key.
 *
 * @property length        Length of the algorithm.
 * @property value         Value of the algorithm.
 */
enum class Algorithm {

    /**
     * `FIPS_P256_ELLIPTIC_CURVE` algorithm will be used to calculate the shared secret.
     * This has been renamed to `BTM_ECDH_P256_CMAC_AES128_AES_CCM` in Mesh Protocol 1.1.
     */
    @Deprecated(
        message = "Renamed to BTM_ECDH_P256_CMAC_AES128_AES_CCM in Mesh Protocol 1.1",
        replaceWith = ReplaceWith("BTM_ECDH_P256_CMAC_AES128_AES_CCM"),
        level = DeprecationLevel.WARNING
    )
    FIPS_P256_ELLIPTIC_CURVE,

    /**
     * `BTM_ECDH_P256_CMAC_AES128_AES_CCM` algorithm will be used to calculate the shared secret.
     */
    BTM_ECDH_P256_CMAC_AES128_AES_CCM,

    /**
     * `BTM_ECDH_P256_HMAC_SHA256_AES_CCM` algorithm will be used to calculate the shared secret.
     */
    BTM_ECDH_P256_HMAC_SHA256_AES_CCM;

    @Suppress("DEPRECATION")
    val length: Int
        get() = when (this) {
            FIPS_P256_ELLIPTIC_CURVE, BTM_ECDH_P256_CMAC_AES128_AES_CCM -> 128
            BTM_ECDH_P256_HMAC_SHA256_AES_CCM -> 256
        }

    @Suppress("DEPRECATION")
    val value: UByte
        get() = when (this) {
            FIPS_P256_ELLIPTIC_CURVE, BTM_ECDH_P256_CMAC_AES128_AES_CCM -> 0x00u
            BTM_ECDH_P256_HMAC_SHA256_AES_CCM -> 0x01u
        }


    companion object {
        /**
         * Returns the algorithm from the given provisioning pdu.
         *
         * @param pdu The provisioning pdu.
         */
        fun from(pdu: ByteArray): Algorithm? = when (pdu[1]) {
            0x00.toByte() -> BTM_ECDH_P256_CMAC_AES128_AES_CCM
            0x01.toByte() -> BTM_ECDH_P256_HMAC_SHA256_AES_CCM
            else -> null
        }

        /**
         * Returns the strongest algorithm from a given list of supported algorithms.
         *
         * @receiver list of Algorithms.
         * @return strongest algorithm supported.
         */
        fun List<Algorithms>.strongest(): Algorithm = find {
            it.rawValue == Algorithms.BtmEcdhP256HmacSha256AesCcm.rawValue
        }?.let {
            BTM_ECDH_P256_HMAC_SHA256_AES_CCM
        } ?: BTM_ECDH_P256_CMAC_AES128_AES_CCM
    }
}

/**
 * A set of algorithms supported by the unprovisioned device.
 *
 * @property rawValue The raw value of the algorithm.
 */
sealed class Algorithms(val rawValue: UShort) {

    /**
     * Convenience constructor to accept Int values.
     *
     * @param rawValue The raw value of the algorithm.
     */
    constructor(rawValue: Int) : this(rawValue.toUShort())

    @Deprecated(
        message = "Renamed to BtmEcdhP256CmacAes128AesCcm in Mesh Protocol 1.1",
        replaceWith = ReplaceWith("BtmEcdhP256CmacAes128AesCcm"),
        level = DeprecationLevel.WARNING
    )

    object FipsP256EllipticCurve : Algorithms(rawValue = 1 shl 0) {
        override fun toString(): String = "FIPS P256 ELLIPTIC CURVE"
    }

    object BtmEcdhP256CmacAes128AesCcm : Algorithms(rawValue = 1 shl 0) {
        override fun toString(): String = "BTM ECDH P256 CMAC AES128 AES CCM"
    }

    object BtmEcdhP256HmacSha256AesCcm : Algorithms(rawValue = 1 shl 1) {
        override fun toString(): String = "BTM ECDH P256 HMAC SHA256 AES CCM"
    }

    companion object {

        /**
         * simdo-patch (2026-08-26) — `val` → `get()`.
         *
         * 종전 `val algorithms = listOf(BtmEcdhP256CmacAes128AesCcm, …)` 는 클래스 초기화
         * 순환에 걸려 **원소가 `null` 인 리스트**가 될 수 있었다:
         *
         * 1. 누군가 `Algorithms.BtmEcdhP256CmacAes128AesCcm` 을 먼저 만진다,
         * 2. JVM 이 그 object 의 `<clinit>` 을 시작하고, 상위 클래스 `Algorithms` 를 먼저
         *    초기화해야 하므로 `Algorithms.<clinit>` 으로 들어간다,
         * 3. 거기서 companion 이 `listOf(BtmEcdhP256CmacAes128AesCcm, …)` 를 평가하는데
         *    그 object 는 **같은 스레드에서 초기화 진행 중**이라 JVM 이 기다리지 않고
         *    `INSTANCE == null` 을 그대로 돌려준다,
         * 4. 리스트에 null 이 박히고 **프로세스가 살아 있는 내내** 그대로 남는다.
         *
         * 증상은 그 다음 [from] 호출에서 `NullPointerException: Cannot invoke
         * Algorithms.getRawValue() because "it" is null` — 즉 Provisioning Capabilities
         * 파싱이 통째로 죽는다. companion 을 먼저 만지는 순서(= 지금까지의 실사용 경로)에서는
         * 증상이 없어 오래 숨어 있었다. getter 로 만들면 두 object 가 모두 완전히 초기화된
         * 뒤에 평가되므로 순서에 무관해진다.
         */
        val algorithms: List<Algorithms>
            get() = listOf(BtmEcdhP256CmacAes128AesCcm, BtmEcdhP256HmacSha256AesCcm)

        /**
         * Returns the list supported algorithms.
         *
         * @param supportedAlgorithms Supported algorithms from provisioning capabilities pdu.
         * @return a list of supported algorithms or an empty list if none is supported.
         */
        fun from(supportedAlgorithms: UShort): List<Algorithms> = algorithms.filter {
            it.rawValue.toInt() and supportedAlgorithms.toInt() != 0
        }

        /**
         * Converts a list of supported algorithms to a UShort value.
         *
         * @receiver List of supported algorithms.
         * @return UShort containing the raw value of the list of algorithms.
         */
        fun List<Algorithms>.toUShort(): UShort {
            var value = 0
            forEach {
                value = value or it.rawValue.toInt()
            }
            return value.toUShort()
        }

        /**
         * Returns the strongest algorithm from a given list of supported algorithms.
         *
         * @receiver list of Algorithms.
         * @return strongest algorithm supported.
         */
        fun List<Algorithms>.strongest(): Algorithms = find {
            it.rawValue == BtmEcdhP256HmacSha256AesCcm.rawValue
        } ?: BtmEcdhP256CmacAes128AesCcm
    }
}
