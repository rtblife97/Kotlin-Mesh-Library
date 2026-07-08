@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import no.nordicsemi.kotlin.data.HexString
import no.nordicsemi.kotlin.mesh.core.model.serialization.ModelIdSerializer

/**
 * Represents Model ID of a Bluetooth mesh model.
 *
 * @property id                         16-bit company identifier and the 16-bit model identifier
 *                                      where the company identifier being the 2-most significant
 *                                      bytes. In the case of a Bluetooth SIG defined model, the
 *                                      company identifier is 0.
 * @property isBluetoothSigAssigned     True if the model is a Bluetooth SIG defined model.
 */
@Serializable(with = ModelIdSerializer::class)
sealed class ModelId {
    @SerialName(value = "modelId")
    abstract val id: UInt
    val isBluetoothSigAssigned: Boolean
        get() = this is SigModelId

    /**
     * Converts ModelID to hex.
     *
     * @param prefix0x If true prefixes hex value with 0x.
     */
    fun toHex(prefix0x: Boolean = false) = when (id and 0xFFFF0000u) {
        0u -> "%04X".format(id.toShort())
        else -> "%08X".format(id.toInt())
    }.also {
        return if (prefix0x) "0x$it" else it
    }

    companion object {

        /**
         * Converts a [HexString] encoded model ID to a [ModelId].
         *
         * @return [ModelId] instance.
         */
        @Throws(NumberFormatException::class)
        fun HexString.decode(): ModelId = toUInt(radix = 16).decode()

        /**
         * Converts a [UInt] encoded model ID to a [ModelId].
         *
         * @return [ModelId] instance.
         */
        fun UInt.decode(): ModelId = when (this and 0xFFFF0000u) {
            0u -> SigModelId(modelIdentifier = this.toUShort())
            else -> VendorModelId(id = this)
        }
    }
}

/**
 * Wrapper class for 16-bit Bluetooth SIG model identifier.
 *
 * @property modelIdentifier 16-bit model identifier.
 */
@Serializable
class SigModelId(
    val modelIdentifier: UShort,
) : ModelId() {
    @SerialName(value = "modelId")
    override val id: UInt = modelIdentifier.toUInt()

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString(): String =
        "SigModelId(${
            modelIdentifier.toHexString(
                format = HexFormat {
                    number {
                        prefix = "0x"
                        minLength = 4
                        removeLeadingZeros = true
                    }
                }
            )
        })"

    override fun equals(other: Any?): Boolean {
        if (other !is SigModelId)
            return false
        return modelIdentifier == other.modelIdentifier
    }

    override fun hashCode(): Int {
        var result = modelIdentifier.hashCode()
        result = 31 * result + id.hashCode()
        return result
    }
}

/**
 * Wrapper class for 32-bit vendor model identifier.
 *
 * @property modelIdentifier    16-bit model identifier.
 * @property companyIdentifier  16-bit company identifier.
 */
@Serializable
class VendorModelId(
    @SerialName(value = "modelId")
    override val id: UInt,
) : ModelId() {
    val companyIdentifier: UShort = ((id and 0xFFFF0000u) shr 16).toUShort()
    val modelIdentifier: UShort = (id and 0x0000FFFFu).toUShort()

    /**
     * Constructs a VendorModelId from the given model identifier and company identifier.
     *
     * @param modelIdentifier    16-bit model identifier.
     * @param companyIdentifier  16-bit company identifier.
     * @constructor Creates a VendorModelId.
     */
    constructor(
        modelIdentifier: UShort,
        companyIdentifier: UShort,
    ) : this((modelIdentifier.toUInt()) or (companyIdentifier.toUInt() shl 16))

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString(): String {
        return "VendorModelId(${
                    modelIdentifier.toHexString(
                        format = HexFormat {
                            number {
                                prefix = "0x"
                                minLength = 4
                                upperCase = true
                            }
                        }
                    )
                }, " +
                "companyIdentifier: ${
                    companyIdentifier.toHexString(
                        format = HexFormat {
                            number {
                                prefix = "0x"
                                minLength = 4
                                upperCase = true
                            }
                        }
                    )
                })"
    }

    // simdo-fork (2026-07-08) — copy-paste 버그 fix: 종전 `if (other !is SigModelId) return false`
    // 였다 → 동일 VendorModelId 두 개가 절대 equal 이 아니었고(vendor-vs-vendor 는 항상 false),
    // hashCode 와도 불일치(HashSet/HashMap/distinctBy/== 전부 vendor 에서 오작동). 실기기(Silvair
    // Cylinder Sensor cid 0x0136, vendor model 14개) identity 실패의 근원. id(32-bit) 단일 비교로 교정.
    override fun equals(other: Any?): Boolean {
        if (other !is VendorModelId)
            return false
        return id == other.id
    }

    override fun hashCode(): Int = id.hashCode()
}