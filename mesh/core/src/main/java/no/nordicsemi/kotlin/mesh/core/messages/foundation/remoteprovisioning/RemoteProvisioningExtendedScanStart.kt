package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.data.getUuid
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.AdType
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedRemoteProvisioningMessage
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * A Remote Provisioning Extended Scan Start message is an unacknowledged message used by the
 * Remote Provisioning Client to request additional advertising information about a specific
 * unprovisioned device, or about the Remote Provisioning Server itself.
 *
 * The server answers with an unsolicited [RemoteProvisioningExtendedScanReport] — there is no
 * Status message for this procedure.
 *
 * Wire: `ADTypeFilterCount (1) | ADTypeFilter (n) | UUID (16, optional) | Timeout (1, optional)`
 * — `BT_MESH_LEN_MIN(1)`. The UUID and Timeout are present together or not at all.
 *
 * ### Why this matters for us
 *
 * A plain Scan Report only carries the Device UUID, so unprovisioned nodes are indistinguishable
 * to an installer. The Extended Scan is the only in-band way to obtain the **advertised device
 * name** (AD Type [AdType.COMPLETE_LOCAL_NAME]). Our `neo_mesh_commissioner` dongle is built
 * with `CONFIG_BT_MESH_RPR_AD_TYPES_MAX=4` specifically to support this.
 *
 * ### Server-side constraints (NCS `rpr_srv.c handle_extended_scan_start`)
 *
 * These all cause `-EINVAL`, which means the server sends **nothing at all** — the client sees
 * only a timeout:
 *  - `adTypeFilter` empty or larger than 16 entries,
 *  - any entry in [AdType.prohibitedInFilter],
 *  - duplicate entries,
 *  - trailing data that is neither absent nor exactly 17 octets,
 *  - a [timeout] outside 1..21 seconds.
 *
 * These are enforced in [init], so a malformed message cannot be constructed.
 *
 * Additionally, a filter longer than `CONFIG_BT_MESH_RPR_AD_TYPES_MAX` is **silently truncated**
 * to the first N entries by the server (`ad_count = MIN(ad_count, ...)`) — put the AD Types you
 * care about most first. For our dongle N = 4.
 *
 * The procedure also requires the provisioning link to be idle and no other Extended Scan to be
 * in progress; otherwise the server replies with an Extended Scan Report carrying
 * [no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus.LIMITED_RESOURCES].
 *
 * @property adTypeFilter The AD Types to be reported, in priority order.
 * @property uuid         Device UUID of the unprovisioned device to inspect. When `null`, the
 *                        server reports information about **itself**.
 * @property timeout      Time limit for the extended scan, 1..21 seconds. Must be `null` when
 *                        [uuid] is `null`, and non-`null` otherwise.
 */
@OptIn(ExperimentalUuidApi::class)
class RemoteProvisioningExtendedScanStart(
    val adTypeFilter: List<UByte>,
    val uuid: Uuid? = null,
    val timeout: Duration? = null,
) : UnacknowledgedRemoteProvisioningMessage {

    init {
        require(adTypeFilter.isNotEmpty() && adTypeFilter.size <= MAX_AD_TYPE_FILTER_COUNT) {
            "AD Type filter must contain 1..$MAX_AD_TYPE_FILTER_COUNT entries, " +
                    "was ${adTypeFilter.size}"
        }
        require(adTypeFilter.none { it in AdType.prohibitedInFilter }) {
            "AD Type filter must not contain prohibited AD Types " +
                    "(Shortened Local Name, Incomplete List of Service Class UUIDs)"
        }
        require(adTypeFilter.distinct().size == adTypeFilter.size) {
            "AD Type filter must not contain duplicates"
        }
        require((uuid == null) == (timeout == null)) {
            "UUID and timeout must both be present or both be absent"
        }
        timeout?.let {
            require(it.inWholeSeconds in EXT_SCAN_TIMEOUT_MIN_SECONDS..EXT_SCAN_TIMEOUT_MAX_SECONDS) {
                "Extended scan timeout must be in range " +
                        "$EXT_SCAN_TIMEOUT_MIN_SECONDS..$EXT_SCAN_TIMEOUT_MAX_SECONDS seconds, " +
                        "was $it"
            }
        }
    }

    /** Number of AD Types in the AD Type Filter field. */
    val adTypeFilterCount: UByte = adTypeFilter.size.toUByte()

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(adTypeFilterCount.toByte()) +
            ByteArray(adTypeFilter.size) { adTypeFilter[it].toByte() } +
            (uuid?.let { id ->
                id.toByteArray() +
                        byteArrayOf(timeout!!.inWholeSeconds.toInt().toByte())
            } ?: byteArrayOf())

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString() = "RemoteProvisioningExtendedScanStart(" +
            "adTypeFilter: ${
                adTypeFilter.joinToString(prefix = "[", postfix = "]") {
                    "0x${it.toString(radix = 16).padStart(length = 2, padChar = '0').uppercase()}"
                }
            }" +
            (uuid?.let { ", uuid: $it, timeout: ${timeout?.inWholeSeconds} s" }
                ?: ", target: self") + ")"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8056u

        /** Maximum number of AD Types the specification allows in the filter. */
        const val MAX_AD_TYPE_FILTER_COUNT = 0x10

        /** `BT_MESH_RPR_EXT_SCAN_TIME_MIN` in `zephyr/bluetooth/mesh/rpr.h`. */
        const val EXT_SCAN_TIMEOUT_MIN_SECONDS = 1L

        /** `BT_MESH_RPR_EXT_SCAN_TIME_MAX` in `zephyr/bluetooth/mesh/rpr.h`. */
        const val EXT_SCAN_TIMEOUT_MAX_SECONDS = 21L

        override fun init(parameters: ByteArray?): RemoteProvisioningExtendedScanStart? {
            val params = parameters?.takeIf { it.size > 1 } ?: return null
            val count = params[0].toInt() and 0xFF
            if (count == 0 || count > MAX_AD_TYPE_FILTER_COUNT) return null
            if (params.size != 1 + count && params.size != 1 + count + 17) return null
            val filter = List(count) { params[1 + it].toUByte() }
            if (filter.any { it in AdType.prohibitedInFilter }) return null
            if (filter.distinct().size != filter.size) return null
            return if (params.size == 1 + count) {
                RemoteProvisioningExtendedScanStart(adTypeFilter = filter)
            } else {
                val timeoutSeconds = params[1 + count + 16].toInt() and 0xFF
                if (timeoutSeconds.toLong() !in
                    EXT_SCAN_TIMEOUT_MIN_SECONDS..EXT_SCAN_TIMEOUT_MAX_SECONDS
                ) return null
                RemoteProvisioningExtendedScanStart(
                    adTypeFilter = filter,
                    uuid = params.getUuid(offset = 1 + count),
                    timeout = timeoutSeconds.seconds
                )
            }
        }
    }
}
