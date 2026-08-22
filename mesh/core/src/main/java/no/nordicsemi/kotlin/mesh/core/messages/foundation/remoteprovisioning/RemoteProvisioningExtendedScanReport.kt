package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.getUuid
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.AdStructure
import no.nordicsemi.kotlin.mesh.core.messages.AdType
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningStatusMessage
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.oob.OobInformation
import java.nio.ByteOrder
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * A Remote Provisioning Extended Scan Report message is an unacknowledged message used by the
 * Remote Provisioning Server to report the advertising data requested by the client in a
 * [RemoteProvisioningExtendedScanStart] message.
 *
 * Wire: `Status (1) | UUID (16) | OOBInformation (2, optional) | ADStructures (n, optional)` —
 * `BT_MESH_LEN_MIN(17)` in `rpr_cli.c`.
 *
 * Three shapes occur in practice (`rpr_srv.c scan_ext_report_send` / the error path of
 * `handle_extended_scan_start`):
 *  - 17 octets: either the request failed ([status] != SUCCESS) or the device was not heard
 *    during the scan window — [oobInformationRaw] and [adStructures] are absent.
 *  - 19 octets: the device was heard, but none of the requested AD Types were present.
 *  - > 19 octets: the concatenated AD Structures follow.
 *
 * See [RemoteProvisioningScanReport] for why OOB Information is decoded **little endian**
 * (iOS 4.1.0 uses big endian here and does not interoperate with a Zephyr server) and why
 * [oobInformation] is nullable.
 *
 * @property status            Status of the Extended Scan procedure.
 * @property uuid              Device UUID the report refers to. When the client requested
 *                             information about the server itself, this is the server's own
 *                             Device UUID.
 * @property oobInformationRaw Raw 16-bit OOB Information bit field, or `null` when absent.
 * @property adStructures      AD Structures matching the requested AD Type filter. Empty when
 *                             none were collected.
 */
@OptIn(ExperimentalUuidApi::class)
class RemoteProvisioningExtendedScanReport(
    override val status: RemoteProvisioningMessageStatus,
    val uuid: Uuid,
    val oobInformationRaw: UShort? = null,
    val adStructures: List<AdStructure> = emptyList(),
) : UnacknowledgedRemoteProvisioningMessage, RemoteProvisioningStatusMessage {

    /** See [RemoteProvisioningScanReport.oobInformation]. `null` when unknown or absent. */
    val oobInformation: OobInformation?
        get() = oobInformationRaw?.let { raw ->
            runCatching { OobInformation.from(raw) }.getOrNull()
        }

    /**
     * The Complete (or Shortened) Local Name advertised by the device, decoded as UTF-8.
     *
     * This is the field that lets an installer tell one unprovisioned node from another.
     */
    val localName: String?
        get() = adStructures
            .firstOrNull { it.type == AdType.COMPLETE_LOCAL_NAME }
            ?.value
            ?.toString(Charsets.UTF_8)

    /** The advertised Tx Power Level in dBm, or `null` when not reported. */
    val txPowerLevel: Int?
        get() = adStructures
            .firstOrNull { it.type == AdType.TX_POWER_LEVEL }
            ?.value
            ?.takeIf { it.isNotEmpty() }
            ?.first()
            ?.toInt()

    /** The advertised URI, decoded as UTF-8, or `null` when not reported. */
    val uri: String?
        get() = adStructures
            .firstOrNull { it.type == AdType.URI }
            ?.value
            ?.toString(Charsets.UTF_8)

    /** Manufacturer Specific Data, or `null` when not reported. */
    val manufacturerData: ByteArray?
        get() = adStructures
            .firstOrNull { it.type == AdType.MANUFACTURER_SPECIFIC_DATA }
            ?.value

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(status.value.toByte()) +
            uuid.toByteArray() +
            (oobInformationRaw?.let { raw ->
                raw.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
                        adStructures.fold(byteArrayOf()) { acc, ad -> acc + ad.encode() }
            } ?: byteArrayOf())

    /** MshPRT 1.1, section 4.4.5.5.1.7 — scan reports are sent as segmented messages. */
    override val isSegmented: Boolean = true

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString() = "RemoteProvisioningExtendedScanReport(" +
            "status: $status, " +
            "uuid: $uuid" +
            (oobInformationRaw?.let {
                ", oobInformation: 0x${
                    it.toString(radix = 16).padStart(length = 4, padChar = '0').uppercase()
                }"
            } ?: "") +
            (localName?.let { ", localName: $it" } ?: "") +
            (adStructures.takeIf { it.isNotEmpty() }?.let { ", adStructures: $it" } ?: "") + ")"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8057u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size >= 17 }
            ?.let { params ->
                val status = RemoteProvisioningMessageStatus.from(params[0].toUByte())
                    ?: return@let null
                // The OOB Information field is only present when the device was actually
                // found; NCS gates it on `buf->len >= 2` after pulling the UUID.
                val hasOob = params.size >= 19
                RemoteProvisioningExtendedScanReport(
                    status = status,
                    uuid = params.getUuid(offset = 1),
                    oobInformationRaw = if (hasOob) {
                        params.getUShort(offset = 17, order = ByteOrder.LITTLE_ENDIAN)
                    } else null,
                    adStructures = if (params.size > 19) {
                        AdStructure.parse(data = params, offset = 19)
                    } else emptyList()
                )
            }
    }
}
