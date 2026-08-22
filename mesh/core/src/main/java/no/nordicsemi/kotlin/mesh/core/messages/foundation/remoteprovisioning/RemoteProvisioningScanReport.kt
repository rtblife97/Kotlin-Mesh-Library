package no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning

import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.getUuid
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.UnacknowledgedRemoteProvisioningMessage
import no.nordicsemi.kotlin.mesh.core.oob.OobInformation
import java.nio.ByteOrder
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * A Remote Provisioning Scan Report message is an unacknowledged message used by the Remote
 * Provisioning Server to report the scanned Device UUID of an unprovisioned device.
 *
 * Based on Scan Reports received from multiple Remote Provisioning Servers, the client can
 * select the most suitable server to execute the Extended Scan procedure and/or to provision
 * the unprovisioned device — [rssi] is measured **by the server**, so it is a proximity metric
 * between the device and that server, not between the device and the phone.
 *
 * Wire: `RSSI (1, int8) | UUID (16) | OOBInformation (2) | URIHash (4, optional)` —
 * `BT_MESH_LEN_MIN(19)` in `rpr_cli.c`, and the client rejects anything other than 19 or 23.
 *
 * ### ⚠️ OOB Information endianness — iOS 4.1.0 is wrong here
 *
 * The Remote Provisioning Scan Report is an **access message**, so its multi-octet fields are
 * little endian. NCS agrees on both sides: `rpr_srv.c scan_report_send()` writes
 * `net_buf_simple_add_le16(&buf, dev->oob)` and `rpr_cli.c handle_scan_report()` reads
 * `net_buf_simple_pull_le16(buf)`.
 *
 * iOS `IOS-nRF-Mesh-Library` 4.1.0 encodes `oobInformation.rawValue.bigEndian` and decodes with
 * `readBigEndian` (via `OptionSet+Data.swift`), which does **not** interoperate with a Zephyr
 * Remote Provisioning Server. This library follows NCS.
 *
 * (Do not confuse this with the Unprovisioned Device Beacon, where the OOB Information field
 * really is big endian — `beacon.c` uses `net_buf_simple_add_be16` / `pull_be16`.)
 *
 * ### ⚠️ [oobInformation] may be `null`
 *
 * OOB Information is a **bit field**; a device may advertise e.g. `QR Code | On Device`
 * (0x8004). [OobInformation.from] throws on any value that is not exactly one known flag, and
 * the model dispatch path has no try/catch around message decoding — an exception there kills
 * the RX coroutine. Therefore this class keeps [oobInformationRaw] as the source of truth and
 * exposes the typed value only as a nullable convenience.
 *
 * @property rssi               Received signal strength in dBm as measured by the server,
 *                              a signed 8-bit value.
 * @property uuid               Device UUID of the unprovisioned device. This is the identity
 *                              used both for de-duplication across scan sessions and for
 *                              [RemoteProvisioningLinkOpen].
 * @property oobInformationRaw  Raw 16-bit OOB Information bit field.
 * @property uriHash            First 4 octets of `s1(URI Data)` when the device advertises a
 *                              URI, `null` otherwise.
 */
@OptIn(ExperimentalUuidApi::class)
class RemoteProvisioningScanReport(
    val rssi: Byte,
    val uuid: Uuid,
    val oobInformationRaw: UShort,
    val uriHash: ByteArray? = null,
) : UnacknowledgedRemoteProvisioningMessage {

    /** Convenience accessor for [rssi] in dBm. */
    val rssiDbm: Int
        get() = rssi.toInt()

    /**
     * The typed OOB Information, or `null` when the raw value does not map to exactly one
     * known flag (e.g. it is a combination, or uses a bit this library does not know).
     * Use [oobInformationRaw] when the exact bits matter.
     */
    val oobInformation: OobInformation?
        get() = runCatching { OobInformation.from(oobInformationRaw) }.getOrNull()

    override val opCode = Initializer.opCode
    override val parameters = byteArrayOf(rssi) +
            uuid.toByteArray() +
            oobInformationRaw.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            (uriHash ?: byteArrayOf())

    /**
     * MshPRT 1.1, section 4.4.5.5.1.7 requires scan reports to be sent as segmented messages
     * to ensure delivery, even when the PDU would fit in an unsegmented Access message.
     */
    override val isSegmented: Boolean = true

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString() = "RemoteProvisioningScanReport(" +
            "rssi: $rssi dBm, " +
            "uuid: $uuid, " +
            "oobInformation: 0x${
                oobInformationRaw.toString(radix = 16).padStart(length = 4, padChar = '0')
                    .uppercase()
            }" +
            (uriHash?.let { ", uriHash: ${it.toHexString()}" } ?: "") + ")"

    companion object Initializer : RemoteProvisioningMessageInitializer {
        override val opCode = 0x8055u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size == 19 || it.size == 23 }
            ?.let { params ->
                RemoteProvisioningScanReport(
                    rssi = params[0],
                    uuid = params.getUuid(offset = 1),
                    oobInformationRaw = params.getUShort(
                        offset = 17,
                        order = ByteOrder.LITTLE_ENDIAN
                    ),
                    uriHash = if (params.size == 23) params.copyOfRange(19, 23) else null
                )
            }
    }
}
