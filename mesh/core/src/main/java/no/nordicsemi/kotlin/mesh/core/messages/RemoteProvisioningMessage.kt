@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package no.nordicsemi.kotlin.mesh.core.messages

/**
 * A base interface for all Remote Provisioning messages.
 *
 * Remote Provisioning messages are exchanged between the Remote Provisioning Client model
 * (hosted by this library on the local Provisioner) and the Remote Provisioning Server model
 * on a remote Node. Like Configuration messages they are secured with the **Device Key** of
 * the Node hosting the Remote Provisioning Server, therefore they extend [ConfigMessage].
 *
 * See MshPRT 1.1, section 4.4.5 (Remote Provisioning models).
 *
 * simdo-fork (2026-08-12): 이 파일과 `messages/foundation/remoteprovisioning/` 전체는
 * upstream 에 존재하지 않는 자체 구현이다. 참조 정답지는 두 가지를 교차 사용했다:
 *  - iOS `IOS-nRF-Mesh-Library` 4.1.0 `Mesh Messages/Foundation/Remote Provisioning/`
 *    (구조·API 형태)
 *  - **NCS v3.4.0** `zephyr/subsys/bluetooth/mesh/{rpr.h, rpr_srv.c, rpr_cli.c}`
 *    (**wire 포맷의 최종 권위** — 우리 동글 `neo_mesh_commissioner` 가 이 코드를 그대로 돌린다)
 * 두 정답지가 어긋나는 지점은 각 메시지 클래스 KDoc 에 `⚠️ iOS 상이` 로 표기했다.
 */
interface RemoteProvisioningMessage : ConfigMessage

/**
 * A base decoder interface for Remote Provisioning messages.
 */
interface RemoteProvisioningMessageInitializer : ConfigMessageInitializer

/**
 * A base interface for unacknowledged Remote Provisioning messages.
 */
interface UnacknowledgedRemoteProvisioningMessage :
    RemoteProvisioningMessage, UnacknowledgedConfigMessage

/**
 * A base interface for Remote Provisioning messages sent as a response to an acknowledged
 * Remote Provisioning message.
 */
interface RemoteProvisioningResponse : ConfigResponse, UnacknowledgedRemoteProvisioningMessage

/**
 * A base interface for acknowledged Remote Provisioning messages.
 *
 * Acknowledged messages will be responded to with a status message.
 */
interface AcknowledgedRemoteProvisioningMessage :
    RemoteProvisioningMessage, AcknowledgedConfigMessage

/**
 * A base interface for Remote Provisioning messages carrying an operation status.
 *
 * A Remote Provisioning status message may be received as a response to an acknowledged
 * message, or unsolicited as a Report message.
 */
interface RemoteProvisioningStatusMessage : RemoteProvisioningMessage, StatusMessage {

    /** Status of the requested operation. */
    val status: RemoteProvisioningMessageStatus

    override val isSuccess: Boolean
        get() = status == RemoteProvisioningMessageStatus.SUCCESS

    override val message: String
        get() = status.toString()
}

/**
 * A base interface for Remote Provisioning messages reporting the link state.
 */
interface RemoteProvisioningLinkStateMessage : RemoteProvisioningMessage {

    /** Remote Provisioning Link state. */
    val linkState: RemoteProvisioningLinkState
}

/**
 * Status of a Remote Provisioning operation.
 *
 * Values match `enum bt_mesh_rpr_status` in
 * `zephyr/include/zephyr/bluetooth/mesh/rpr.h` (verified against NCS v3.4.0).
 *
 * @property value The status value transported on the wire.
 */
enum class RemoteProvisioningMessageStatus(val value: UByte) {

    /** Success. */
    SUCCESS(0x00u),

    /** Scanning Cannot Start. */
    SCANNING_CANNOT_START(0x01u),

    /** Invalid State. */
    INVALID_STATE(0x02u),

    /** Limited Resources. */
    LIMITED_RESOURCES(0x03u),

    /** Link Cannot Open. */
    LINK_CANNOT_OPEN(0x04u),

    /** Link Open Failed. */
    LINK_OPEN_FAILED(0x05u),

    /** Link Closed by Device. */
    LINK_CLOSED_BY_DEVICE(0x06u),

    /** Link Closed by Server. */
    LINK_CLOSED_BY_SERVER(0x07u),

    /** Link Closed by Client. */
    LINK_CLOSED_BY_CLIENT(0x08u),

    /** Link Closed as Cannot Receive PDU. */
    LINK_CLOSED_AS_CANNOT_RECEIVE_PDU(0x09u),

    /** Link Closed as Cannot Send PDU. */
    LINK_CLOSED_AS_CANNOT_SEND_PDU(0x0Au),

    /** Link Closed as Cannot Deliver PDU Report. */
    LINK_CLOSED_AS_CANNOT_DELIVER_PDU_REPORT(0x0Bu);

    override fun toString() = when (this) {
        SUCCESS -> "Success"
        SCANNING_CANNOT_START -> "Scanning cannot start"
        INVALID_STATE -> "Invalid state"
        LIMITED_RESOURCES -> "Limited resources"
        LINK_CANNOT_OPEN -> "Link cannot open"
        LINK_OPEN_FAILED -> "Link open failed"
        LINK_CLOSED_BY_DEVICE -> "Link closed by device"
        LINK_CLOSED_BY_SERVER -> "Link closed by server"
        LINK_CLOSED_BY_CLIENT -> "Link closed by client"
        LINK_CLOSED_AS_CANNOT_RECEIVE_PDU -> "Link closed as cannot receive PDU"
        LINK_CLOSED_AS_CANNOT_SEND_PDU -> "Link closed as cannot send PDU"
        LINK_CLOSED_AS_CANNOT_DELIVER_PDU_REPORT -> "Link closed as cannot deliver PDU report"
    }

    companion object {

        /**
         * Returns the status for the given raw value, or `null` if the value is not defined.
         *
         * simdo-fork: 절대 throw 하지 않는다. 디코더에서 예외가 나면
         * [no.nordicsemi.kotlin.mesh.core.ModelEventHandler] 의 decode 경로에 try/catch 가 없어
         * **RX 코루틴이 죽는다** (Mesh DFU 백포트 감사 #6/#7 과 같은 부류).
         */
        fun from(value: UByte): RemoteProvisioningMessageStatus? =
            entries.firstOrNull { it.value == value }
    }
}

/**
 * The Remote Provisioning Scan state describes the state of the Remote Provisioning Scan
 * procedure in the Remote Provisioning Server model.
 *
 * Values match `enum bt_mesh_rpr_scan` (NCS v3.4.0).
 *
 * @property value The scan state value transported on the wire.
 */
enum class RemoteProvisioningScanState(val value: UByte) {

    /** Idle — no scan is running. */
    IDLE(0x00u),

    /** Remote Provisioning Multiple Devices Scan (not limited to one device). */
    MULTIPLE_DEVICE_SCAN(0x01u),

    /** Remote Provisioning Single Device Scan (limited to one device). */
    SINGLE_DEVICE_SCAN(0x02u);

    override fun toString() = when (this) {
        IDLE -> "Idle"
        MULTIPLE_DEVICE_SCAN -> "Multiple device scan"
        SINGLE_DEVICE_SCAN -> "Single device scan"
    }

    companion object {

        /** Returns the scan state for the given raw value, or `null` if not defined. */
        fun from(value: UByte): RemoteProvisioningScanState? =
            entries.firstOrNull { it.value == value }
    }
}

/**
 * The Remote Provisioning Link state describes the state of the provisioning bearer link
 * of the Remote Provisioning Server model.
 *
 * During the execution of any of the Node Provisioning Protocol Interface procedures the
 * [LINK_OPENING], [OUTBOUND_PACKET_TRANSFER] and [LINK_CLOSING] values are not used.
 *
 * Values match `enum bt_mesh_rpr_link_state` (NCS v3.4.0). Note that NCS names value 0x03
 * `BT_MESH_RPR_LINK_SENDING`; the specification calls it Outbound Packet Transfer.
 *
 * @property value The link state value transported on the wire.
 */
enum class RemoteProvisioningLinkState(val value: UByte) {

    /** Idle. */
    IDLE(0x00u),

    /** Link Opening. */
    LINK_OPENING(0x01u),

    /** Link Active. */
    LINK_ACTIVE(0x02u),

    /** Outbound Packet Transfer. */
    OUTBOUND_PACKET_TRANSFER(0x03u),

    /** Link Closing. */
    LINK_CLOSING(0x04u);

    override fun toString() = when (this) {
        IDLE -> "Idle"
        LINK_OPENING -> "Link opening"
        LINK_ACTIVE -> "Link active"
        OUTBOUND_PACKET_TRANSFER -> "Outbound packet transfer"
        LINK_CLOSING -> "Link closing"
    }

    companion object {

        /** Returns the link state for the given raw value, or `null` if not defined. */
        fun from(value: UByte): RemoteProvisioningLinkState? =
            entries.firstOrNull { it.value == value }
    }
}

/**
 * Provisioning bearer link close reason.
 *
 * ⚠️ The NCS Remote Provisioning Server rejects (`-EINVAL`, no response at all) any
 * Remote Provisioning Link Close message whose reason is neither [SUCCESS] nor [FAIL]
 * (`rpr_srv.c handle_link_close`). [UNRECOGNIZED] therefore must never be sent; it exists
 * only to represent a value received from a peer that this library does not know.
 *
 * @property value The reason value transported on the wire.
 */
enum class RemoteProvisioningLinkCloseReason(val value: UByte) {

    /** Success. */
    SUCCESS(0x00u),

    // Value 0x01 is prohibited.

    /** Fail. */
    FAIL(0x02u),

    /** Unrecognized reason that may be defined in the future. Never sent by this library. */
    UNRECOGNIZED(0xFFu);

    override fun toString() = when (this) {
        SUCCESS -> "Success"
        FAIL -> "Fail"
        UNRECOGNIZED -> "Unrecognized"
    }

    companion object {

        /**
         * Returns the close reason for the given raw value, falling back to [UNRECOGNIZED].
         *
         * Never returns `null` — an unknown reason must not discard the whole message.
         */
        fun from(value: UByte): RemoteProvisioningLinkCloseReason =
            entries.firstOrNull { it.value == value } ?: UNRECOGNIZED
    }
}

/**
 * The Node Provisioning Protocol Interface (NPPI) is an interface used by a Node to route
 * Provisioning PDUs between the Provisioner and the layer that is executing the provisioning
 * protocol, without the Node being reprovisioned.
 *
 * Values match `enum bt_mesh_rpr_node_refresh` (NCS v3.4.0).
 *
 * @property value The procedure value transported on the wire.
 */
enum class NodeProvisioningProtocolInterfaceProcedure(val value: UByte) {

    /**
     * The Device Key Refresh procedure is used to change the Device Key without reprovisioning
     * a Node and without a need to reconfigure the Node.
     *
     * The Unicast Address, Network Key, Network Key Index and IV Index are not affected.
     */
    DEVICE_KEY_REFRESH(0x00u),

    /**
     * The Node Address Refresh procedure is used to change the Node's Device Key and Unicast
     * Address without reprovisioning.
     *
     * This procedure terminates all friendships, if applicable, and copies Composition Data
     * Page 128 to Page 0.
     */
    NODE_ADDRESS_REFRESH(0x01u),

    /**
     * The Node Composition Refresh procedure is used to change the Device Key of the Node and
     * to add or remove models or features of the Node without reprovisioning.
     *
     * This procedure copies Composition Data Page 128 to Page 0.
     *
     * Note: the NCS server rejects this procedure with
     * [RemoteProvisioningMessageStatus.LINK_CANNOT_OPEN] unless the Composition Data has
     * actually changed (`BT_MESH_COMP_DIRTY`).
     */
    NODE_COMPOSITION_REFRESH(0x02u);

    override fun toString() = when (this) {
        DEVICE_KEY_REFRESH -> "Device Key Refresh"
        NODE_ADDRESS_REFRESH -> "Node Address Refresh"
        NODE_COMPOSITION_REFRESH -> "Node Composition Refresh"
    }

    companion object {

        /** Returns the NPPI procedure for the given raw value, or `null` if not defined. */
        fun from(value: UByte): NodeProvisioningProtocolInterfaceProcedure? =
            entries.firstOrNull { it.value == value }
    }
}

/**
 * A subset of AD Types defined by Bluetooth SIG in the Assigned Numbers document under
 * Common Data Types, useful when composing the AD Type filter of a
 * [no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningExtendedScanStart]
 * message.
 *
 * simdo-fork: iOS models AD Types as an `enum` whose `length` accessor calls `fatalError()`
 * for unsupported types. AD Types are an **open registry**, so this library uses plain
 * constants instead — an unknown AD Type must never crash nor discard a report.
 */
object AdType {

    /** Incomplete List of 16-bit Service Class UUIDs. Prohibited in an Extended Scan filter. */
    const val INCOMPLETE_LIST_OF_16_BIT_SERVICE_UUIDS: UByte = 0x02u

    /** Complete List of 16-bit Service Class UUIDs. */
    const val COMPLETE_LIST_OF_16_BIT_SERVICE_UUIDS: UByte = 0x03u

    /** Incomplete List of 32-bit Service Class UUIDs. Prohibited in an Extended Scan filter. */
    const val INCOMPLETE_LIST_OF_32_BIT_SERVICE_UUIDS: UByte = 0x04u

    /** Complete List of 32-bit Service Class UUIDs. */
    const val COMPLETE_LIST_OF_32_BIT_SERVICE_UUIDS: UByte = 0x05u

    /** Incomplete List of 128-bit Service Class UUIDs. Prohibited in an Extended Scan filter. */
    const val INCOMPLETE_LIST_OF_128_BIT_SERVICE_UUIDS: UByte = 0x06u

    /** Complete List of 128-bit Service Class UUIDs. */
    const val COMPLETE_LIST_OF_128_BIT_SERVICE_UUIDS: UByte = 0x07u

    /** Shortened Local Name. Prohibited in an Extended Scan filter, see [COMPLETE_LOCAL_NAME]. */
    const val SHORTENED_LOCAL_NAME: UByte = 0x08u

    /**
     * Complete Local Name.
     *
     * When this AD Type is present in the filter, the client is requesting *either* the
     * Complete Local Name or the Shortened Local Name; the NCS server substitutes the
     * shortened name under this type (`rpr_srv.c` `adv_handle_ext_scan`).
     */
    const val COMPLETE_LOCAL_NAME: UByte = 0x09u

    /** Tx Power Level. */
    const val TX_POWER_LEVEL: UByte = 0x0Au

    /** List of 16-bit Service Solicitation UUIDs. */
    const val LIST_OF_16_BIT_SERVICE_SOLICITATION_UUIDS: UByte = 0x14u

    /** List of 128-bit Service Solicitation UUIDs. */
    const val LIST_OF_128_BIT_SERVICE_SOLICITATION_UUIDS: UByte = 0x15u

    /** Service Data - 16-bit UUID. */
    const val SERVICE_DATA_16_BIT_UUID: UByte = 0x16u

    /** Service Data - 32-bit UUID. */
    const val SERVICE_DATA_32_BIT_UUID: UByte = 0x20u

    /** Service Data - 128-bit UUID. */
    const val SERVICE_DATA_128_BIT_UUID: UByte = 0x21u

    /** List of 32-bit Service Solicitation UUIDs. */
    const val LIST_OF_32_BIT_SERVICE_SOLICITATION_UUIDS: UByte = 0x1Fu

    /** Uniform Resource Identifier. */
    const val URI: UByte = 0x24u

    /** Manufacturer Specific Data. */
    const val MANUFACTURER_SPECIFIC_DATA: UByte = 0xFFu

    /**
     * AD Types that are prohibited in the AD Type filter of a Remote Provisioning Extended
     * Scan Start message.
     *
     * The NCS server rejects the whole message with `-EINVAL` (i.e. **no report at all**) if
     * any of these is present — see `rpr_srv.c handle_extended_scan_start`.
     */
    val prohibitedInFilter: Set<UByte> = setOf(
        SHORTENED_LOCAL_NAME,
        INCOMPLETE_LIST_OF_16_BIT_SERVICE_UUIDS,
        INCOMPLETE_LIST_OF_32_BIT_SERVICE_UUIDS,
        INCOMPLETE_LIST_OF_128_BIT_SERVICE_UUIDS,
    )
}

/**
 * A single Advertising Structure, as carried inside a Remote Provisioning Extended Scan Report.
 *
 * On the wire an AD Structure is encoded as `Length (1) | AD Type (1) | AD Data (Length - 1)`.
 *
 * @property type  The AD Type, see [AdType].
 * @property value The AD Data (the Length and Type octets are not included).
 */
class AdStructure(val type: UByte, val value: ByteArray) {

    /** The number of octets this structure occupies on the wire, including the length octet. */
    val encodedSize: Int
        get() = value.size + 2

    /** Encodes this structure as `Length | Type | Data`. */
    fun encode(): ByteArray = byteArrayOf(
        (value.size + 1).toByte(),
        type.toByte()
    ) + value

    override fun equals(other: Any?): Boolean = other is AdStructure &&
            type == other.type && value.contentEquals(other.value)

    override fun hashCode(): Int = 31 * type.hashCode() + value.contentHashCode()

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString() = "AdStructure(type: 0x${
        type.toString(radix = 16).padStart(length = 2, padChar = '0').uppercase()
    }, value: ${value.toHexString()})"

    companion object {

        /**
         * Parses a concatenated list of AD Structures.
         *
         * Malformed trailing data is ignored rather than discarding the already parsed
         * structures — the Extended Scan Report is best-effort information and must never be
         * dropped as a whole because a peer appended something unexpected.
         *
         * @param data   Buffer holding the concatenated AD Structures.
         * @param offset Offset from which to start parsing.
         * @return The parsed AD Structures, possibly empty.
         */
        fun parse(data: ByteArray, offset: Int): List<AdStructure> {
            val result = mutableListOf<AdStructure>()
            var i = offset
            while (i < data.size) {
                // Length octet covers the AD Type octet plus the AD Data.
                val length = data[i].toInt() and 0xFF
                // A zero length terminates the list; a truncated structure is dropped.
                if (length == 0 || i + 1 + length > data.size) break
                val type = data[i + 1].toUByte()
                val value = data.copyOfRange(i + 2, i + 1 + length)
                result.add(AdStructure(type = type, value = value))
                i += 1 + length
            }
            return result
        }
    }
}
