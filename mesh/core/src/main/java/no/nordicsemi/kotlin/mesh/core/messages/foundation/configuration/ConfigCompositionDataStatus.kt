@file:Suppress("unused")

package no.nordicsemi.kotlin.mesh.core.messages.foundation.configuration

import no.nordicsemi.kotlin.data.IntFormat
import no.nordicsemi.kotlin.data.getInt
import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.ConfigMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.ConfigResponse
import no.nordicsemi.kotlin.mesh.core.model.Element
import no.nordicsemi.kotlin.mesh.core.model.Features
import no.nordicsemi.kotlin.mesh.core.model.Location
import no.nordicsemi.kotlin.mesh.core.model.Model
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.SigModelId
import no.nordicsemi.kotlin.mesh.core.model.VendorModelId
import no.nordicsemi.kotlin.mesh.core.model.composition
import java.nio.ByteOrder

/**
 * Base interface for a Composition Data Page.
 *
 * Composition Data state contains information about the composition of a give Node, the Elements it
 * includes and the models that are supported by each element.
 *
 * @property page Page number of the Composition Data.
 * @property parameters The parameters of the page.
 */
sealed interface CompositionDataPage {
    val page: UByte
    val parameters: ByteArray?
}

/**
 * This message is the response received when requesting the composition data of a Node using
 * [ConfigCompositionDataGet] message.
 *
 * @property page Page containing the composition of a node.
 * @constructor Creates a ConfigCompositionDataStatus message.
 */
class ConfigCompositionDataStatus(val page: CompositionDataPage) : ConfigResponse {
    override val opCode: UInt = Initializer.opCode
    override val parameters: ByteArray? = page.parameters

    override fun toString() = "ConfigCompositionDataStatus(page: $page)"

    companion object Initializer : ConfigMessageInitializer {
        override val opCode = 0x02u

        override fun init(parameters: ByteArray?) = parameters?.takeIf {
            it.isNotEmpty()
        }?.let {
            when (it[0].toUByte().toInt()) {
                PAGE_0 -> Page0.init(it)?.let { page -> ConfigCompositionDataStatus(page = page) }
                // simdo-patch (2026-08-26) — Composition Data Page 128 (MshPRT 1.1 §4.2.1.2).
                // NPPI 의 Node Composition Refresh 절차(§3.11.8.6)가 Page 0 으로 승격시킬
                // **예정** composition 이다. 이걸 못 읽으면 "이 노드에 반영 대기 중인
                // composition 변경이 있는가?" 를 알 방법이 없어 NPPI 0x02 를 맹목적으로
                // 쏘게 되고, Remote Provisioning Server 는 composition 이 dirty 하지 않으면
                // Link Open 을 `LINK_CANNOT_OPEN` 으로 거절한다(Zephyr `rpr_srv.c:876-880`).
                PAGE_128 -> Page128.init(it)?.let { page ->
                    ConfigCompositionDataStatus(page = page)
                }

                else -> null
            }
        }

        private const val PAGE_0 = 0
        private const val PAGE_128 = 0x80
    }
}

/**
 * Composition Data Page 0 shall be present on a Node.
 *
 * Composition Data Page 0 shall not change during a term of Node in the network.
 *
 * @property page                                    Page number of the Composition Data.
 * @property companyIdentifier                       16-bit Company Identifier (CID) assigned by
 *                                                   Bluetooth SIG. CIDs can be found on
 *                                                   [Assigned Numbers](https://www.bluetooth.com/specifications/assigned-numbers/company-identifiers/)
 * @property productIdentifier                       16-bit vendor-assigned Product Identifier (PID).
 * @property versionIdentifier                       16-bit vendor-assigned Version Identifier (VID).
 * @property minimumNumberOfReplayProtectionList     Minimum number of entries in the Replay
 *                                                   Protection List for a given node.
 * @property features                                Features supported by the node. Page 0 of the
 *                                                   Composition Data does not provide information
 *                                                   whether a feature is enabled or disabled, just
 *                                                   whether it is supported or not. Read the state
 *                                                   of each feature using corresponding Config
 *                                                   message.
 * @property elements                                List of elements that are present on the node.
 * @constructor Creates a Page 0 of the Composition Data.
 */
data class Page0(
    override val page: UByte,
    val companyIdentifier: UShort,
    val productIdentifier: UShort,
    val versionIdentifier: UShort,
    val minimumNumberOfReplayProtectionList: UShort,
    val features: Features,
    val elements: List<Element>,
) : CompositionDataPage {

    override val parameters: ByteArray = byteArrayOf(page.toByte()) +
            companyIdentifier.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            productIdentifier.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            versionIdentifier.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            minimumNumberOfReplayProtectionList.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            features.rawValue.toByteArray(ByteOrder.LITTLE_ENDIAN) +
            elements.composition()

    /**
     * Constructs the parameters of the Composition Data Page 0.
     *
     * @param node Node to get the composition data from.
     */
    constructor(node: Node) : this(
        page = 0u,
        companyIdentifier = node.companyIdentifier ?: 0u,
        productIdentifier = node.productIdentifier ?: 0u,
        versionIdentifier = node.versionIdentifier ?: 0u,
        minimumNumberOfReplayProtectionList = node.replayProtectionCount ?: 0u,
        features = node.features,
        elements = node.elements
    )

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString(): String {
        return "Page0(" +
                "page: $page, " +
                "cid: ${
                    companyIdentifier.toHexString(
                        format = HexFormat {
                            number {
                                prefix = "0x"
                                minLength = 4
                                upperCase = true
                            }
                        }
                    )
                }, " +
                "pid: ${
                    productIdentifier.toHexString(
                        format = HexFormat {
                            number {
                                prefix = "0x"
                                minLength = 4
                                upperCase = true
                            }
                        }
                    )
                }, " +
                "vid: ${
                    versionIdentifier.toHexString(
                        format = HexFormat {
                            number {
                                prefix = "0x"
                                minLength = 4
                                upperCase = true
                            }
                        }
                    )
                }, " +
                "crpl: $minimumNumberOfReplayProtectionList, " +
                "features: [$features], " +
                "elements: [${elements.map {  element ->
                    "Element(location: ${element.location}, models: ${element.models.joinToString { it.modelId.toString() }})"
                }}])"
    }

    companion object {

        /**
         * Constructs the Composition Data Page 0 from the given parameters.
         *
         * @param parameters The parameters of the page.
         * @return The Composition Data Page 0 or null otherwise.
         */
        fun init(parameters: ByteArray?) =
            parseCompositionDataPage0Format(parameters = parameters, expectedPage = 0)?.let {
                Page0(
                    page = 0u,
                    companyIdentifier = it.companyIdentifier,
                    productIdentifier = it.productIdentifier,
                    versionIdentifier = it.versionIdentifier,
                    minimumNumberOfReplayProtectionList = it.minimumNumberOfReplayProtectionList,
                    features = it.features,
                    elements = it.elements,
                )
            }
    }
}

/**
 * Composition Data Page 128 — the composition data that **will** become Page 0 once the
 * Node Composition Refresh procedure (MshPRT 1.1 §3.11.8.6) completes.
 *
 * simdo-patch (2026-08-26).
 *
 * A node that detects a change of its own composition (typically after a firmware update)
 * instantiates Page 128 with the *new* composition while Page 0 keeps reporting the *old* one,
 * so that a Provisioner's cached configuration stays valid until it is ready to migrate.
 *
 * The wire format is identical to [Page0] except for the leading page number (0x80) —
 * Zephyr serves both from the same routine, `access.c bt_mesh_comp_data_get_page()`:
 * `if (page == 0 || page == 128) return bt_mesh_comp_data_get_page_0(...)`.
 *
 * ### 🚨 "Page 128 로 답이 왔다" 는 변경 대기의 증거가 **아니다**
 *
 * Zephyr 는 `CONFIG_BT_MESH_RPR_SRV=y` 인 노드라면 composition 이 dirty 하지 않아도 Page 128
 * 요청에 Page 128 로 답한다(`access.c bt_mesh_comp_page()`:
 * `page >= 128 && (COMP_DIRTY || IS_ENABLED(CONFIG_BT_MESH_RPR_SRV))`). 반영 대기 중인 변경이
 * 있는지 판정하려면 **Page 0 과 Page 128 의 내용을 비교**해야 한다 — 다르면 대기 중,
 * 같으면 없다. Page 0 은 dirty 일 때만 저장본(옛 composition)에서 나온다
 * (`cfg_srv.c dev_comp_data_get()`).
 *
 * @property page                                    Page number of the Composition Data (0x80).
 * @property companyIdentifier                       16-bit Company Identifier (CID).
 * @property productIdentifier                       16-bit vendor-assigned Product Identifier.
 * @property versionIdentifier                       16-bit vendor-assigned Version Identifier.
 * @property minimumNumberOfReplayProtectionList     Minimum number of RPL entries.
 * @property features                                Features supported after the change.
 * @property elements                                Elements present after the change.
 */
data class Page128(
    override val page: UByte,
    val companyIdentifier: UShort,
    val productIdentifier: UShort,
    val versionIdentifier: UShort,
    val minimumNumberOfReplayProtectionList: UShort,
    val features: Features,
    val elements: List<Element>,
) : CompositionDataPage {

    override val parameters: ByteArray = byteArrayOf(page.toByte()) +
            companyIdentifier.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            productIdentifier.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            versionIdentifier.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            minimumNumberOfReplayProtectionList.toByteArray(order = ByteOrder.LITTLE_ENDIAN) +
            features.rawValue.toByteArray(ByteOrder.LITTLE_ENDIAN) +
            elements.composition()

    override fun toString() = "Page128(elements: ${elements.size}, models: " +
            "${elements.sumOf { it.models.size }})"

    companion object {

        /**
         * Constructs the Composition Data Page 128 from the given parameters.
         *
         * @param parameters The parameters of the page.
         * @return The Composition Data Page 128 or null otherwise.
         */
        fun init(parameters: ByteArray?) =
            parseCompositionDataPage0Format(parameters = parameters, expectedPage = 0x80)?.let {
                Page128(
                    page = 0x80u,
                    companyIdentifier = it.companyIdentifier,
                    productIdentifier = it.productIdentifier,
                    versionIdentifier = it.versionIdentifier,
                    minimumNumberOfReplayProtectionList = it.minimumNumberOfReplayProtectionList,
                    features = it.features,
                    elements = it.elements,
                )
            }
    }
}

/**
 * Fields shared by every Composition Data page that uses the Page 0 layout (Page 0 and Page 128).
 */
internal class CompositionDataPage0Fields(
    val companyIdentifier: UShort,
    val productIdentifier: UShort,
    val versionIdentifier: UShort,
    val minimumNumberOfReplayProtectionList: UShort,
    val features: Features,
    val elements: List<Element>,
)

/**
 * Parses the Page 0 layout (MshPRT 1.1 §4.2.1.1). Shared by [Page0] and [Page128] — Zephyr
 * serialises both from the same routine, so a single parser is the honest mirror of the wire.
 *
 * @param parameters   Raw access message parameters, first octet being the page number.
 * @param expectedPage Page number this parser is being used for (0 or 0x80).
 * @return Parsed fields, or `null` if the payload is malformed or is a different page.
 */
internal fun parseCompositionDataPage0Format(
    parameters: ByteArray?,
    expectedPage: Int,
): CompositionDataPage0Fields? {
    return parameters?.takeIf {
        it.size >= 11 && it[0].toUByte().toInt() == expectedPage
    }?.let { compositionData ->
        val companyIdentifier = compositionData.getUShort(
            offset = 1,
            order = ByteOrder.LITTLE_ENDIAN
        )
        val productIdentifier = compositionData.getUShort(
            offset = 3,
            order = ByteOrder.LITTLE_ENDIAN
        )
        val versionIdentifier = compositionData.getUShort(
            offset = 5,
            order = ByteOrder.LITTLE_ENDIAN
        )
        val minimumNumberOfReplayProtectionList = compositionData.getUShort(
            offset = 7,
            order = ByteOrder.LITTLE_ENDIAN
        )
        val features = Features.init(
            mask = compositionData.getUShort(
                offset = 9,
                order = ByteOrder.LITTLE_ENDIAN
            )
        )
        val elements = mutableListOf<Element>()
        var offset = 11
        while (offset < compositionData.size) {
            require(compositionData.size >= offset + 4) {
                return null
            }
            val rawValue = compositionData.getUShort(
                offset = offset,
                order = ByteOrder.LITTLE_ENDIAN
            )
            val location = Location.from(value = rawValue)
            val sigModelsByteCount = compositionData.getInt(
                offset = offset + 2,
                format = IntFormat.UINT8
            ) * 2
            val vendorModelsByteCount = compositionData.getInt(
                offset = offset + 3,
                format = IntFormat.UINT8
            ) * 4

            require(compositionData.size >= (offset + 3 + sigModelsByteCount + vendorModelsByteCount)) {
                return null
            }

            // 4 bytes have been read.
            offset += 4

            // Set temporary index.
            // Final index will be set when Element is added to the Node.
            val index = 0

            // Read models.
            val element = Element(_name = "Element ${elements.size + 1}", location = location)
                .apply { this.index = index }

            for (i in offset until offset + sigModelsByteCount step 2) {
                val sigModelId = compositionData.getUShort(
                    offset = i,
                    order = ByteOrder.LITTLE_ENDIAN
                )
                element.add(model = Model(modelId = SigModelId(sigModelId)))
            }
            offset += sigModelsByteCount

            for (i in offset until offset + vendorModelsByteCount step 4) {
                element.add(
                    Model(
                        modelId = VendorModelId(
                            companyIdentifier = compositionData.getUShort(
                                offset = i,
                                order = ByteOrder.LITTLE_ENDIAN
                            ),
                            modelIdentifier = compositionData.getUShort(
                                offset = i + 2,
                                order = ByteOrder.LITTLE_ENDIAN
                            )
                        )
                    )
                )
            }
            offset += vendorModelsByteCount
            elements.add(element = element)
        }
        CompositionDataPage0Fields(
            companyIdentifier = companyIdentifier,
            productIdentifier = productIdentifier,
            versionIdentifier = versionIdentifier,
            minimumNumberOfReplayProtectionList = minimumNumberOfReplayProtectionList,
            features = features,
            elements = elements,
        )
    }
}