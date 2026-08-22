package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.data.getUShort
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageInitializer
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareId
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareInformation
import no.nordicsemi.kotlin.mesh.core.messages.MeshResponse
import java.net.URI
import java.nio.ByteOrder


/**
 * Firmware Update Information Get message is an acknowledged message used to get information about
 * the firmware images installed on a Node.
 *
 * @property totalCount   Total number of entries in the Firmware Information List State.
 * @property firstIndex   Index of the first requested entry from the Firmware Information List
 *                        state.
 * @property list         Total number of entries in the Firmware Information List state.
 */
class FirmwareUpdateInformationStatus(
    val totalCount: UByte,
    val firstIndex: UByte,
    val list: List<FirmwareInformation>,
) : MeshResponse {
    override val opCode: UInt = Initializer.opCode
    override val parameters: ByteArray
        get() {
            var data = totalCount.toByteArray() + firstIndex.toByteArray()
            list.forEach {
                val idLength = (it.currentFirmwareId.version.size + 2).toByteArray()
                val uriLength = it.updateUri?.toString()
                    ?.toByteArray(charset = Charsets.UTF_8)?.size?.toByteArray() ?: 0.toByteArray()
                val udiData =
                    it.updateUri?.toString()?.toByteArray(charset = Charsets.UTF_8) ?: byteArrayOf()
                data += idLength + it.currentFirmwareId.version + uriLength + udiData

            }
            return data
        }

    /**
     * Creates the Firmware Update Information Get message. This convenience constructor will only
     * request the total count of entries in the Firmware Information List state.
     */
    @Suppress("unused")
    constructor(
        request: FirmwareUpdateInformationGet,
        list: List<FirmwareInformation>,
    ) : this(
        list = list,
        firstIndex = request.firstIndex,
        totalCount = list.size.toUByte()
    )

    override fun toString() = "FirmwareUpdateInformationStatus(firstIndex: $firstIndex, " +
            "Total Count: $totalCount, " +
            "List: [${list.joinToString(separator = ", ")}])"

    companion object Initializer : FirmwareDistributionMessageInitializer {
        override val opCode: UInt = 0x8309u

        override fun init(parameters: ByteArray?) = parameters
            ?.takeIf { it.size >= 2 }
            ?.let { params ->
                val firmwareList = mutableListOf<FirmwareInformation>()
                var offset = 2
                while (offset < params.size) {
                    // Decode Firmware ID
                    // simdo-patch(mesh-dfu backport): 부호확장 방어. 아래 range 검사가 있어
                    //   현재는 오파싱이 아니라 null 로 degrade 하지만, 길이 필드는 U8 이다.
                    val currentFirmwareIdLength = params[offset].toInt() and 0xFF
                    offset += 1
                    require(
                        currentFirmwareIdLength >= 2 &&
                                currentFirmwareIdLength <= 2 + 106 &&
                                params.size >= offset + currentFirmwareIdLength
                    ) {
                        return@let null
                    }

                    val cid = params.getUShort(offset = offset, order = ByteOrder.LITTLE_ENDIAN)
                    val version = params.copyOfRange(
                        fromIndex = offset + 2,
                        toIndex = offset + currentFirmwareIdLength
                    )
                    val currentFirmwareId = FirmwareId(companyIdentifier = cid, version = version)
                    offset += currentFirmwareIdLength
                    // Decode Update URI
                    // simdo-patch(mesh-dfu backport): 인자 1개짜리 require 는 lazy message 가 없어
                    //   IllegalArgumentException 을 **던진다**. 바로 위/아래 require 두 개는
                    //   `{ return@let null }` 로 null 반환한다 — 여기만 빠뜨린 것.
                    //   수신 파싱은 try/catch 없는 코루틴(NetworkManager.handle 의 scope.launch)
                    //   에서 돌기 때문에 잘린 PDU 하나가 예외를 그대로 escape 시킨다.
                    //   NCS dfu_cli.c 는 같은 상황에서 -EINVAL 을 반환한다.
                    require(params.size >= offset + 1) { return@let null }
                    val updateUriLength = params[offset].toInt() and 0xFF
                    offset += 1
                    require(updateUriLength >= 0 && params.size >= offset + updateUriLength) {
                        return@let null
                    }

                    var updateUri: String? = null
                    if (updateUriLength > 0) {
                        updateUri = params
                            .copyOfRange(fromIndex = offset, toIndex = offset + updateUriLength)
                            .toString(Charsets.UTF_8)
                    }

                    offset += updateUriLength
                    // simdo-patch(mesh-dfu backport): URI.create() 는 URI 로 허용되지 않는
                    //   문자에서, URI.toURL() 은 비절대 URI/미지원 프로토콜에서 예외를 던진다.
                    //   둘 다 잡히지 않은 채 수신 코루틴으로 escape 했다. NCS dfu_cli.c 는 이
                    //   필드를 **불투명 바이트**로 다루며(memcpy + NUL 종단) 해석하지 않는다.
                    //   파싱 실패는 해당 노드의 updateUri 를 null 로 떨어뜨릴 뿐, 메시지 전체나
                    //   RX 경로를 죽여선 안 된다.
                    val entry = FirmwareInformation(
                        currentFirmwareId = currentFirmwareId,
                        updateUri = updateUri?.let { raw ->
                            runCatching { URI.create(raw) }.getOrNull()
                        }
                    )
                    firmwareList.add(entry)
                }
                FirmwareUpdateInformationStatus(
                    totalCount = params[0].toUByte(),
                    firstIndex = params[1].toUByte(),
                    list = firmwareList.toList(),
                )
            }
    }
}