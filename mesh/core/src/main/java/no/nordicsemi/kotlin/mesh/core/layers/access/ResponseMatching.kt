package no.nordicsemi.kotlin.mesh.core.layers.access

import no.nordicsemi.kotlin.mesh.core.messages.BaseMeshMessage
import no.nordicsemi.kotlin.mesh.core.messages.ConfigAddressMessage
import no.nordicsemi.kotlin.mesh.core.messages.ConfigAppKeyMessage
import no.nordicsemi.kotlin.mesh.core.messages.ConfigElementMessage
import no.nordicsemi.kotlin.mesh.core.messages.ConfigModelMessage
import no.nordicsemi.kotlin.mesh.core.messages.ConfigNetKeyMessage

/**
 * simdo-fork (2026-09-29) — [response] 가 [request] 의 응답인가 (opcode·주소 외에 **요청 파라미터까지**).
 *
 * Config Status 는 요청의 파라미터를 되돌려 준다 (Mesh Profile 1.0.1 §4.3.2): Config Model App Status = ElementAddress·AppKeyIndex·
 * ModelIdentifier, Config Model Subscription Status = ElementAddress·Address·ModelIdentifier, Config Model Publication Status =
 * ElementAddress·ModelIdentifier(·Publish…), Config AppKey Status = NetKeyIndex·AppKeyIndex, Config NetKey Status·Node Identity
 * Status·Key Refresh Phase Status·Heartbeat Publication Status = NetKeyIndex, Config SIG/Vendor Model App·Subscription List =
 * ElementAddress·ModelIdentifier. 양쪽이 같은 종류의 필드를 가지면 값이 같아야 그 요청의 응답이다. 한쪽에만 있는 필드는 보지
 * 않는다 (예: Subscription Delete All·Virtual Address 요청에는 Address 필드가 없다).
 *
 * 왜: 종전에는 (보낸 주소, 응답 opcode) 만 봤다. 앱이 한 요청을 포기(15 s)해도 lib 는 그 요청을 30 s 까지 재전송하므로, 늦게 온
 * 옛 응답이 다음 요청(같은 opcode)의 성공으로 기록되고 CDB 에도 다음 요청의 값으로 반영됐다(ConfigurationClientHandler 가 request
 * 를 쓴다). 실기기 2026-09-28 17:57:11.259.
 *
 * 모델 식별은 `.id`(UInt) 로 비교한다 — `VendorModelId.equals` 는 lib 버그로 두 VendorModelId 를 같다고 보지 않는다.
 */
internal fun responseMatchesRequest(request: BaseMeshMessage, response: BaseMeshMessage): Boolean {
    if (request is ConfigElementMessage && response is ConfigElementMessage &&
        request.elementAddress.address != response.elementAddress.address
    ) return false
    if (request is ConfigModelMessage && response is ConfigModelMessage &&
        request.modelId.id != response.modelId.id
    ) return false
    if (request is ConfigAppKeyMessage && response is ConfigAppKeyMessage &&
        request.applicationKeyIndex != response.applicationKeyIndex
    ) return false
    if (request is ConfigNetKeyMessage && response is ConfigNetKeyMessage &&
        request.networkKeyIndex != response.networkKeyIndex
    ) return false
    if (request is ConfigAddressMessage && response is ConfigAddressMessage &&
        request.address != response.address
    ) return false
    return true
}
