@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.listSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import no.nordicsemi.kotlin.mesh.core.model.Feature
import no.nordicsemi.kotlin.mesh.core.model.FeatureState
import no.nordicsemi.kotlin.mesh.core.model.Friend
import no.nordicsemi.kotlin.mesh.core.model.LowPower
import no.nordicsemi.kotlin.mesh.core.model.Proxy
import no.nordicsemi.kotlin.mesh.core.model.Relay

/**
 * CDB v1.0.1 `heartbeatPublication.features` 직렬화기.
 *
 * 스펙(`mesh-cdb-1-0-1-schema.json` §heartbeatPublication.features)은 이 필드를
 * **활성화(enabled)된 feature 의 소문자 이름 문자열 배열** 로 정의한다
 * (`enum: ["relay", "proxy", "friend", "lowPower"]`, `minItems: 0`).
 *
 * 기본 [Feature] 봉인 클래스 직렬화기는 kotlinx 다형성 blob
 * (`{"type":"no.nordicsemi....Relay","state":{...}}`)을 내보내 CDB 스키마/CHECK 제약을
 * 위반하므로, 이 직렬화기로 canonical 문자열 배열을 emit/parse 한다.
 *
 * - 직렬화: `feature.isEnabled` 인 feature 만 소문자 이름으로 배열에 포함.
 *   Disabled/Unsupported feature 는 제외. 전부 비활성이면 `[]`.
 * - 역직렬화: **두 형식 모두** backward-compatible 하게 처리한다.
 *   1. canonical(신규): 소문자 이름 문자열 배열 `["relay","proxy",...]`
 *      → 해당 feature 의 [FeatureState.Enabled].
 *   2. legacy(다형성): `[{"type":"...Relay","state":{"type":"...FeatureState.Disabled"},...}, ...]`
 *      → `type` 에서 feature 종류, `state.type` 에서 Enabled/Disabled 판별.
 *      **Enabled 인 것만** 결과에 포함 (canonical 의미와 일치 — 배열 포함 = enabled).
 *   원소별로 JsonPrimitive(canonical) vs JsonObject(legacy) 를 peek 해서 분기한다.
 *   알 수 없는 type/형식은 throw 대신 skip + stderr 경고 — 한 노드 때문에 전체
 *   mesh 로딩이 깨지는 fail-hard 회귀를 회피한다.
 *
 * read legacy / write canonical 비대칭으로 점진적 마이그레이션을 수행한다
 * (다시 저장될 때 canonical 로 정규화).
 */
internal object HeartbeatFeaturesSerializer : KSerializer<List<Feature>> {

    private const val RELAY = "relay"
    private const val PROXY = "proxy"
    private const val FRIEND = "friend"
    private const val LOW_POWER = "lowPower"

    override val descriptor: SerialDescriptor =
        listSerialDescriptor(String.serializer().descriptor)

    override fun serialize(encoder: Encoder, value: List<Feature>) {
        val names = value
            .filter { it.isEnabled }
            .mapNotNull { it.name() }
        val array = JsonArray(names.map { JsonPrimitive(it) })
        (encoder as JsonEncoder).encodeJsonElement(array)
    }

    override fun deserialize(decoder: Decoder): List<Feature> {
        val element = (decoder as JsonDecoder).decodeJsonElement()
        // 배열이 아닌 형식(예상 외) 은 throw 대신 빈 set 으로 안전 처리.
        val array = (element as? JsonArray) ?: run {
            warn("expected JSON array, got ${element::class.simpleName}: $element")
            return emptyList()
        }
        return array.mapNotNull { item -> parseFeature(item) }
    }

    /**
     * 단일 배열 원소를 [Feature] 로 변환. canonical(string) / legacy(object) 자동 분기.
     * Enabled feature 만 반환 (canonical 의미: 배열 존재 = enabled). 그 외 null → skip.
     */
    private fun parseFeature(item: JsonElement): Feature? = when (item) {
        // canonical: "relay" 등 소문자 이름 문자열 → enabled.
        is JsonPrimitive -> if (item.isString) {
            featureFromName(item.content, enabled = true)
                ?: run { warn("unknown feature name: ${item.content}"); null }
        } else {
            warn("unexpected primitive feature element: $item"); null
        }
        // legacy: {"type":"...Relay","state":{"type":"...FeatureState.Disabled"},"rawValue":N}
        is JsonObject -> parseLegacyFeature(item)
        else -> { warn("unexpected feature element: $item"); null }
    }

    /**
     * legacy 다형성 객체 파싱. `type` FQCN 끝(simpleName)에서 feature 종류,
     * `state.type` FQCN 끝에서 Enabled/Disabled 판별. **Enabled 인 것만** 반환.
     */
    private fun parseLegacyFeature(obj: JsonObject): Feature? {
        val type = (obj["type"] as? JsonPrimitive)?.contentOrNull
            ?: run { warn("legacy feature missing 'type': $obj"); return null }
        // FQCN("no.nordicsemi....Relay") → simpleName("Relay").
        val featureKind = type.substringAfterLast('.')
        // state.type = "no.nordicsemi....FeatureState.Enabled" → 끝 토큰 "Enabled".
        val stateType = (obj["state"] as? JsonObject)
            ?.get("type")
            ?.let { (it as? JsonPrimitive)?.contentOrNull }
        val enabled = stateType?.substringAfterLast('.') == "Enabled"
        // Disabled / Unsupported / state 누락 → canonical 의미상 제외(null).
        if (!enabled) return null
        val name = when (featureKind) {
            "Relay" -> RELAY
            "Proxy" -> PROXY
            "Friend" -> FRIEND
            "LowPower" -> LOW_POWER
            else -> { warn("unknown legacy feature type: $type"); return null }
        }
        return featureFromName(name, enabled = true)
    }

    private fun featureFromName(name: String, enabled: Boolean): Feature? {
        val state = if (enabled) FeatureState.Enabled else FeatureState.Disabled
        return when (name) {
            RELAY -> Relay(state)
            PROXY -> Proxy(state)
            FRIEND -> Friend(state)
            LOW_POWER -> LowPower(state)
            else -> null
        }
    }

    private fun warn(message: String) {
        System.err.println("[HeartbeatFeaturesSerializer] skip feature: $message")
    }

    private fun Feature.name(): String? = when (this) {
        is Relay -> RELAY
        is Proxy -> PROXY
        is Friend -> FRIEND
        is LowPower -> LOW_POWER
    }
}
