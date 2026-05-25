package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.serialization.json.Json
import no.nordicsemi.kotlin.mesh.core.model.serialization.HeartbeatFeaturesSerializer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * CDB v1.0.1 `heartbeatPublication.features` 직렬화기 검증.
 *
 * 스펙: 활성화된 feature 의 소문자 이름 문자열 배열 (`["relay","proxy","friend","lowPower"]`).
 * 다형성 blob (`{"type":"no.nordicsemi....Relay","state":{...}}`) 누수 회귀를 차단한다.
 */
class HeartbeatFeaturesSerializerTest {

    private val json = Json {
        encodeDefaults = true
        explicitNulls = false
        ignoreUnknownKeys = true
    }

    private val serializer = HeartbeatFeaturesSerializer

    @Test
    fun `전부 Disabled 면 빈 배열을 emit (live node case)`() {
        val features: List<Feature> = listOf(
            Relay(FeatureState.Disabled),
            Proxy(FeatureState.Disabled),
            Friend(FeatureState.Disabled),
            LowPower(FeatureState.Disabled),
        )
        val out = json.encodeToString(serializer, features)
        assertEquals("[]", out)
    }

    @Test
    fun `Unsupported feature 도 배열에서 제외`() {
        val features: List<Feature> = listOf(
            Relay(FeatureState.Unsupported),
            Proxy(FeatureState.Unsupported),
            Friend(FeatureState.Unsupported),
            LowPower(FeatureState.Unsupported),
        )
        val out = json.encodeToString(serializer, features)
        assertEquals("[]", out)
    }

    @Test
    fun `Enabled feature 만 소문자 이름 배열로 emit`() {
        val features: List<Feature> = listOf(
            Relay(FeatureState.Enabled),
            Proxy(FeatureState.Disabled),
            Friend(FeatureState.Unsupported),
            LowPower(FeatureState.Enabled),
        )
        val out = json.encodeToString(serializer, features)
        // relay + lowPower 만 enabled. lowPower 는 camelCase 그대로.
        assertEquals("""["relay","lowPower"]""", out)
    }

    @Test
    fun `다형성 type 필드를 절대 emit 하지 않음`() {
        val features: List<Feature> = listOf(
            Relay(FeatureState.Enabled),
            Proxy(FeatureState.Enabled),
        )
        val out = json.encodeToString(serializer, features)
        assertTrue(!out.contains("type"), "type 필드 누수: $out")
        assertTrue(!out.contains("no.nordicsemi"), "FQCN 누수: $out")
        assertTrue(!out.contains("state"), "state 객체 누수: $out")
    }

    @Test
    fun `소문자 이름 배열을 enabled feature 로 역직렬화`() {
        val parsed = json.decodeFromString(serializer, """["relay","friend"]""")
        // relay, friend 만 복원되며 모두 Enabled.
        assertEquals(2, parsed.size)
        assertTrue(parsed.any { it is Relay && it.isEnabled })
        assertTrue(parsed.any { it is Friend && it.isEnabled })
    }

    @Test
    fun `빈 배열은 빈 feature list 로 역직렬화`() {
        val parsed = json.decodeFromString(serializer, "[]")
        assertTrue(parsed.isEmpty())
    }

    @Test
    fun `round-trip — enabled set 보존`() {
        val original = json.decodeFromString(serializer, """["proxy"]""")
        val reEmitted = json.encodeToString(serializer, original)
        assertEquals("""["proxy"]""", reEmitted)
    }

    // --- backward-compat: legacy 다형성 객체 배열 역직렬화 ---
    // fix 이전 old serializer 가 저장한 CDB / 서버 CDB / 기존 config 된 노드의 형식.
    // 라이브 회귀 증거(PID 10913)와 동일한 shape.

    private val legacyFqcn = "no.nordicsemi.kotlin.mesh.core.model"

    private fun legacyFeature(type: String, state: String, raw: Int) =
        """{"type":"$legacyFqcn.$type","state":{"type":"$legacyFqcn.FeatureState.$state"},"rawValue":$raw}"""

    @Test
    fun `legacy 폴리모픽 배열 — 전부 Disabled 면 빈 set (라이브 회귀 case)`() {
        // 라이브 PID 10913 의 정확한 입력: 4개 전부 Disabled.
        val legacy = """[
            ${legacyFeature("Relay", "Disabled", 0)},
            ${legacyFeature("Proxy", "Disabled", 0)},
            ${legacyFeature("Friend", "Disabled", 0)},
            ${legacyFeature("LowPower", "Disabled", 0)}
        ]"""
        val parsed = json.decodeFromString(serializer, legacy)
        assertTrue(parsed.isEmpty(), "전부 Disabled 인 legacy 배열은 빈 set 이어야 함: $parsed")
    }

    @Test
    fun `legacy 폴리모픽 배열 — 일부 Enabled 면 해당 feature 만 복원`() {
        val legacy = """[
            ${legacyFeature("Relay", "Enabled", 1)},
            ${legacyFeature("Proxy", "Disabled", 0)},
            ${legacyFeature("Friend", "Unsupported", 2)},
            ${legacyFeature("LowPower", "Enabled", 1)}
        ]"""
        val parsed = json.decodeFromString(serializer, legacy)
        assertEquals(2, parsed.size)
        assertTrue(parsed.any { it is Relay && it.isEnabled })
        assertTrue(parsed.any { it is LowPower && it.isEnabled })
        assertTrue(parsed.none { it is Proxy }, "Disabled Proxy 누수")
        assertTrue(parsed.none { it is Friend }, "Unsupported Friend 누수")
    }

    @Test
    fun `legacy round-trip — legacy in → canonical out`() {
        val legacy = """[
            ${legacyFeature("Relay", "Enabled", 1)},
            ${legacyFeature("Proxy", "Disabled", 0)}
        ]"""
        val parsed = json.decodeFromString(serializer, legacy)
        val reEmitted = json.encodeToString(serializer, parsed)
        // write 는 항상 canonical → 다시 저장될 때 정규화.
        assertEquals("""["relay"]""", reEmitted)
        assertTrue(!reEmitted.contains("type"), "canonical out 에 type 누수: $reEmitted")
    }

    @Test
    fun `canonical 과 legacy 혼재 배열도 안전 처리`() {
        val mixed = """[
            "relay",
            ${legacyFeature("Proxy", "Enabled", 1)},
            ${legacyFeature("Friend", "Disabled", 0)}
        ]"""
        val parsed = json.decodeFromString(serializer, mixed)
        // canonical relay(enabled) + legacy proxy(enabled). friend(disabled) 제외.
        assertEquals(2, parsed.size)
        assertTrue(parsed.any { it is Relay && it.isEnabled })
        assertTrue(parsed.any { it is Proxy && it.isEnabled })
    }

    @Test
    fun `알 수 없는 형식은 throw 대신 skip — 전체 로딩 깨지지 않음`() {
        // unknown canonical name + unknown legacy type + state 누락 + 비배열 원소.
        val weird = """[
            "elephant",
            {"type":"$legacyFqcn.Wombat","state":{"type":"$legacyFqcn.FeatureState.Enabled"},"rawValue":0},
            {"type":"$legacyFqcn.Relay","rawValue":0},
            "relay"
        ]"""
        // throw 없이 정상 반환되어야 함 (fail-hard 회귀 차단).
        val parsed = json.decodeFromString(serializer, weird)
        // 살아남는 건 마지막 canonical "relay" 만.
        assertEquals(1, parsed.size)
        assertTrue(parsed.single() is Relay)
    }

    @Test
    fun `legacy 빈 배열도 빈 set`() {
        assertTrue(json.decodeFromString(serializer, "[]").isEmpty())
    }

    @Test
    fun `HeartbeatPublication 전체 직렬화 시 features 가 문자열 배열`() {
        // 라이브 노드 = sink group destination, 전부 Disabled → features 는 [].
        val publication = HeartbeatPublication(
            address = GroupAddress(0xC0F0u),
            period = 64u,
            ttl = 7u,
            index = 0u,
            features = listOf(
                Relay(FeatureState.Disabled),
                Proxy(FeatureState.Disabled),
                Friend(FeatureState.Disabled),
                LowPower(FeatureState.Disabled),
            ),
        )
        val out = json.encodeToString(HeartbeatPublication.serializer(), publication)
        assertTrue(out.contains("\"features\":[]"), "features 가 빈 배열이 아님: $out")
        assertTrue(!out.contains("type"), "polymorphic type 누수: $out")
    }
}
