package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import kotlin.random.Random
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-31) — emblaze 조명 모양 망 (벤치·시험 공용).
 *
 * 노드 1대 = 엘리먼트 5, 모델 24 (현장 골든 fine_3rd_floor 와 같은 구성): AppKey 0·2 바인딩, 존 그룹 구독 16, 존 그룹 발행 2.
 * 직렬화하면 노드당 약 2.3 KB — 현장 폰 CDB(600노드 1.4 MB)와 같다. 장치 키·UUID 는 난수 (현장 값 아님).
 */
@OptIn(ExperimentalUuidApi::class)
internal object EmblazeCdbFixture {

    val base: ByteArray by lazy {
        EmblazeCdbFixture::class.java.classLoader!!.getResourceAsStream("cdb_json.json")!!.readAllBytes()
    }

    /**
     * 시험용 CDB 의 키·프로비저너(·노드 10대)에 emblaze 조명 [nodeCount] 대와 존 그룹(8대당 1개)을 더한다. 주소는 [elementsPerNode]
     * 칸씩: 0x0400 부터 들어가면 시험용 노드 10대를 두고, 안 들어가면(주소 공간 끝 = 0x7FFF: 엘리먼트 5 → 6,550대, 8 → 4,095대)
     * 시험용 노드를 빼고 0x0001 부터.
     * [elementsPerNode] > 5 면 엘리먼트 5~ 는 엘리먼트 1(LC·센서, 모델 6) 과 같은 모양을 더한다.
     */
    fun network(nodeCount: Int, elementsPerNode: Int = 5, seed: Int = 7): ByteArray {
        require(elementsPerNode >= 5)
        val keepFixtureNodes = 0x0400 + nodeCount * elementsPerNode - 1 <= 0x7FFF
        val start = if (keepFixtureNodes) 0x0400 else 0x0001
        require(start + nodeCount * elementsPerNode - 1 <= 0x7FFF) { "주소 공간 넘침: $nodeCount × $elementsPerNode" }
        val rnd = Random(seed)
        val root = Json.parseToJsonElement(String(base)).jsonObject
        val zoneCount = (nodeCount / 8).coerceAtLeast(1)
        val zones = (0 until zoneCount).map { 0xC100 + it }
        val nodes = (0 until nodeCount).map { i ->
            node(i, zone = zones[i % zoneCount], rnd = rnd, address = start + i * elementsPerNode, elementsPerNode = elementsPerNode)
        }
        val fixtureNodes = if (keepFixtureNodes) root["nodes"]!!.jsonArray else JsonArray(emptyList())
        val groups = zones.map { addr ->
            buildJsonObject {
                put("name", "Zone 0x%04X".format(addr))
                put("address", "%04X".format(addr))
                put("parentAddress", "0000")
            }
        }
        return JsonObject(root.toMutableMap().apply {
            put("nodes", JsonArray(fixtureNodes + nodes))
            put("groups", JsonArray(root["groups"]!!.jsonArray + groups))
        }).toString().toByteArray()
    }

    private fun hex(rnd: Random, bytes: Int) = rnd.nextBytes(bytes).joinToString("") { "%02X".format(it) }

    private fun node(i: Int, zone: Int, rnd: Random, address: Int, elementsPerNode: Int): JsonObject {
        val zoneHex = "%04X".format(zone)
        fun model(id: String, bind: Boolean = true, sub: Boolean = false, pub: Boolean = false) = buildJsonObject {
            put("modelId", id)
            put("bind", if (bind) buildJsonArray { add(JsonPrimitive(0)); add(JsonPrimitive(2)) } else JsonArray(emptyList()))
            put("subscribe", if (sub) JsonArray(listOf(JsonPrimitive(zoneHex))) else JsonArray(emptyList()))
            if (pub) put("publish", buildJsonObject {
                put("address", zoneHex)
                put("index", 2)
                put("ttl", 15)
                put("period", buildJsonObject { put("numberOfSteps", 0); put("resolution", 100) })
                put("credentials", 0)
                put("retransmit", buildJsonObject { put("count", 0); put("interval", 50) })
            })
        }
        fun element(index: Int, models: List<JsonElement>) = buildJsonObject {
            put("index", index)
            put("location", "0000")
            put("models", JsonArray(models))
        }
        val elements = listOf(
            element(0, listOf(
                model("0000", bind = false), model("0002", bind = false),
                model("1002", sub = true), model("1000", sub = true), model("1004", sub = true), model("1006", sub = true),
                model("1007", sub = true), model("1300", sub = true), model("1301", sub = true), model("1203", sub = true),
                model("1204", sub = true), model("1302", pub = true),
            )),
            element(1, listOf(
                model("1000", sub = true), model("130F", sub = true), model("1310", sub = true),
                model("1100", pub = true), model("1101"), model("1302"),
            )),
            element(2, listOf(model("0059E100", sub = true))),
            element(3, listOf(
                model("0059E000", sub = true), model("0059E010", sub = true), model("0059E020"), model("0059E021", sub = true),
            )),
            element(4, listOf(model("00590105", bind = false))),
        ) + (5 until elementsPerNode).map { e ->
            element(e, listOf(
                model("1000", sub = true), model("130F", sub = true), model("1310", sub = true),
                model("1100", pub = true), model("1101"), model("1302"),
            ))
        }
        val uuid = Uuid.random().toString().uppercase()
        return buildJsonObject {
            put("UUID", uuid)
            put("name", "emblazeL-%s-%02d".format(uuid.take(4), i % 100))
            put("unicastAddress", "%04X".format(address))
            put("deviceKey", hex(rnd, 16))
            put("security", "insecure")
            put("netKeys", buildJsonArray { add(buildJsonObject { put("index", 0); put("updated", false) }) })
            put("configComplete", true)
            put("cid", "0059")
            put("pid", "1220")
            put("vid", "1107")
            put("crpl", "00FF")
            put("features", buildJsonObject { put("relay", 1); put("proxy", 1); put("friend", 2); put("lowPower", 2) })
            put("defaultTTL", 20)
            put("networkTransmit", buildJsonObject { put("count", 3); put("interval", 50) })
            put("relayRetransmit", buildJsonObject { put("count", 5); put("interval", 50) })
            put("appKeys", buildJsonArray {
                add(buildJsonObject { put("index", 0); put("updated", false) })
                add(buildJsonObject { put("index", 2); put("updated", false) })
            })
            put("elements", JsonArray(elements))
            put("excluded", false)
        }
    }
}
