@file:OptIn(ExperimentalSerializationApi::class)

package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.decodeFromStream
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import no.nordicsemi.kotlin.mesh.core.PersistenceDiagnostics
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.NetworkConfiguration
import java.io.BufferedWriter
import java.io.ByteArrayInputStream
import java.io.OutputStream
import java.io.OutputStreamWriter

internal object MeshNetworkSerializer {

    private val jsonSerializer = Json {
        encodeDefaults = true       // Encodes default values of properties.
        explicitNulls = false       // Avoids encoding null values.
        ignoreUnknownKeys = true    // Ignores the keys not available in the mesh network mode.
    }

    private const val KEY_SCHEMA = "${'$'}schema"
    private const val KEY_ID = "id"
    private const val KEY_VERSION = "version"
    private const val schema = "http://json-schema.org/draft-04/schema#"
    private const val id =
        "http://www.bluetooth.com/specifications/assigned-numbers/meshprofile/cdb-schema.json#"
    private const val version = "1.0.0"
    private val VERSION_PATTERN = Regex("^[0-9]\\.[0-9]\\.[0-9]\$")

    /**
     * Deserializes the array in to a mesh network.
     *
     * @param array in to a mesh network.
     */
    // simdo-fork (2026-10-31) — 망 전체 JsonElement 트리를 만들지 않고 디코드한다. 종전은 바이트 → 트리(출력의 약 15배, 6,550노드
    // 15 MB → 224 MB) → 모델 순서라 올리기 1회 할당이 513 MB 였다 (CdbScaleBenchTest). 지금: (1) 헤더(`$schema`·`version`) 만 담는 작은
    // 클래스로 한 번 훑고(나머지 키는 ignoreUnknownKeys 로 건너뜀, 트리 없음) 검사, (2) 같은 바이트를 모델로 바로 디코드. 검사·결과는
    // 종전과 같다 (MeshNetworkSerializerNodeSliceTest 가 종전 경로와 비교).
    @kotlinx.serialization.Serializable
    private class CdbHeader(
        @kotlinx.serialization.SerialName(KEY_SCHEMA) val schema: JsonElement? = null,
        @kotlinx.serialization.SerialName(KEY_VERSION) val version: JsonElement? = null,
    )

    @OptIn(ExperimentalSerializationApi::class)
    internal fun deserialize(array: ByteArray) = jsonSerializer.run {
        val header = decodeFromStream(CdbHeader.serializer(), ByteArrayInputStream(array))
        // Validates the json with the required properties. (종전과 같은 판정: 원시값이 아니면 jsonPrimitive 가 예외.)
        require(schema == header.schema?.jsonPrimitive?.content) {
            "Invalid Json schema!"
        }
        // ID does not need checking as it has changed in the past and may change again.
        require(header.version?.jsonPrimitive?.content?.matches(VERSION_PATTERN) ?: false) { "Invalid version!" }
        decodeFromStream(MeshNetwork.serializer(), ByteArrayInputStream(array)).apply {
            // simdo-fork (2026-06-08, Phase 3) — parent 참조 복원을 MeshNetwork.rewireAfterDeserialize() 로 이동.
            // `_nodes` private 봉인으로 serializer 의 raw 순회가 컴파일 불가 → MeshNetwork 멤버(intra-class)로 위임.
            // 의미 동일(network 링크/elements·models parent 재설정).
            rewireAfterDeserialize()
        }
    }

    /** simdo-fork (2026-10-31) — 종전 디코드 (트리 경유). 시험에서 새 디코드와 결과를 비교하는 데만 쓴다. */
    @OptIn(ExperimentalSerializationApi::class)
    internal fun deserializeViaTree(array: ByteArray) = jsonSerializer.run {
        val networkElement: JsonElement = decodeFromStream(ByteArrayInputStream(array))
        networkElement.jsonObject.let { networkObject ->
            require(schema == networkObject[KEY_SCHEMA]?.jsonPrimitive?.content) { "Invalid Json schema!" }
            require(
                networkObject[KEY_VERSION]?.jsonPrimitive?.content?.matches(VERSION_PATTERN) ?: false
            ) { "Invalid version!" }
        }
        decodeFromJsonElement<MeshNetwork>(networkElement).apply { rewireAfterDeserialize() }
    }

    /**
     * Serializes the given mesh network to a JsonObject.
     *
     * @param network          MeshNetwork to be serialized.
     * @param configuration    Configuration to be applied when serializing.
     */
    // simdo-fork (2026-06-08, P6) — kotlinx 인코딩(network.apply(config) → backing `_nodes` 직접 순회)을
    // MeshNetwork.withNodesLock 으로 감싼다. autosave 스레드의 이 트래버설이 nodesMonitor-only [add]/[remove]
    // 구조 변이와 동시에 돌면 ConcurrentModificationException 이 나므로, _nodes 컨테이너의 단일 guardian 인
    // nodesMonitor 아래에서 직렬화한다(Partial 경로의 _nodes filter-reassign 도 같은 lock 으로 배타).
    //
    // simdo-fork (2026-10-26, #80) — Full 은 잠금을 노드 단위로 쪼갠다. 종전엔 망 전체 인코딩(600노드 1.4 MB, 화면 끔 3.8 s)
    // 내내 nodesMonitor 를 쥐어, 수신 PDU 마다 그 잠금으로 노드를 찾는 경로(Provisioner.node → MeshNetwork.node)가 그동안 멈췄다.
    // 지금: (1) nodesMonitor 아래에서 노드 목록 스냅샷 + 나머지(껍데기: 키·프로비저너·그룹·장면·제외 목록) 인코딩, (2) 노드마다
    // nodesMonitor 를 잡고 그 노드만 인코딩, (3) 잠금 밖에서 껍데기의 `nodes` 자리에 끼운다. 출력은 종전과 바이트 단위로 같다
    // (MeshNetworkSerializerNodeSliceTest). CME 보호(P6 의 이유)는 그대로: `_nodes` 순회는 (1) 의 스냅샷 하나뿐이고 add/remove
    // cascade 가 건드리는 컬렉션은 (1) 에서 같은 잠금 아래 인코딩한다. Partial 은 apply() 가 망 자체를 거르므로 종전 그대로 통째 잠근다.
    internal fun serialize(network: MeshNetwork, configuration: NetworkConfiguration): JsonObject =
        when (configuration) {
            is NetworkConfiguration.Full -> {
                val meter = LockHoldMeter()
                val (nodes, shell) = meter.hold { network.withNodesLock { captureShell(network) } }
                val encoded = nodes.map { node -> meter.hold { network.withNodesLock { encodeNode(node) } } }
                PersistenceDiagnostics.recordSerializationLock(meter.totalNanos, meter.maxNanos)
                assemble(shell = shell, nodes = encoded)
            }

            is NetworkConfiguration.Partial -> network.withNodesLock {
                JsonObject(content = jsonSerializer.run {
                    header() + encodeToJsonElement(network.apply(configuration)).jsonObject
                })
            }
        }

    /**
     * simdo-fork (2026-10-26, #80) — 망 전체(Full)를 CDB 잠금([MeshNetwork.cdbMutex]) 을 노드 단위로 잡으며 직렬화한다. 저장(save)용.
     *
     * 종전 저장은 `withCdbLock { export() }` 로 인코딩 내내 cdbMutex 를 쥐었다 — 그동안 Config Status 반영(AccessLayer, cdbMutex 안)이
     * 하위 전송 잠금 안에서 줄을 서 수신 전체가 멈췄다 (화면 끔 3.8 s, 10 s 주기 응답 틈 4.0 s, 2026-10-26 실측). 지금은
     * 껍데기(노드 목록 스냅샷 포함)를 cdbMutex + nodesMonitor 아래 한 번, 노드는 하나씩 cdbMutex 아래에서 인코딩한다. 노드 사이에
     * 기다리던 수신 처리가 들어온다 (코루틴 Mutex 는 줄 선 순서대로 넘겨준다).
     *
     * 노드 인코딩 구간에 nodesMonitor 는 잡지 않는다: 노드 내부를 바꾸는 라이브러리 경로(설정 응답 반영·NPPI 종결·프로비저닝 add)는
     * 전부 cdbMutex 안이고, nodesMonitor 만 잡는 add/remove 는 노드 내부(직렬화 필드)를 바꾸지 않는다. 그래서 수신 PDU 의 노드 찾기는
     * 노드 인코딩 중에도 기다리지 않는다.
     *
     * 일관성: 파일 하나 안의 노드들은 서로 다른 순간(이 직렬화가 걸리는 시간 안)의 상태일 수 있다. 노드 하나는 한 순간의 상태다.
     * 그 사이 바뀐 것은 더러움 표시(markNetworkDirty)나 앱의 명시 save() 로 다음 저장에 들어간다 — "파일이 메모리보다 뒤처지는 창"
     * 은 종전과 같이 합침 저장 상한(saveMaxDelay)으로 묶인다.
     *
     * cdbMutex 는 비재진입이다 — 이미 쥔 채 부르면 교착한다.
     */
    internal suspend fun serializeWithCdbLock(network: MeshNetwork): JsonObject {
        val meter = LockHoldMeter()
        val (nodes, shell) = network.cdbMutex.withLock {
            meter.hold { network.withNodesLock { captureShell(network) } }
        }
        val encoded = ArrayList<JsonElement>(nodes.size)
        for (node in nodes) {
            encoded += network.cdbMutex.withLock { meter.hold { encodeNode(node) } }
        }
        PersistenceDiagnostics.recordSerializationLock(meter.totalNanos, meter.maxNanos)
        return assemble(shell = shell, nodes = encoded)
    }

    /**
     * simdo-fork (2026-10-31) — [serializeWithCdbLock] 과 같은 바이트를 [out] 에 UTF-8 로 바로 쓴다. 저장(save)용.
     *
     * 종전 저장은 망 전체를 JsonElement 트리로 만든 뒤 문자열 → 바이트 배열로 바꿔 넘겼다. 노드 수에 비례하는 큰 덩어리 셋이 한꺼번에
     * 살아 있었다 (JVM 벤치 3,000노드: 저장 1회 할당 ≈ 출력의 수십 배, 트리만 수십 MB — `CdbScaleBenchTest`). 폰 힙 한도 가까이에서
     * 이 순간 할당이 OOM·GC 정지를 만든다 (2026-10-30 Flip7 1,400노드 OOM). 여기서는 노드를 하나씩 문자열로 인코딩해 곧바로 흘려 보낸다:
     * 순간 할당은 노드 1대분 + 쓰기 버퍼뿐이다.
     *
     * 잠금은 [serializeWithCdbLock] 과 같다: 껍데기 = cdbMutex + nodesMonitor 아래 한 번, 노드 = 하나씩 cdbMutex 아래. 쓰기(파일 IO)는
     * 잠금 밖이다. [out] 은 닫지 않는다 (호출자 몫). 도중에 예외가 나면 [out] 에는 잘린 JSON 이 남는다 — 호출자는 그 출력을 버려야 한다
     * (앱 저장소 = 임시 파일에 쓰고 끝까지 썼을 때만 이름 바꿈).
     *
     * cdbMutex 는 비재진입이다 — 이미 쥔 채 부르면 교착한다.
     */
    internal suspend fun writeWithCdbLock(network: MeshNetwork, out: OutputStream) {
        val meter = LockHoldMeter()
        val (nodes, shell) = network.cdbMutex.withLock {
            meter.hold { network.withNodesLock { captureShell(network) } }
        }
        writeAssembled(out = out, shell = shell, nodes = nodes) { node ->
            network.cdbMutex.withLock { meter.hold { encodeNodeToString(node) } }
        }
        PersistenceDiagnostics.recordSerializationLock(meter.totalNanos, meter.maxNanos)
    }

    /**
     * simdo-fork (2026-10-31) — [serialize] (Full) 와 같은 바이트를 [out] 에 바로 쓴다. 잠금은 [serialize] 와 같다 (nodesMonitor 를 노드
     * 단위로, cdbMutex 없음). 비 suspend export 용.
     */
    internal fun writeFull(network: MeshNetwork, out: OutputStream) {
        val meter = LockHoldMeter()
        val (nodes, shell) = meter.hold { network.withNodesLock { captureShell(network) } }
        writeAssembled(out = out, shell = shell, nodes = nodes) { node ->
            meter.hold { network.withNodesLock { encodeNodeToString(node) } }
        }
        PersistenceDiagnostics.recordSerializationLock(meter.totalNanos, meter.maxNanos)
    }

    /**
     * [assemble] 결과의 `toString()` 과 같은 문자를 [out] 에 쓴다: 헤더 + 껍데기(키 순서 그대로), `nodes` 자리는 [encode] 로 노드마다.
     * 인라인이라 [encode] 안에서 suspend 잠금을 잡을 수 있다. 노드 인코딩([encode], 잠금 안) 과 쓰기(잠금 밖) 를 번갈아 한다.
     */
    private inline fun writeAssembled(
        out: OutputStream,
        shell: JsonObject,
        nodes: List<Node>,
        encode: (Node) -> String,
    ) {
        val writer = BufferedWriter(OutputStreamWriter(out, Charsets.UTF_8), WRITE_BUFFER_CHARS)
        writer.write('{'.code)
        var firstKey = true
        for ((key, value) in layout(shell)) {
            if (!firstKey) writer.write(','.code)
            firstKey = false
            // 키는 JsonObject.toString 과 같은 따옴표·이스케이프로.
            writer.write(JsonPrimitive(key).toString())
            writer.write(':'.code)
            if (value === NODES_SLOT) {
                writer.write('['.code)
                for ((i, node) in nodes.withIndex()) {
                    val json = encode(node)
                    if (i > 0) writer.write(','.code)
                    writer.write(json)
                }
                writer.write(']'.code)
            } else {
                writer.write(value.toString())
            }
        }
        writer.write('}'.code)
        writer.flush()
    }

    /** simdo-fork (2026-10-31) — 저널용: nodesMonitor 아래에서 부른다 (노드 목록 스냅숏 + 노드 없는 껍데기). */
    internal fun captureShellForJournal(network: MeshNetwork): Pair<List<Node>, JsonObject> = captureShell(network)

    /**
     * simdo-fork (2026-10-31) — 전체 출력([writeAssembled]) 을 `nodes` 배열 안쪽에서 둘로 가른다: 앞 = `"nodes":[` 까지, 뒤 = `]` 부터.
     * 앞 + 노드 JSON 들을 쉼표로 이은 것 + 뒤 = 전체 출력 (CdbJournal 이 이렇게 다시 짠다).
     */
    internal fun shellParts(shell: JsonObject): Pair<String, String> {
        val prefix = StringBuilder(4 * 1024)
        val suffix = StringBuilder(4 * 1024)
        var current = prefix
        current.append('{')
        var firstKey = true
        for ((key, value) in layout(shell)) {
            if (!firstKey) current.append(',')
            firstKey = false
            current.append(JsonPrimitive(key).toString())
            current.append(':')
            if (value === NODES_SLOT) {
                current.append('[')
                current = suffix
                current.append(']')
            } else {
                current.append(value.toString())
            }
        }
        current.append('}')
        return prefix.toString() to suffix.toString()
    }

    /** [assemble] 과 같은 키 순서. `nodes` 자리는 [NODES_SLOT]. */
    private fun layout(shell: JsonObject): Map<String, JsonElement> {
        val content = LinkedHashMap<String, JsonElement>(shell.size + 3)
        content.putAll(header())
        for ((key, value) in shell) {
            content[key] = if (key == KEY_NODES) NODES_SLOT else value
        }
        if (KEY_NODES !in shell) content[KEY_NODES] = NODES_SLOT
        return content
    }

    /** `nodes` 자리 표시 (동일성으로만 비교한다). */
    private val NODES_SLOT: JsonElement = JsonArray(emptyList())

    private const val WRITE_BUFFER_CHARS = 32 * 1024

    /**
     * simdo-fork (2026-10-31) — [encodeNode] 의 `toString()` 과 같은 문자열을 트리 없이 바로 인코딩한다 (kotlinx 스트리밍 인코더, 같은
     * 설정). 같은 결과임은 `MeshNetworkSerializerNodeSliceTest` 가 잠근다. 예외 재시도도 [encodeNode] 와 같다.
     */
    internal fun encodeNodeToString(node: Node): String {
        var last: RuntimeException? = null
        repeat(NODE_ENCODE_ATTEMPTS) {
            try {
                return jsonSerializer.encodeToString(Node.serializer(), node)
            } catch (e: ConcurrentModificationException) {
                last = e
            } catch (e: NoSuchElementException) {
                last = e
            } catch (e: IndexOutOfBoundsException) {
                last = e
            }
        }
        throw last!!
    }

    /**
     * simdo-fork (2026-10-26, #80) — 노드 하나를 CDB 노드 JSON 으로. 전체 export 의 `nodes[]` 원소와 같다
     * (MeshNetworkSerializerNodeSliceTest). 잠금은 호출자가 잡는다.
     *
     * 잠금 없이 노드 내부를 바꾸는 앱 경로가 남아 있을 수 있어, 인코딩 중 리스트가 바뀌어 난 예외는 그 노드만 몇 번 다시 인코딩한다
     * (노드 하나는 수 ms 라 다시 해도 싸다). 그래도 실패하면 예외를 그대로 올린다.
     */
    internal fun encodeNode(node: Node): JsonElement {
        var last: RuntimeException? = null
        repeat(NODE_ENCODE_ATTEMPTS) {
            try {
                return jsonSerializer.encodeToJsonElement(Node.serializer(), node)
            } catch (e: ConcurrentModificationException) {
                last = e
            } catch (e: NoSuchElementException) {
                last = e
            } catch (e: IndexOutOfBoundsException) {
                last = e
            }
        }
        throw last!!
    }

    /**
     * simdo-fork (2026-10-28, simdo #99) — [encodeNode] 출력(노드 JSON 문자열) 을 노드로. 같은 직렬화기 설정(모르는 키 무시).
     * 망 부모 연결은 `MeshNetwork.add(node)` 가 한다.
     */
    internal fun decodeNode(json: String): Node = jsonSerializer.decodeFromString(Node.serializer(), json)

    private const val NODE_ENCODE_ATTEMPTS = 3
    private const val KEY_NODES = "nodes"

    private fun header(): JsonObject = JsonObject(
        linkedMapOf(
            KEY_SCHEMA to JsonPrimitive(value = schema),
            KEY_ID to JsonPrimitive(value = id),
            KEY_VERSION to JsonPrimitive(value = version),
        )
    )

    /** nodesMonitor 아래에서 부른다: 노드 목록 스냅샷 + `nodes` 를 빈 배열로 낸 망 인코딩. */
    private fun captureShell(network: MeshNetwork): Pair<List<Node>, JsonObject> {
        val nodes = network.nodes
        val shell = NodeListFieldSerializer.withoutNodes {
            jsonSerializer.encodeToJsonElement(network).jsonObject
        }
        return nodes to shell
    }

    /** 헤더 + 껍데기(키 순서 그대로) 에 `nodes` 자리만 노드별 인코딩으로 바꾼다. 잠금 밖. */
    private fun assemble(shell: JsonObject, nodes: List<JsonElement>): JsonObject {
        val content = LinkedHashMap<String, JsonElement>(shell.size + 3)
        content.putAll(header())
        for ((key, value) in shell) {
            content[key] = if (key == KEY_NODES) JsonArray(nodes) else value
        }
        if (KEY_NODES !in shell) content[KEY_NODES] = JsonArray(nodes)
        return JsonObject(content)
    }

    /** 직렬화가 잠금을 쥔 시간 (누적·한 번 최장). 한 직렬화 안에서 순서대로만 쓴다. */
    private class LockHoldMeter {
        var totalNanos = 0L
            private set
        var maxNanos = 0L
            private set

        inline fun <T> hold(block: () -> T): T {
            val t0 = System.nanoTime()
            try {
                return block()
            } finally {
                val held = System.nanoTime() - t0
                totalNanos += held
                if (held > maxNanos) maxNanos = held
            }
        }
    }
}
