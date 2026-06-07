@file:OptIn(ExperimentalSerializationApi::class)

package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.decodeFromStream
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.NetworkConfiguration
import java.io.ByteArrayInputStream

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
    @OptIn(ExperimentalSerializationApi::class)
    internal fun deserialize(array: ByteArray) = jsonSerializer.run {
        val networkElement: JsonElement = decodeFromStream(ByteArrayInputStream(array)
            .also { stream -> stream.close() })
        // Validates the json with the required properties.
        networkElement.jsonObject.let { networkObject ->
            require(schema == networkObject[KEY_SCHEMA]?.jsonPrimitive?.content) {
                "Invalid Json schema!"
            }
            // ID does not need checking as it has changed in the past and may change again.
            // require(id == networkObject[KEY_ID]?.jsonPrimitive?.content) { "Invalid Json id!" }
            require(
                networkObject[KEY_VERSION]?.jsonPrimitive?.content?.matches(VERSION_PATTERN)
                    ?: false
            ) { "Invalid version!" }
        }
        decodeFromJsonElement<MeshNetwork>(networkElement).apply {
            // Assign network reference to access parent network within the object.
            _networkKeys.forEach {
                it.network = this
            }
            _applicationKeys.forEach {
                it.network = this
            }
            _groups.forEach {
                it.network = this
            }
            _scenes.forEach {
                it.network = this
            }
            _provisioners.forEach {
                it.network = this
            }
            _nodes.forEach { node ->
                node.network = this
                node.elements.forEach { element ->
                    element.parentNode = node
                    element.models.forEach { model ->
                        model.parentElement = element
                    }
                }
            }
            _networkExclusions.forEach {
                it.network = this
            }
        }
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
    internal fun serialize(network: MeshNetwork, configuration: NetworkConfiguration) =
        network.withNodesLock {
            JsonObject(content = jsonSerializer.run {
                encodeToJsonElement(value = buildMap<String, JsonElement> {
                    put(KEY_SCHEMA, JsonPrimitive(value = schema))
                    put(KEY_ID, JsonPrimitive(value = id))
                    put(KEY_VERSION, JsonPrimitive(value = version))
                }).jsonObject + encodeToJsonElement(network.apply(configuration)).jsonObject
            })
        }
}
