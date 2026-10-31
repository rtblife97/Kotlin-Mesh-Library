package no.nordicsemi.kotlin.mesh.core

import java.io.ByteArrayOutputStream
import java.io.OutputStream

/**
 * Allows the users to save the mesh network information in a custom location.
 */
interface Storage {
    /**
     * Loads the Mesh Network from a user specified storage.
     */
    suspend fun load(): ByteArray

    /**
     * Saves the Mesh Network in a user specified storage.
     */
    suspend fun save(network: ByteArray)

    /**
     * simdo-fork (2026-10-31) — 망을 스트림으로 저장한다. 라이브러리 저장([MeshNetworkManager.save], 합침 저장)은 이것만 부른다.
     *
     * [writer] 가 CDB JSON 전체를 주어진 스트림에 UTF-8 로 쓴다 (노드 하나씩, 노드마다 CDB 잠금). 망 전체를 바이트 배열로 만들지 않으므로
     * 저장 1회의 순간 메모리가 노드 수와 무관하다 — 파일에 바로 쓰는 구현을 권한다. [writer] 가 예외를 던지면 스트림에는 잘린 JSON 이
     * 남으므로 구현은 그 출력을 버려야 한다 (임시 파일에 쓰고 끝까지 썼을 때만 바꿔치기).
     *
     * 기본 구현은 출력을 바이트 배열로 모아 [save] 에 넘긴다 — 이 메서드를 구현하지 않은 저장소도 종전처럼 동작한다 (망 전체 JsonElement
     * 트리·문자열은 여전히 만들지 않는다).
     */
    suspend fun saveStreaming(writer: suspend (OutputStream) -> Unit) {
        val buffer = ByteArrayOutputStream(64 * 1024)
        writer(buffer)
        save(network = buffer.toByteArray())
    }
}
