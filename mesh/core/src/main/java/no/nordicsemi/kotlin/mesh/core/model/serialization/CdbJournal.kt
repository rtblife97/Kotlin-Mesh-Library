package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.coroutines.sync.withLock
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.NetworkChangeLog
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.io.OutputStreamWriter
import kotlin.uuid.ExperimentalUuidApi

/**
 * simdo-fork (2026-10-31) — 노드 단위 증분 저장의 저널 형식 ([no.nordicsemi.kotlin.mesh.core.JournalStorage]).
 *
 * UTF-8 텍스트, 한 줄 = 한 기록 (CDB JSON 은 압축 출력이라 문자열 안의 줄바꿈·탭도 이스케이프된다 — 줄·탭이 구분자로 안전하다):
 *  - `S<탭>앞<탭>뒤` 껍데기. 앞 = 망 JSON 의 `"nodes":[` 까지, 뒤 = `]` 부터 끝까지 (헤더·키 순서는 전체 저장과 같다).
 *  - `N<탭>UUID<탭>노드 JSON` 노드 넣기·바꾸기. 있던 노드는 제자리, 없던 노드는 끝에 붙인다.
 *  - `D<탭>UUID` 노드 지우기.
 *  - `C` 묶음 끝 (커밋). 올릴 때 이 줄이 없는 마지막 묶음은 버린다.
 * 파일 = 스냅숏 묶음(S + 모든 노드 N + C) 하나 + 증분 묶음(S + D… + N… + C) 들.
 *
 * 올리기([assemble]) 는 끝난 묶음을 차례로 접어 전체 CDB JSON 바이트를 다시 짠다 — 결과는 같은 순간의 [MeshNetworkSerializer.writeFull]
 * 출력과 바이트 단위로 같다 (CdbJournalTest). 노드는 마지막 상태 문자열만 들고 있으므로 올리는 동안 메모리 ≈ 전체 JSON 크기.
 */
@OptIn(ExperimentalUuidApi::class)
internal object CdbJournal {

    private const val SHELL = 'S'
    private const val NODE = 'N'
    private const val DELETE = 'D'
    private const val COMMIT = 'C'
    private const val TAB = '\t'
    private const val NL = '\n'

    /** 전체 스냅숏 묶음을 [out] 에 쓴다. 잠금은 [MeshNetworkSerializer.writeWithCdbLock] 과 같다 (노드마다 cdbMutex, 쓰기는 잠금 밖). */
    suspend fun writeSnapshot(network: MeshNetwork, out: OutputStream) {
        val (nodes, shell) = network.cdbMutex.withLock {
            network.withNodesLock { MeshNetworkSerializer.captureShellForJournal(network) }
        }
        val writer = BufferedWriter(OutputStreamWriter(out, Charsets.UTF_8), 32 * 1024)
        writeShell(writer, shell)
        for (node in nodes) {
            val json = network.cdbMutex.withLock { MeshNetworkSerializer.encodeNodeToString(node) }
            writer.write(NODE.code)
            writer.write(TAB.code)
            writer.write(node.uuid.toString())
            writer.write(TAB.code)
            writer.write(json)
            writer.write(NL.code)
        }
        writer.write(COMMIT.code)
        writer.write(NL.code)
        writer.flush()
    }

    /**
     * 증분 묶음: 껍데기 + [changes] 의 지운 노드 + 바뀐 노드 (지금 망에 없으면 지움). 껍데기는 늘 통째로 쓴다 — 키·그룹·프로비저너
     * 변경을 따로 추적하지 않아도 된다 (노드 없는 껍데기는 작다).
     */
    suspend fun encodeDelta(network: MeshNetwork, changes: NetworkChangeLog.Changes): ByteArray {
        val shell = network.cdbMutex.withLock {
            network.withNodesLock { MeshNetworkSerializer.captureShellForJournal(network).second }
        }
        val buffer = ByteArrayOutputStream(16 * 1024)
        val writer = BufferedWriter(OutputStreamWriter(buffer, Charsets.UTF_8), 16 * 1024)
        writeShell(writer, shell)
        for (uuid in changes.removed) {
            writeDelete(writer, uuid.toString())
        }
        for (uuid in changes.nodes) {
            val json = network.cdbMutex.withLock { network.node(uuid = uuid)?.let { MeshNetworkSerializer.encodeNodeToString(it) } }
            if (json == null) {
                // 적힌 뒤 지워졌다 (지움 기록이 같은 묶음에 이미 있을 수 있다 — 두 번 지워도 같다).
                writeDelete(writer, uuid.toString())
            } else {
                writer.write(NODE.code)
                writer.write(TAB.code)
                writer.write(uuid.toString())
                writer.write(TAB.code)
                writer.write(json)
                writer.write(NL.code)
            }
        }
        writer.write(COMMIT.code)
        writer.write(NL.code)
        writer.flush()
        return buffer.toByteArray()
    }

    /**
     * 저널을 접어 전체 CDB JSON 바이트로. 끝난 묶음이 하나도 없거나 껍데기가 없으면 null (빈 저널 = 망 없음).
     * 알 수 없는 기록은 예외 (저널이 깨졌다 — 호출자가 판단).
     */
    fun assemble(input: InputStream): ByteArray? {
        var shell: Pair<String, String>? = null
        val nodes = LinkedHashMap<String, String>()
        // 아직 커밋 안 된 묶음.
        var pendingShell: Pair<String, String>? = null
        val pending = ArrayList<Pair<Char, Pair<String, String?>>>()
        input.bufferedReader(Charsets.UTF_8).useLines { lines ->
            val iterator = lines.iterator()
            while (iterator.hasNext()) {
                val line = iterator.next()
                if (line.isEmpty()) continue
                // 마지막 줄은 덧붙이다 끊겼을 수 있다 — 형식이 깨졌으면 그 묶음째 버린다 (커밋 줄이 없으니 어차피 반영 안 됨).
                if (!iterator.hasNext() && line[0] != COMMIT && !isWellFormed(line)) break
                when (line[0]) {
                    SHELL -> {
                        val second = line.indexOf(TAB, startIndex = 2)
                        require(line.length > 1 && line[1] == TAB && second > 0) { "깨진 껍데기 기록" }
                        pendingShell = line.substring(2, second) to line.substring(second + 1)
                    }

                    NODE -> {
                        val second = line.indexOf(TAB, startIndex = 2)
                        require(line.length > 1 && line[1] == TAB && second > 0) { "깨진 노드 기록" }
                        pending += NODE to (line.substring(2, second) to line.substring(second + 1))
                    }

                    DELETE -> {
                        require(line.length > 2 && line[1] == TAB) { "깨진 지움 기록" }
                        pending += DELETE to (line.substring(2) to null)
                    }

                    COMMIT -> {
                        pendingShell?.let { shell = it }
                        for ((kind, op) in pending) {
                            when (kind) {
                                NODE -> nodes[op.first] = op.second!!
                                DELETE -> nodes.remove(op.first)
                            }
                        }
                        pendingShell = null
                        pending.clear()
                    }

                    else -> throw IllegalStateException("알 수 없는 저널 기록 '${line[0]}'")
                }
            }
        }
        val (prefix, suffix) = shell ?: return null
        val out = ByteArrayOutputStream(prefix.length + suffix.length + nodes.values.sumOf { it.length + 1 })
        val writer = BufferedWriter(OutputStreamWriter(out, Charsets.UTF_8), 32 * 1024)
        writer.write(prefix)
        var first = true
        for (json in nodes.values) {
            if (!first) writer.write(','.code)
            first = false
            writer.write(json)
        }
        writer.write(suffix)
        writer.flush()
        return out.toByteArray()
    }

    private fun isWellFormed(line: String): Boolean = when (line[0]) {
        SHELL, NODE -> line.length > 1 && line[1] == TAB && line.indexOf(TAB, startIndex = 2) > 0
        DELETE -> line.length > 2 && line[1] == TAB
        else -> false
    }

    private fun writeShell(writer: BufferedWriter, shell: kotlinx.serialization.json.JsonObject) {
        val (prefix, suffix) = MeshNetworkSerializer.shellParts(shell)
        writer.write(SHELL.code)
        writer.write(TAB.code)
        writer.write(prefix)
        writer.write(TAB.code)
        writer.write(suffix)
        writer.write(NL.code)
    }

    private fun writeDelete(writer: BufferedWriter, uuid: String) {
        writer.write(DELETE.code)
        writer.write(TAB.code)
        writer.write(uuid)
        writer.write(NL.code)
    }
}
