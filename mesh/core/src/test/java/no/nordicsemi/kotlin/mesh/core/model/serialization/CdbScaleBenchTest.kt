package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.io.File
import java.io.OutputStream
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-fork (2026-10-31) — 폰 망 노드 수별 저장 비용 벤치 (단언 없음, 평소엔 건너뜀).
 *
 * emblaze 조명 모양([EmblazeCdbFixture], 엘리먼트 5·모델 24)으로 600 / 1,400 / 2,200 / 3,000 노드 망을 만들어 잰다:
 *  - 종전 저장 경로 (망 전체 JsonElement 트리 → toString → toByteArray): 단계별 시간·할당, 트리가 한순간 차지하는 힙
 *  - 지금 저장 경로 (`mgr.save()` → Storage.saveStreaming → 임시 파일에 노드마다 바로 쓰기 → 이름 바꿈): 시간·할당
 *  - 망을 올렸을 때 남는 힙, 올리기(import) 시간·할당
 *  - 수신·송신 PDU 마다 도는 주소 → 노드 조회 (목록 끝 노드) 1회 시간·할당
 * 현장 `설정 측정` export_ms 와 맞춰 본다 (2026-10-30 Flip7: 600~799 200 ms, 1,400~1,599 639 ms).
 *
 * `MESH_SCALE_BENCH=1 gradle -p <lib> :mesh:core:test --tests '*CdbScaleBenchTest*' -i | grep BENCH`
 * 노드 수 `MESH_SCALE_BENCH_NODES=600,3000`, 반복 `MESH_SCALE_BENCH_ROUNDS=7`.
 */
@OptIn(ExperimentalUuidApi::class)
class CdbScaleBenchTest {

    private val threadMx = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
    private val memoryMx = ManagementFactory.getMemoryMXBean()

    private fun allocatedBytes(): Long = threadMx.getThreadAllocatedBytes(Thread.currentThread().id)

    private fun usedAfterGc(): Long {
        repeat(5) {
            System.gc()
            Thread.sleep(40)
        }
        return memoryMx.heapMemoryUsage.used
    }

    /** 한 번 잰 값: 시간(ms)·이 스레드 할당(MB). */
    private data class Cost(val ms: Double, val allocMb: Double)

    private inline fun <T> measure(block: () -> T): Pair<T, Cost> {
        val a0 = allocatedBytes()
        val t0 = System.nanoTime()
        val r = block()
        val ms = (System.nanoTime() - t0) / 1e6
        return r to Cost(ms, (allocatedBytes() - a0) / 1e6)
    }

    private fun List<Cost>.median(): Cost = sortedBy { it.ms }[size / 2]

    private fun Cost.label() = "%.1fms/%.1fMB".format(ms, allocMb)

    @Test
    fun `bench 노드 수별 저장 비용`() = runBlocking {
        assumeTrue(System.getenv("MESH_SCALE_BENCH") != null)
        // "노드수" 또는 "노드수x엘리먼트수" (기본 엘리먼트 5). 6550 = 엘리먼트 5 주소 공간 끝, 4095x8 = 엘리먼트 8 주소 공간 끝.
        val sizes = (System.getenv("MESH_SCALE_BENCH_NODES") ?: "600,1400,3000,5000,6550,4095x8")
            .split(',').map { it.trim() }.map { spec ->
                val parts = spec.split('x')
                parts[0].toInt() to (parts.getOrNull(1)?.toInt() ?: 5)
            }
        val rounds = (System.getenv("MESH_SCALE_BENCH_ROUNDS") ?: "7").toInt()
        val dir = File(System.getProperty("java.io.tmpdir"), "cdb-scale-bench-${System.nanoTime()}").apply { mkdirs() }
        println("BENCH maxHeap=${Runtime.getRuntime().maxMemory() / 1_000_000} MB rounds=$rounds")
        for ((n, el) in sizes) {
            var cdb: ByteArray? = EmblazeCdbFixture.network(nodeCount = n, elementsPerNode = el)

            // 올리기 비용 + 남는 힙 (입력 바이트를 놓은 뒤).
            val base = usedAfterGc()
            val storage = FileStreamingStorage(File(dir, "mesh-cdb-$n.json"))
            val (mgr, importCost) = measure { manager(cdb!!, storage) }
            cdb = null
            val retainedMb = (usedAfterGc() - base) / 1e6
            val network = mgr.network!!

            // 종전 경로 단계별.
            repeat(3) { MeshNetworkSerializer.serializeWithCdbLock(network).toString().toByteArray() }
            val tree = ArrayList<Cost>(); val str = ArrayList<Cost>(); val bytes = ArrayList<Cost>(); val legacy = ArrayList<Cost>()
            var lastTree: JsonObject? = null
            var outLen = 0
            repeat(rounds) {
                val a0 = allocatedBytes(); val t0 = System.nanoTime()
                val (t, c1) = measure { MeshNetworkSerializer.serializeWithCdbLock(network) }
                val (s, c2) = measure { t.toString() }
                val (b, c3) = measure { s.toByteArray() }
                legacy += Cost((System.nanoTime() - t0) / 1e6, (allocatedBytes() - a0) / 1e6)
                tree += c1; str += c2; bytes += c3
                lastTree = t; outLen = b.size
            }
            val beforeDrop = usedAfterGc()
            lastTree = null
            val treeLiveMb = (beforeDrop - usedAfterGc()) / 1e6

            // 지금 저장 경로 (파일 스트리밍, 쓰기 포함).
            repeat(3) { mgr.save() }
            val save = (0 until rounds).map { measure { mgr.save() }.second }
            check(storage.file.length().toInt() == outLen) { "파일 크기 ${storage.file.length()} ≠ 종전 출력 $outLen" }

            // 노드 단위 증분 저장 (저널): 스냅숏 1회 뒤, 노드 1대가 바뀐 저장 (조명 1대 설정 중 저장의 모양).
            val journal = FileJournalStorage(File(dir, "mesh-cdb-$n.journal"))
            val jm = manager(storage.file.readBytes(), journal)
            jm.save() // 스냅숏
            val (_, snapshotCost) = measure { jm.network!!.changeLog.requireFull(); jm.save() }
            val jn = jm.network!!.nodes
            repeat(3) { jn[it].name = "예열 $it"; jm.save() }
            val delta = (0 until rounds).map { r ->
                jn[(r * 37) % jn.size].name = "증분 $r"
                measure { jm.save() }.second
            }
            jm.clear()

            // 올리기: 종전(트리 경유) 대 지금(트리 없음). 같은 바이트.
            val bytesForLoad = storage.file.readBytes()
            repeat(2) { MeshNetworkSerializer.deserialize(bytesForLoad); MeshNetworkSerializer.deserializeViaTree(bytesForLoad) }
            val (_, loadLegacy) = measure { MeshNetworkSerializer.deserializeViaTree(bytesForLoad) }
            val (_, loadNew) = measure { MeshNetworkSerializer.deserialize(bytesForLoad) }

            // 주소 → 노드 조회 (목록 끝 노드 = 최악, 새 노드는 끝에 붙는다). 메시지마다 약 4회.
            val lastNode = network.nodes.last()
            val lastAddress = UnicastAddress(lastNode.primaryUnicastAddress.address.toInt() + lastNode.elementsCount - 1)
            repeat(2_000) { network.node(lastAddress) }
            val lookupIters = 200
            val (_, lookup) = measure { repeat(lookupIters) { network.node(lastAddress) } }

            println(
                "BENCH nodes=$n el=$el elements=${n * el} json=${"%.2f".format(outLen / 1e6)}MB (${outLen / n} B/node) " +
                    "retained=${"%.1f".format(retainedMb)}MB (${"%.1f".format(retainedMb * 1e3 / n)} KB/node) " +
                    "import=${importCost.label()} | " +
                    "legacy=${legacy.median().label()} [tree ${tree.median().label()} toString ${str.median().label()} " +
                    "bytes ${bytes.median().label()}] tree_live=${"%.1f".format(treeLiveMb)}MB | " +
                    "stream_save=${save.median().label()} journal_snapshot=${snapshotCost.label()} delta_save=${delta.median().label()} | " +
                    "load_legacy=${loadLegacy.label()} load_new=${loadNew.label()} | " +
                    "lookup=${"%.1f".format(lookup.ms * 1e3 / lookupIters)}us/${"%.1f".format(lookup.allocMb * 1e6 / lookupIters / 1e3)}KB"
            )
            mgr.clear()
        }
        dir.deleteRecursively()
        Unit
    }

    /**
     * 커미셔닝 앱 층 세션이 들고 있는 1004 응답 CDB (벤더 서버의 건물 전체 망 — 폰 CDB 가 아니다) 의 보유 힙. 앱은 이것을 JsonObject
     * 트리(`MeshNetworkResponse.cdb`, 할당기 람다의 CdbView 도 같은 트리) 와 원문 문자열(`FloorLoopInputs.cdbJson`) 로 둘 다 든다.
     * 형식이 같은 CDB 를 텍스트에서 파싱해 잰다. 한글 이름이 하나라도 있으면 JVM·ART 문자열은 UTF-16 (글자당 2바이트).
     */
    @Test
    fun `bench 1004 건물 CDB 보유 힙`() = runBlocking {
        assumeTrue(System.getenv("MESH_SCALE_BENCH") != null)
        for (n in listOf(2017, 3000, 6550)) {
            val text = String(EmblazeCdbFixture.network(nodeCount = n))
            repeat(2) { kotlinx.serialization.json.Json.parseToJsonElement(text) } // 예열
            val base = usedAfterGc()
            val measured = measure { kotlinx.serialization.json.Json.parseToJsonElement(text) }
            var tree: kotlinx.serialization.json.JsonElement? = measured.first
            val parse = measured.second
            val withTree = usedAfterGc()
            val treeMb = (withTree - base) / 1e6
            var korean: String? = text.replaceFirst("\"meshName\":\"", "\"meshName\":\"신동 ")
            val koreanMb = (usedAfterGc() - withTree) / 1e6
            println(
                "BENCH 1004 nodes=$n json=${"%.2f".format(text.length / 1e6)}MB tree_retained=${"%.1f".format(treeMb)}MB " +
                    "(${"%.1f".format(treeMb / (text.length / 1e6))}x json) parse=${parse.label()} " +
                    "string_latin1=${"%.1f".format(text.length / 1e6)}MB string_utf16(한글 포함)=${"%.1f".format(koreanMb)}MB"
            )
            tree = null; korean = null
        }
    }

    /** 앱 MeshNetworkStorage 의 저널과 같은 방식: 스냅숏 = 임시 파일 → 이름 바꿈, 증분 = 파일 끝에 덧붙임. */
    private class FileJournalStorage(val file: File) : Storage, no.nordicsemi.kotlin.mesh.core.JournalStorage {
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) = Unit
        override suspend fun openJournal() = file.takeIf { it.exists() }?.inputStream()
        override suspend fun replaceJournal(writer: suspend (OutputStream) -> Unit) {
            val tmp = File(file.parentFile, file.name + ".tmp")
            tmp.outputStream().buffered(64 * 1024).use { writer(it) }
            tmp.renameTo(file)
        }
        override suspend fun appendJournal(batch: ByteArray) {
            java.io.FileOutputStream(file, true).use { it.write(batch) }
        }
        override suspend fun journalSize(): Long = if (file.exists()) file.length() else 0L
    }

    /** 앱 MeshNetworkStorage 와 같은 방식: 임시 파일에 버퍼로 쓰고 이름 바꾸기. */
    private class FileStreamingStorage(val file: File) : Storage {
        override suspend fun load(): ByteArray = ByteArray(0)
        override suspend fun save(network: ByteArray) {
            val tmp = File(file.parentFile, file.name + ".tmp")
            tmp.writeBytes(network)
            tmp.renameTo(file)
        }

        override suspend fun saveStreaming(writer: suspend (OutputStream) -> Unit) {
            val tmp = File(file.parentFile, file.name + ".tmp")
            tmp.outputStream().buffered(64 * 1024).use { writer(it) }
            tmp.renameTo(file)
        }
    }

    private fun manager(cdb: ByteArray, storage: Storage): MeshNetworkManager =
        MeshNetworkManager(
            storage = storage,
            secureProperties = StubSecureProperties(),
            ioDispatcher = Dispatchers.Default,
        ).also { runBlocking { it.import(cdb) } }

    private class StubSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid): IvIndex = IvIndex()
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) = Unit
        private val seq = AtomicInteger(0)
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = seq.getAndIncrement().toUInt()
        override suspend fun storeNextSequenceNumber(uuid: Uuid, address: UnicastAddress, sequenceNumber: UInt) = Unit
        override suspend fun resetSequenceNumber(uuid: Uuid, address: UnicastAddress) = Unit
        override suspend fun lastSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(uuid: Uuid, source: UnicastAddress, lastSeqAuth: ULong) = Unit
        override suspend fun previousSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(uuid: Uuid, source: UnicastAddress, seqAuth: ULong) = Unit
        override suspend fun storeLocalProvisioner(uuid: Uuid, localProvisionerUuid: Uuid) = Unit
        override suspend fun localProvisioner(uuid: Uuid): String? = null
    }
}
