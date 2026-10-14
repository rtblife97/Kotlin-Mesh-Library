package no.nordicsemi.kotlin.mesh.core.layers.network

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * simdo 2026-10-14 — 기본 베어러(메인 프록시 링크) 로 받은 Network PDU 진단 카운터 (프로세스 전역). 동작에는 영향이 없다.
 *
 * 앱의 "공중 부하 탐침" 이 쓴다: 카운터를 [reset] → 프록시 필터를 빈 거부 목록(전체 수신)으로 연다 → 잠시 기다린다 → [snapshot].
 * 노드가 GATT 로 넘겨 준 망의 고유 메시지 수 (Network Message Cache 중복 제거 뒤) 이지 공중 송신 횟수가 아니다.
 *
 * 세는 것: 중복 제거를 통과한 Network PDU 하나마다 발신 주소, 네트워크 키로 풀었는지, 상위 계층(access) 이 풀렸으면 그 opcode.
 * 분할 메시지는 분할마다 PDU 1 개로 세고 opcode 는 재조립이 끝난 분할에만 붙는다. 목적지별 등록 링크·광고 베어러로 온 PDU 와 은퇴 중인 옛
 * 스택이 받은 PDU 는 세지 않는다 (같은 PDU 를 두 번 세지 않게).
 */
object NetworkRxDiagnostics {
    private val pdus = AtomicInteger(0)
    private val undecryptable = AtomicInteger(0)
    private val bySource = ConcurrentHashMap<Int, AtomicInteger>()
    private val byOpCode = ConcurrentHashMap<Int, AtomicInteger>()

    /** 한 번 읽은 값. [opCodes] 는 opcode → 수. */
    data class Snapshot(
        val pdus: Int,
        val undecryptable: Int,
        val sources: Int,
        val opCodes: Map<Int, Int>,
    )

    /** 모든 값을 0 으로. */
    fun reset() {
        pdus.set(0)
        undecryptable.set(0)
        bySource.clear()
        byOpCode.clear()
    }

    fun snapshot(): Snapshot = Snapshot(
        pdus = pdus.get(),
        undecryptable = undecryptable.get(),
        sources = bySource.size,
        opCodes = byOpCode.mapValues { it.value.get() },
    )

    /** 네트워크 키로 풀지 못한 PDU 하나. */
    internal fun recordUndecryptable() {
        pdus.incrementAndGet()
        undecryptable.incrementAndGet()
    }

    /** 네트워크 키로 풀린 PDU 하나 (발신 [source]). */
    internal fun recordDecoded(source: Int) {
        pdus.incrementAndGet()
        bySource.getOrPut(source) { AtomicInteger(0) }.incrementAndGet()
    }

    /** 상위 계층까지 풀린 메시지 하나의 [opCode] (분할이면 재조립이 끝났을 때 한 번). */
    internal fun recordOpCode(opCode: Int) {
        byOpCode.getOrPut(opCode) { AtomicInteger(0) }.incrementAndGet()
    }
}
