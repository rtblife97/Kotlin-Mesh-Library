package no.nordicsemi.kotlin.mesh.core

import java.io.InputStream
import java.io.OutputStream

/**
 * simdo-fork (2026-10-31) — 노드 단위 증분 저장을 받을 수 있는 저장소. [Storage] 와 함께 구현한다.
 *
 * 저장 1회가 망 전체가 아니라 "껍데기(키·프로비저너·그룹·장면·제외 목록) + 바뀐 노드" 만 쓰도록 하려는 것이다. 종전 저장(망 전체
 * 직렬화)은 노드 수에 비례해 조명 1대 설정마다 2~3회 돌았고, 주소 공간 끝(엘리먼트 5 → 6,550대)에서는 폰에서 1회 수 초였다.
 *
 * 저널은 라이브러리가 정한 줄 형식의 텍스트다 (`CdbJournal`): 스냅숏 묶음 하나 + 그 뒤에 덧붙인 증분 묶음들. 묶음은 커밋 줄로 끝나고,
 * 올릴 때는 끝난 묶음만 읽는다 — 덧붙이는 도중 프로세스가 죽어도 앞 묶음까지는 그대로 올라온다. 저장소는 바이트를 해석하지 않는다.
 *
 * 저널이 커지면(스냅숏보다 커지거나, 마지막 전체 저장 뒤 [MeshNetworkManager.fullSaveMaxInterval] 이 지나면) 라이브러리가 전체
 * 스냅숏으로 [replaceJournal] 한다.
 */
interface JournalStorage {

    /**
     * 지금 저널을 쓰는가. false 면 라이브러리는 이 저장소를 저널 없는 [Storage] 로 다룬다 (전체 파일 저장·올리기, 종전과 같음).
     * 같은 저장소 구현을 여러 앱이 쓸 때 앱마다 고르게 하려는 것이다 (simdo: 커미셔닝 앱만 켠다).
     */
    val journalEnabled: Boolean get() = true

    /** 저널을 읽을 스트림. 저널이 없으면 null. 호출자가 닫는다. */
    suspend fun openJournal(): InputStream?

    /** 저널 전체를 [writer] 의 출력으로 바꾼다 (원자적으로 — 끝까지 쓴 뒤에만 바꿔치기, 실패하면 기존 저널 유지). */
    suspend fun replaceJournal(writer: suspend (OutputStream) -> Unit)

    /** 저널 끝에 [batch] 를 덧붙인다. */
    suspend fun appendJournal(batch: ByteArray)

    /** 지금 저널 크기 (바이트). 없으면 0. */
    suspend fun journalSize(): Long
}
