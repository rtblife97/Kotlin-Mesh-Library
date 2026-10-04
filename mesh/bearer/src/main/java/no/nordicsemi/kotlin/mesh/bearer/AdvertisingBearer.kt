package no.nordicsemi.kotlin.mesh.bearer

/**
 * simdo-fork (2026-10-05, 커미셔닝 동글 "얇은 무선" 1단계) — 광고 베어러 표시.
 *
 * 광고 베어러(ADV bearer)는 GATT 프록시와 달리 **필터가 없다**: 듣는 범위의 망 메시지가 목적지와 관계없이 전부 들어온다
 * (다른 노드끼리 주고받는 Status, 다른 폰의 설정 요청과 그 응답, 릴레이 사본). 이 표시가 붙은 베어러로 들어온 Network PDU 는
 * 네트워크 계층이 해독 직후 목적지를 보고 로컬 노드가 받을 것(로컬 엘리먼트 주소 · 로컬 모델이 구독한 그룹 · All Nodes ·
 * 프록시 필터가 통과시켰을 주소)만 하위 전송 계층으로 넘긴다 (`NetworkLayer.handle`).
 *
 * 등록 규칙 (`NetworkManager.registerBearer`):
 *  - 목적지별 등록은 **송신 경로만** 정한다. 광고 베어러를 엘리먼트 주소마다 등록해도 수신 수집기를 따로 띄우지 않는다 —
 *    같은 공중 수신을 엘리먼트 수만큼 처리하지 않게.
 *  - 수신은 `MeshNetworkManager.attachAdvertisingReceiver` 로 **한 번** 붙인다.
 *
 * 프록시 설정 PDU 는 보내지도 받지도 않는다 (프록시가 없다).
 */
interface AdvertisingBearer : MeshBearer
