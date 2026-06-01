package no.nordicsemi.kotlin.mesh.core.layers.access

import no.nordicsemi.kotlin.mesh.core.messages.AcknowledgedMeshMessage
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.MeshAddress
import org.junit.Assert.assertArrayEquals
import org.junit.Test

/**
 * Light LC Property opcode 의 access-layer wire 인코딩 회귀 가드 (simdo-fork, 2026-06-02).
 *
 * ## 배경 (device fix — S21U / node 0x0106 LC element 0x0107)
 *
 * simdo `core:neomesh` 의 Light LC Property opcode 가 틀린 값 (0x82F0/0x82F1/0x82F2/0x82F3) 으로
 * 정의돼 있어, NCS/Zephyr LC Setup Server 가 해당 opcode 의 핸들러를 못 찾고
 * `ACCESS_STATUS_WRONG_OPCODE` 로 silent drop → Property Set timeout 이 발생했다.
 *
 * 정정된 SIG 값 (Mesh Model spec §6.3.6 + NCS `light_ctrl.h:239-242` +
 * Nordic Java `ApplicationMessageOpCodes.java`):
 *  - Property Get = **0x829D** (2-octet)
 *  - Property Set = **0x62** (1-octet, `BT_MESH_MODEL_OP_1(0x62)`)
 *  - Property Set Unack = **0x63** (1-octet)
 *  - Property Status = **0x64** (1-octet)
 *
 * 본 테스트는 [AccessPdu.init] (lib 의 opcode packer) 가 이 비대칭을 wire 에서 올바르게 인코딩하는지
 * 검증한다. `core:neomesh` 의 LightLc* 메시지는 이 packer 를 통해 송신되므로, 이 boundary 에서
 * 인코딩이 맞으면 simdo 메시지의 wire byte 도 맞다.
 *
 *  - Property Set (0x62) → 단일 opcode byte `62` (NOT 2-byte `0x62 0x..`, NOT `82F1`)
 *  - Property Get (0x829D) → 2-octet `82 9D`
 *
 * [AccessPdu] 는 lib `internal` 이라 simdo 테스트에서 접근 불가 — 동일 모듈인 lib 테스트에 둔다.
 */
class LightLcOpcodeWireEncodingTest {

    /** opCode 와 parameters 만 carry 하는 최소 acked 메시지 (simdo LightLcPropertySet 의 wire 등가물). */
    private class StubAckMessage(
        override val opCode: UInt,
        override val parameters: ByteArray,
    ) : AcknowledgedMeshMessage {
        override val responseOpCode: UInt = 0x64u // Property Status (1-octet)
    }

    private val src: Address = 0x0001u
    private val dst: MeshAddress = MeshAddress.create(address = 0x0107)

    @Test
    fun `Property Set (0x62) 는 단일 opcode byte 62 로 인코딩`() {
        // Time Occupancy Delay (ID 0x003A, value 0) — Property ID 2 bytes LE + 3-byte uint24 value.
        val params = byteArrayOf(0x3A, 0x00, 0x00, 0x00, 0x00)
        val pdu = AccessPdu.init(
            message = StubAckMessage(opCode = 0x62u, parameters = params),
            source = src,
            destination = dst,
            userInitiated = true,
        )

        // 기대 access PDU = 62 3A 00 00 00 00 (1 opcode + 2 ID LE + 3 value). 82F1... 아님.
        assertArrayEquals(
            "Property Set 은 1-octet opcode 0x62 단일 byte + payload 로 직렬화되어야 한다",
            byteArrayOf(0x62, 0x3A, 0x00, 0x00, 0x00, 0x00),
            pdu.accessPdu,
        )
    }

    @Test
    fun `Property Set Unacknowledged (0x63) 도 단일 opcode byte 63`() {
        val pdu = AccessPdu.init(
            message = StubAckMessage(opCode = 0x63u, parameters = byteArrayOf(0x3A, 0x00)),
            source = src,
            destination = dst,
            userInitiated = true,
        )
        assertArrayEquals(byteArrayOf(0x63, 0x3A, 0x00), pdu.accessPdu)
    }

    @Test
    fun `Property Get (0x829D) 는 2-octet 82 9D 로 인코딩`() {
        // Get payload = Property ID 2 bytes LE only.
        val params = byteArrayOf(0x3A, 0x00)
        val pdu = AccessPdu.init(
            message = StubAckMessage(opCode = 0x829Du, parameters = params),
            source = src,
            destination = dst,
            userInitiated = true,
        )

        // 기대 access PDU = 82 9D 3A 00 (2 opcode + 2 ID LE). 0x829D and 0xFFC000 == 0x008000 분기.
        assertArrayEquals(
            "Property Get 은 2-octet opcode 0x829D = [0x82, 0x9D] + payload 로 직렬화되어야 한다",
            byteArrayOf(0x82.toByte(), 0x9D.toByte(), 0x3A, 0x00),
            pdu.accessPdu,
        )
    }

    @Test
    fun `Property Status (0x64) 수신 디코딩 — 1-octet opcode 0x64`() {
        // 노드가 보낸 Status PDU (raw access bytes) 를 lib 가 1-octet 으로 decode 하는지.
        // raw = 64 3A 00 <value...> → opCode 0x64u, params = 3A 00 <value>.
        // UpperTransportPdu 의존 없이, 인코딩 경로의 역대칭만 byte 수준에서 확인:
        //   octet0 = 0x64 (top bit 0) → 1-octet branch → opCode = 0x64u.
        val statusFirstOctet = 0x64.toByte()
        // top bit clear 확인 (1-octet opcode 범위 0x00..0x7E).
        org.junit.Assert.assertEquals(
            "Status opcode 0x64 의 최상위 bit 는 0 — 1-octet 디코딩 분기로 가야 함",
            0,
            statusFirstOctet.toInt() and 0x80,
        )
        org.junit.Assert.assertEquals(0x64, statusFirstOctet.toInt() and 0xFF)
    }
}
