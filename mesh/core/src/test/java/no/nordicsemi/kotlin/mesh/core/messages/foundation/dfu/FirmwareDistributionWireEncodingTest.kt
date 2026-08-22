package no.nordicsemi.kotlin.mesh.core.messages.foundation.dfu

import no.nordicsemi.kotlin.mesh.core.layers.foundation.FirmwareDistributionClientHandler
import no.nordicsemi.kotlin.mesh.core.layers.foundation.FirmwareUpdateClientHandler
import no.nordicsemi.kotlin.mesh.core.messages.BLOBTransferMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareDistributionMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareId
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareUpdateMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.RetrievedUpdatePhase
import no.nordicsemi.kotlin.mesh.core.messages.FirmwareUpdatePolicy
import no.nordicsemi.kotlin.mesh.core.messages.TransferMode
import no.nordicsemi.kotlin.mesh.core.model.GroupAddress
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Mesh DFU (Device Firmware Update) 메시지의 wire 인코딩 회귀 가드 (simdo-fork, 2026-08-12).
 *
 * ## 배경
 *
 * 이 테스트가 검증하는 32종 DFU 메시지는 upstream `feature/mesh-dfu` (미머지 WIP 브랜치, 커밋
 * `65e4be021`) 에서 백포트한 것이다. **upstream 은 이 3,645 줄에 대해 테스트를 하나도 갖고 있지
 * 않다** — 실제로 백포트 직후 감사에서 복붙 기인 wire 버그가 여러 건 발견됐다.
 *
 * ## 기준 (authoritative reference)
 *
 * 기대값은 스펙 기억이 아니라 **타깃 펌웨어 소스**에서 직접 뽑았다. 우리 동글
 * (`neo_mesh_commissioner`) 은 NCS 기반이고, DFD/DFU Server 구현은 아래가 정답지다:
 *  - `zephyr/subsys/bluetooth/mesh/dfd.h`      — DFD opcode 0x8311..0x8327
 *  - `zephyr/subsys/bluetooth/mesh/dfu.h`      — DFU opcode 0x8308..0x8310
 *  - `zephyr/subsys/bluetooth/mesh/dfd_srv.c`  — `_bt_mesh_dfd_srv_op[]` 길이표 + 각 handler 파서
 *  - `zephyr/subsys/bluetooth/mesh/dfu_srv.c`  — `_bt_mesh_dfu_srv_op[]` 길이표 + 각 handler 파서
 *
 * NCS **v3.2.1 과 v3.4.0 에서 opcode·길이표가 동일**함을 확인했다 (타깃 emblaze = v2.6.0 계열이나
 * 이 표는 SIG 스펙 고정값이라 버전 간 불변).
 *
 * ## 이 테스트가 잡는 것
 *
 * 펌웨어의 opcode 길이표(`BT_MESH_LEN_EXACT(n)` / `BT_MESH_LEN_MIN(n)`)는 **수신측 하드 게이트**다.
 * 길이가 안 맞으면 handler 진입 전에 드롭되고 **응답이 아예 없다** → 앱은 원인 불명 타임아웃만 본다.
 * 따라서 "보내는 바이트 길이/레이아웃" 회귀는 필드에서 가장 진단하기 어려운 부류다.
 */
class FirmwareDistributionWireEncodingTest {

    // region opcode 대조 — NCS dfu.h / dfd.h 원문 값

    @Test
    fun `Firmware Update opcode 9종이 NCS dfu_h 와 일치`() {
        assertEquals("UPDATE_INFO_GET", 0x8308u, FirmwareUpdateInformationGet.opCode)
        assertEquals("UPDATE_INFO_STATUS", 0x8309u, FirmwareUpdateInformationStatus.opCode)
        assertEquals("UPDATE_METADATA_CHECK", 0x830Au, FirmwareUpdateFirmwareMetadataCheck.opCode)
        assertEquals("UPDATE_METADATA_STATUS", 0x830Bu, FirmwareUpdateFirmwareMetadataStatus.opCode)
        assertEquals("UPDATE_GET", 0x830Cu, FirmwareUpdateGet.opCode)
        assertEquals("UPDATE_START", 0x830Du, FirmwareUpdateStart.opCode)
        assertEquals("UPDATE_CANCEL", 0x830Eu, FirmwareUpdateCancel.opCode)
        assertEquals("UPDATE_APPLY", 0x830Fu, FirmwareUpdateApply.opCode)
        assertEquals("UPDATE_STATUS", 0x8310u, FirmwareUpdateStatus.opCode)
    }

    @Test
    fun `Firmware Distribution opcode 23종이 NCS dfd_h 와 일치`() {
        assertEquals("RECEIVERS_ADD", 0x8311u, FirmwareDistributionReceiversAdd.opCode)
        assertEquals("RECEIVERS_DELETE_ALL", 0x8312u, FirmwareDistributionReceiversDeleteAll.opCode)
        assertEquals("RECEIVERS_STATUS", 0x8313u, FirmwareDistributionReceiversStatus.opCode)
        assertEquals("RECEIVERS_GET", 0x8314u, FirmwareDistributionReceiversGet.opCode)
        assertEquals("RECEIVERS_LIST", 0x8315u, FirmwareDistributionReceiversList.opCode)
        assertEquals("CAPABILITIES_GET", 0x8316u, FirmwareDistributionCapabilitiesGet.opCode)
        assertEquals("CAPABILITIES_STATUS", 0x8317u, FirmwareDistributionCapabilitiesStatus.opCode)
        assertEquals("DFD_GET", 0x8318u, FirmwareDistributionGet.opCode)
        assertEquals("DFD_START", 0x8319u, FirmwareDistributionStart.opCode)
        assertEquals("SUSPEND", 0x831Au, FirmwareDistributionSuspend.opCode)
        assertEquals("CANCEL", 0x831Bu, FirmwareDistributionCancel.opCode)
        assertEquals("APPLY", 0x831Cu, FirmwareDistributionApply.opCode)
        assertEquals("DFD_STATUS", 0x831Du, FirmwareDistributionStatus.opCode)
        assertEquals("UPLOAD_GET", 0x831Eu, FirmwareDistributionUploadGet.opCode)
        assertEquals("UPLOAD_START", 0x831Fu, FirmwareDistributionUploadStart.opCode)
        assertEquals("UPLOAD_START_OOB", 0x8320u, FirmwareDistributionUploadOOBStart.opCode)
        assertEquals("UPLOAD_CANCEL", 0x8321u, FirmwareDistributionUploadCancel.opCode)
        assertEquals("UPLOAD_STATUS", 0x8322u, FirmwareDistributionUploadStatus.opCode)
        assertEquals("FW_GET", 0x8323u, FirmwareDistributionFirmwareGet.opCode)
        assertEquals("FW_GET_BY_INDEX", 0x8324u, FirmwareDistributionFirmwareGetByIndex.opCode)
        assertEquals("FW_DELETE", 0x8325u, FirmwareDistributionFirmwareDelete.opCode)
        assertEquals("FW_DELETE_ALL", 0x8326u, FirmwareDistributionFirmwareDeleteAll.opCode)
        assertEquals("FW_STATUS", 0x8327u, FirmwareDistributionFirmwareStatus.opCode)
    }

    @Test
    fun `DFU opcode 32종은 0x8308부터 0x8327까지 빈틈없이 유일`() {
        val opCodes = setOf(
            FirmwareUpdateInformationGet.opCode,
            FirmwareUpdateInformationStatus.opCode,
            FirmwareUpdateFirmwareMetadataCheck.opCode,
            FirmwareUpdateFirmwareMetadataStatus.opCode,
            FirmwareUpdateGet.opCode,
            FirmwareUpdateStart.opCode,
            FirmwareUpdateCancel.opCode,
            FirmwareUpdateApply.opCode,
            FirmwareUpdateStatus.opCode,
            FirmwareDistributionReceiversAdd.opCode,
            FirmwareDistributionReceiversDeleteAll.opCode,
            FirmwareDistributionReceiversStatus.opCode,
            FirmwareDistributionReceiversGet.opCode,
            FirmwareDistributionReceiversList.opCode,
            FirmwareDistributionCapabilitiesGet.opCode,
            FirmwareDistributionCapabilitiesStatus.opCode,
            FirmwareDistributionGet.opCode,
            FirmwareDistributionStart.opCode,
            FirmwareDistributionSuspend.opCode,
            FirmwareDistributionCancel.opCode,
            FirmwareDistributionApply.opCode,
            FirmwareDistributionStatus.opCode,
            FirmwareDistributionUploadGet.opCode,
            FirmwareDistributionUploadStart.opCode,
            FirmwareDistributionUploadOOBStart.opCode,
            FirmwareDistributionUploadCancel.opCode,
            FirmwareDistributionUploadStatus.opCode,
            FirmwareDistributionFirmwareGet.opCode,
            FirmwareDistributionFirmwareGetByIndex.opCode,
            FirmwareDistributionFirmwareDelete.opCode,
            FirmwareDistributionFirmwareDeleteAll.opCode,
            FirmwareDistributionFirmwareStatus.opCode,
        )
        assertEquals("32종 전부 서로 다른 opcode 여야 한다", 32, opCodes.size)
        assertEquals((0x8308u..0x8327u).toSet(), opCodes)
    }

    // endregion

    // region 무파라미터 메시지 — 펌웨어 BT_MESH_LEN_EXACT(0)

    /**
     * NCS `_bt_mesh_dfd_srv_op[]` / `_bt_mesh_dfu_srv_op[]` 에서 `BT_MESH_LEN_EXACT(0)` 인 opcode 는
     * 파라미터가 **1 바이트라도 붙으면 드롭**된다. `parameters` 는 반드시 null(또는 빈 배열)이어야 한다.
     */
    @Test
    fun `LEN_EXACT_0 메시지는 파라미터를 싣지 않는다`() {
        assertNull("CAPABILITIES_GET", FirmwareDistributionCapabilitiesGet().parameters)
        assertNull("DFD_GET", FirmwareDistributionGet().parameters)
        assertNull("SUSPEND", FirmwareDistributionSuspend().parameters)
        assertNull("CANCEL", FirmwareDistributionCancel().parameters)
        assertNull("APPLY", FirmwareDistributionApply().parameters)
        assertNull("UPLOAD_GET", FirmwareDistributionUploadGet().parameters)
        assertNull("UPLOAD_CANCEL", FirmwareDistributionUploadCancel().parameters)
        assertNull("FW_DELETE_ALL", FirmwareDistributionFirmwareDeleteAll().parameters)
        assertNull("UPDATE_GET", FirmwareUpdateGet().parameters)
        assertNull("UPDATE_CANCEL", FirmwareUpdateCancel().parameters)
        assertNull("UPDATE_APPLY", FirmwareUpdateApply().parameters)

        // ★ 회귀 가드: upstream 은 여기에 firstIndex+entriesLimit 4 바이트를 실어 보냈다
        //   (ReceiversGet 복붙). 펌웨어 dfd_srv.c 는 LEN_EXACT(0) 이라 통째로 드롭한다.
        assertNull(
            "RECEIVERS_DELETE_ALL 은 파라미터가 없다 (NCS: BT_MESH_LEN_EXACT(0))",
            FirmwareDistributionReceiversDeleteAll().parameters
        )
    }

    // endregion

    // region Receivers Add — 백포트 최대 결함 지점

    /**
     * NCS `handle_receivers_add()`:
     * ```c
     * if (buf->len % 3) { return -EINVAL; }
     * while (buf->len >= 3 && ...) {
     *     addr    = net_buf_simple_pull_le16(buf);
     *     img_idx = net_buf_simple_pull_u8(buf);
     * }
     * ```
     * 즉 엔트리당 3옥텟 = 주소(LE16) + 이미지 인덱스(U8) 의 가변 리스트다.
     *
     * upstream 은 이 클래스를 ReceiversGet 의 복붙(firstIndex+entriesLimit = 4바이트)으로 두었다.
     * 4 % 3 != 0 → 펌웨어가 -EINVAL 로 드롭 → **무응답 타임아웃** → Distribution 자체가 불가능.
     */
    @Test
    fun `ReceiversAdd 는 엔트리당 3옥텟 - 주소LE16 + 이미지인덱스U8`() {
        val message = FirmwareDistributionReceiversAdd(
            receivers = listOf(
                FirmwareDistributionReceiversAdd.Receiver(
                    address = UnicastAddress(address = 0x0007),
                    imageIndex = 0x00u
                ),
                FirmwareDistributionReceiversAdd.Receiver(
                    address = UnicastAddress(address = 0x0102),
                    imageIndex = 0x01u
                ),
            )
        )

        assertArrayEquals(
            byteArrayOf(
                0x07, 0x00, 0x00, // 0x0007 LE + image index 0
                0x02, 0x01, 0x01, // 0x0102 LE + image index 1
            ),
            message.parameters
        )
        assertEquals(
            "펌웨어의 `buf->len % 3` 검사를 통과해야 한다",
            0,
            message.parameters.size % 3
        )
    }

    @Test
    fun `ReceiversAdd 는 wire 왕복이 보존된다`() {
        val original = FirmwareDistributionReceiversAdd(
            receivers = listOf(
                FirmwareDistributionReceiversAdd.Receiver(
                    address = UnicastAddress(address = 0x1234),
                    imageIndex = 0x02u
                ),
            )
        )
        val decoded = FirmwareDistributionReceiversAdd.init(parameters = original.parameters)

        assertNotNull(decoded)
        assertEquals(1, decoded!!.receivers.size)
        assertEquals(0x1234.toUShort(), decoded.receivers[0].address.address)
        assertEquals(0x02.toUByte(), decoded.receivers[0].imageIndex)
    }

    @Test
    fun `ReceiversAdd 디코더는 3의 배수가 아닌 길이를 거부`() {
        // upstream 이 보내던 4바이트 페이로드가 바로 이 케이스다.
        assertNull(FirmwareDistributionReceiversAdd.init(parameters = ByteArray(size = 4)))
        assertNull(FirmwareDistributionReceiversAdd.init(parameters = ByteArray(size = 0)))
        assertNull(FirmwareDistributionReceiversAdd.init(parameters = null))
        assertNotNull(FirmwareDistributionReceiversAdd.init(parameters = byteArrayOf(0x01, 0x00, 0x00)))
    }

    // endregion

    // region 응답 opcode 정합 — 잘못되면 응답을 영영 못 맞춰 타임아웃

    /**
     * NCS `handle_receivers_delete_all()` 은 `receivers_status_rsp()` 를 부르고, 이는
     * `BT_MESH_DFD_OP_RECEIVERS_STATUS`(0x8313) 를 보낸다. ReceiversList(0x8315) 가 아니다.
     * upstream 은 ReceiversList 를 기다리도록 해 두어 응답 매칭이 영구 실패했다.
     */
    @Test
    fun `ReceiversDeleteAll 의 응답은 ReceiversStatus 다`() {
        assertEquals(
            FirmwareDistributionReceiversStatus.opCode,
            FirmwareDistributionReceiversDeleteAll().responseOpCode
        )
    }

    @Test
    fun `ReceiversGet 의 응답은 ReceiversList 다`() {
        assertEquals(
            FirmwareDistributionReceiversList.opCode,
            FirmwareDistributionReceiversGet(firstIndex = 0u, entriesLimit = 1u).responseOpCode
        )
    }

    @Test
    fun `ReceiversAdd 의 응답은 ReceiversStatus 다`() {
        val message = FirmwareDistributionReceiversAdd(
            receivers = listOf(
                FirmwareDistributionReceiversAdd.Receiver(
                    address = UnicastAddress(address = 0x0007),
                    imageIndex = 0u
                )
            )
        )
        assertEquals(FirmwareDistributionReceiversStatus.opCode, message.responseOpCode)
    }

    /**
     * 클라이언트 핸들러에 등록된 수신 타입 집합 = 우리가 **파싱할 수 있는** 상태 메시지 전부.
     * 어떤 요청의 `responseOpCode` 가 이 집합 밖이면, 응답이 도착해도 파싱되지 않아 유실된다.
     */
    @Test
    fun `클라이언트 핸들러가 상태 메시지 9종을 모두 등록한다`() {
        val registered = FirmwareDistributionClientHandler().messageTypes.keys +
                FirmwareUpdateClientHandler().messageTypes.keys

        assertEquals(
            setOf(
                FirmwareUpdateInformationStatus.opCode,
                FirmwareUpdateFirmwareMetadataStatus.opCode,
                FirmwareUpdateStatus.opCode,
                FirmwareDistributionReceiversStatus.opCode,
                FirmwareDistributionReceiversList.opCode,
                FirmwareDistributionCapabilitiesStatus.opCode,
                FirmwareDistributionStatus.opCode,
                FirmwareDistributionUploadStatus.opCode,
                FirmwareDistributionFirmwareStatus.opCode,
            ),
            registered
        )
    }

    @Test
    fun `운영 subset 요청의 응답 opcode 는 모두 핸들러에 등록돼 있다`() {
        val registered = FirmwareDistributionClientHandler().messageTypes.keys +
                FirmwareUpdateClientHandler().messageTypes.keys

        val operationalSubset = listOf(
            FirmwareDistributionCapabilitiesGet(),
            FirmwareDistributionGet(),
            FirmwareDistributionSuspend(),
            FirmwareDistributionCancel(),
            FirmwareDistributionApply(),
            FirmwareDistributionReceiversDeleteAll(),
            FirmwareDistributionReceiversGet(firstIndex = 0u, entriesLimit = 0xFFFFu),
            FirmwareDistributionFirmwareGetByIndex(imageIndex = 0u),
            FirmwareUpdateInformationGet(firstIndex = 0u, entriesLimit = 0xFFu),
            FirmwareUpdateFirmwareMetadataCheck(imageIndex = 0u, metaData = null),
        )

        operationalSubset.forEach { message ->
            assertTrue(
                "${message::class.simpleName} 의 responseOpCode 가 핸들러 미등록",
                message.responseOpCode in registered
            )
        }
    }

    // endregion

    // region 나머지 운영 subset 의 바이트 레이아웃

    /** NCS: `{ BT_MESH_DFD_OP_RECEIVERS_GET, BT_MESH_LEN_EXACT(4), ... }` */
    @Test
    fun `ReceiversGet 은 firstIndex + entriesLimit 각 LE16 = 4바이트`() {
        val message = FirmwareDistributionReceiversGet(firstIndex = 0x0001u, entriesLimit = 0x0010u)
        assertArrayEquals(byteArrayOf(0x01, 0x00, 0x10, 0x00), message.parameters)
    }

    /**
     * simdo-fork (2026-08-12, 감사 P1-3) 회귀 가드.
     *
     * NCS `dfd_srv.c handle_receivers_get()` 의 첫 검사가
     * ```c
     * cnt = net_buf_simple_pull_le16(buf);
     * if (cnt == 0) { return -EINVAL; }
     * ```
     * 이라 Entries Limit 0 은 **응답이 아예 오지 않는다**. 배포 진행률 폴링 루프가 원인 불명
     * 타임아웃으로 굳는 자리라 생성 시점에 막는다.
     */
    @Test
    fun `ReceiversGet 은 entriesLimit 0 을 거부한다 - 서버가 무응답 드롭`() {
        assertThrows(IllegalArgumentException::class.java) {
            FirmwareDistributionReceiversGet(firstIndex = 0u, entriesLimit = 0u)
        }
        // 디코더도 같은 PDU 를 받아들이지 않는다.
        assertNull(FirmwareDistributionReceiversGet.init(byteArrayOf(0x00, 0x00, 0x00, 0x00)))
        assertNotNull(FirmwareDistributionReceiversGet.init(byteArrayOf(0x00, 0x00, 0x01, 0x00)))
    }

    /**
     * simdo-fork (2026-08-12, 감사 P1-3 부수): 무인자 `FirmwareUpdateInformationGet()` 는
     * Entries Limit 0 이라 **엔트리가 하나도 없는 응답**을 받는다
     * (`dfu_srv.c handle_info_get()` 의 `limit > 0` 루프 조건). 이름이 의도를 오도해서 제거하고
     * 팩토리 둘로 갈랐다.
     */
    @Test
    fun `FirmwareUpdateInformationGet 은 all 과 count 로 의도를 분리한다`() {
        assertArrayEquals(byteArrayOf(0x00, 0xFF.toByte()), FirmwareUpdateInformationGet.all().parameters)
        assertArrayEquals(byteArrayOf(0x00, 0x00), FirmwareUpdateInformationGet.count().parameters)
    }

    /**
     * NCS `handle_start()`:
     * ```c
     * params.app_idx      = pull_le16();               // offset 0
     * params.ttl          = pull_u8();                 // offset 2
     * params.timeout_base = pull_le16();               // offset 3
     * byte                = pull_u8();                 // offset 5
     * params.xfer_mode    = byte & BIT_MASK(2);        //   bits 0..1
     * params.apply        = (byte >> 2U) & BIT_MASK(1);//   bit 2
     * params.slot_idx     = pull_le16();               // offset 6
     * params.group        = pull_le16();               // offset 8
     * ```
     * → Group 주소 사용 시 정확히 10옥텟 (`BT_MESH_LEN_MIN(10)`).
     */
    @Test
    fun `DistributionStart 는 10옥텟이고 transferMode 는 bit0-1 updatePolicy 는 bit2`() {
        val message = FirmwareDistributionStart(
            firmwareImageIndex = 0x0000u,
            groupAddress = GroupAddress(address = 0xC000),
            applicationKeyIndex = 0x0000u,
            ttl = 0x7Fu,
            distributionTransferMode = TransferMode.PUSH,           // 0x01 → bits 0..1
            updatePolicy = FirmwareUpdatePolicy.VERIFY_AND_APPLY,   // 0x01 → bit 2
            distributionTimeoutBase = 0x0076u,
        )

        assertArrayEquals(
            byteArrayOf(
                0x00, 0x00,             // app key index LE16
                0x7F,                   // ttl
                0x76, 0x00,             // timeout base LE16
                0x05,                   // (PUSH=0b01) | (VERIFY_AND_APPLY=1 shl 2) = 0b0101
                0x00, 0x00,             // firmware image index LE16
                0x00, 0xC0.toByte(),    // multicast (group) address LE16 = 0xC000
            ),
            message.parameters
        )
        assertEquals("펌웨어 BT_MESH_LEN_MIN(10)", 10, message.parameters.size)
    }

    @Test
    fun `DistributionStart 는 wire 왕복이 보존된다`() {
        val original = FirmwareDistributionStart(
            firmwareImageIndex = 0x0003u,
            groupAddress = GroupAddress(address = 0xC123),
            applicationKeyIndex = 0x0002u,
            ttl = 0x05u,
            distributionTransferMode = TransferMode.PULL,        // 0x02 → bits 0..1
            updatePolicy = FirmwareUpdatePolicy.VERIFY_ONLY,     // 0x00 → bit 2
            distributionTimeoutBase = 0x0010u,
        )
        val decoded = FirmwareDistributionStart.init(parameters = original.parameters)

        assertNotNull(decoded)
        assertEquals(0x0002.toUShort(), decoded!!.applicationKeyIndex)
        assertEquals(0x05.toUByte(), decoded.ttl)
        assertEquals(0x0010.toUShort(), decoded.distributionTimeoutBase)
        assertEquals(TransferMode.PULL, decoded.distributionTransferMode)
        assertEquals(FirmwareUpdatePolicy.VERIFY_ONLY, decoded.updatePolicy)
        assertEquals(0x0003.toUShort(), decoded.firmwareImageIndex)
        assertEquals("Multicast Address 는 offset 8 에서 읽어야 한다", 0xC123.toUShort(), decoded.address)
    }

    /** NCS: `{ BT_MESH_DFD_OP_FW_GET_BY_INDEX, BT_MESH_LEN_EXACT(2), ... }` */
    @Test
    fun `FirmwareGetByIndex 는 imageIndex LE16 = 2바이트`() {
        assertArrayEquals(
            byteArrayOf(0x02, 0x00),
            FirmwareDistributionFirmwareGetByIndex(imageIndex = 0x0002u).parameters
        )
    }

    /** NCS: `{ BT_MESH_DFU_OP_UPDATE_INFO_GET, BT_MESH_LEN_EXACT(2), ... }` — 각 필드 U8 */
    @Test
    fun `UpdateInformationGet 은 firstIndex + entriesLimit 각 U8 = 2바이트`() {
        assertArrayEquals(
            byteArrayOf(0x00, 0xFF.toByte()),
            FirmwareUpdateInformationGet(firstIndex = 0x00u, entriesLimit = 0xFFu).parameters
        )
    }

    /**
     * NCS: `{ BT_MESH_DFU_OP_UPDATE_METADATA_CHECK, BT_MESH_LEN_MIN(1), ... }`
     * = 이미지 인덱스(U8) + 가변 메타데이터.
     *
     * 이 메시지가 중요한 이유: 응답인 [FirmwareUpdateFirmwareMetadataStatus] 의
     * Additional Information 필드가 "업데이트 후 노드가 unprovisioned 가 되는가"
     * (BT_MESH_DFU_EFFECT_UNPROV) 를 **배포 전에** 알려주는 유일한 프로토콜 수단이다.
     */
    @Test
    fun `MetadataCheck 는 imageIndex U8 뒤에 가변 메타데이터를 붙인다`() {
        assertArrayEquals(
            byteArrayOf(0x01, 0xDE.toByte(), 0xAD.toByte()),
            FirmwareUpdateFirmwareMetadataCheck(
                imageIndex = 0x01u,
                metaData = byteArrayOf(0xDE.toByte(), 0xAD.toByte())
            ).parameters
        )
        // 메타데이터 없이도 최소 1옥텟은 나가야 한다 (LEN_MIN(1)).
        val noMetadata = FirmwareUpdateFirmwareMetadataCheck(imageIndex = 0x00u, metaData = null)
        assertArrayEquals(byteArrayOf(0x00), noMetadata.parameters)
        assertTrue("펌웨어 BT_MESH_LEN_MIN(1)", noMetadata.parameters.isNotEmpty())
    }

    // endregion

    // region 수신 파싱

    /**
     * NCS `receivers_status_rsp()`:
     * ```c
     * net_buf_simple_add_u8(&buf, status);
     * net_buf_simple_add_le16(&buf, srv->target_cnt);
     * ```
     * = 3옥텟.
     */
    @Test
    fun `ReceiversStatus 는 status U8 + totalCount LE16 = 3바이트를 파싱`() {
        val decoded = FirmwareDistributionReceiversStatus.init(
            parameters = byteArrayOf(0x00, 0x2A, 0x00)
        )

        assertNotNull(decoded)
        assertEquals(FirmwareDistributionMessageStatus.SUCCESS, decoded!!.status)
        assertEquals(42.toUShort(), decoded.totalCount)

        // 길이가 다르면 파싱 거부
        assertNull(FirmwareDistributionReceiversStatus.init(parameters = byteArrayOf(0x00, 0x2A)))
        assertNull(FirmwareDistributionReceiversStatus.init(parameters = null))
    }

    // endregion

    // region ReceiversList — 진행률 모니터링 경로의 부호확장 결함 3건

    /**
     * `Byte.toInt()` 는 부호확장을 한다. Receivers List 엔트리는 5옥텟에 비트필드로 촘촘히
     * 패킹돼 있어서, 특정 필드값이 어떤 옥텟의 bit7 을 세우면 마스크 없는 `ushr` 이 거대한
     * 음수-유래 값을 내고 → enum 매핑 실패 → **메시지 전체 폐기**로 이어졌다.
     *
     * 이 테스트는 인코더(감사에서 correct 확인)를 fixture 생성기로 써서 왕복을 검증한다.
     */
    @Test
    fun `ReceiversList 는 홀수 phase 에서도 파싱된다 - byte1 bit7 부호확장`() {
        // 홀수 phase 는 byte1 의 bit7 을 세운다 → upstream 은 여기서 메시지를 통째로 버렸다.
        val oddPhases = listOf(
            RetrievedUpdatePhase.TRANSFER_ERROR,        // 0x1
            RetrievedUpdatePhase.VERIFYING_UPDATE,      // 0x3
            RetrievedUpdatePhase.VERIFICATION_FAILED,   // 0x5
            RetrievedUpdatePhase.TRANSFER_CANCELED,     // 0x7
            RetrievedUpdatePhase.APPLY_FAILED,          // 0x9
        )

        oddPhases.forEach { phase ->
            val original = FirmwareDistributionReceiversList(
                totalCount = 1u,
                firstIndex = 0u,
                receivers = listOf(
                    FirmwareDistributionReceiversList.ReceiverStatus(
                        address = 0x0007u,
                        phase = phase,
                        updateStatus = FirmwareUpdateMessageStatus.SUCCESS,
                        transferStatus = BLOBTransferMessageStatus.SUCCESS,
                        transferProgress = 0,
                        imageIndex = 0u,
                    )
                )
            )
            val decoded = FirmwareDistributionReceiversList.init(parameters = original.parameters)

            assertNotNull("phase=$phase 에서 메시지가 폐기됐다", decoded)
            assertEquals(phase, decoded!!.receivers.single().phase)
            assertEquals(0x0007.toUShort(), decoded.receivers.single().address)
        }
    }

    @Test
    fun `ReceiversList 는 bit1 이 선 transferStatus 에서도 파싱된다 - byte2 bit7 부호확장`() {
        // Transfer(BLOB) Status 의 bit1 이 선 값들이 byte2 의 bit7 을 세운다.
        // 운영자가 가장 봐야 할 실패 상태들이 바로 이것들이다.
        val highBitStatuses = listOf(
            BLOBTransferMessageStatus.INVALID_BLOCK_SIZE,      // 0x02
            BLOBTransferMessageStatus.INVALID_CHUNK_SIZE,      // 0x03
            BLOBTransferMessageStatus.WRONG_BLOB_ID,           // 0x06
            BLOBTransferMessageStatus.BLOB_TOO_LARGE,          // 0x07
            BLOBTransferMessageStatus.INFORMATION_UNAVAILABLE, // 0x0A
        )

        highBitStatuses.forEach { status ->
            val original = FirmwareDistributionReceiversList(
                totalCount = 1u,
                firstIndex = 0u,
                receivers = listOf(
                    FirmwareDistributionReceiversList.ReceiverStatus(
                        address = 0x0007u,
                        phase = RetrievedUpdatePhase.IDLE,
                        updateStatus = FirmwareUpdateMessageStatus.SUCCESS,
                        transferStatus = status,
                        transferProgress = 0,
                        imageIndex = 0u,
                    )
                )
            )
            val decoded = FirmwareDistributionReceiversList.init(parameters = original.parameters)

            assertNotNull("transferStatus=$status 에서 메시지가 폐기됐다", decoded)
            assertEquals(status, decoded!!.receivers.single().transferStatus)
        }
    }

    @Test
    fun `ReceiversList 진행률은 64퍼센트 이상에서도 정확하다 - byte3 bit7 부호확장`() {
        // 펌웨어는 progress/2 를 6비트에 담으므로 진행률 >= 64% 에서 byte3 이 0x80 을 넘는다.
        // upstream 은 100% 에서 100 대신 2147483620 을 냈다 (드롭이 아닌 조용한 오값).
        listOf(0, 32, 62, 64, 80, 98, 100).forEach { progress ->
            val original = FirmwareDistributionReceiversList(
                totalCount = 1u,
                firstIndex = 0u,
                receivers = listOf(
                    FirmwareDistributionReceiversList.ReceiverStatus(
                        address = 0x0007u,
                        phase = RetrievedUpdatePhase.TRANSFER_ACTIVE,
                        updateStatus = FirmwareUpdateMessageStatus.SUCCESS,
                        transferStatus = BLOBTransferMessageStatus.SUCCESS,
                        transferProgress = progress,
                        imageIndex = 0u,
                    )
                )
            )
            val decoded = FirmwareDistributionReceiversList.init(parameters = original.parameters)

            assertNotNull(decoded)
            assertEquals(
                "진행률 $progress% 가 왜곡됐다",
                progress,
                decoded!!.receivers.single().transferProgress
            )
        }
    }

    @Test
    fun `ReceiversList 는 헤더 4옥텟 + 엔트리 5옥텟 구조를 지킨다`() {
        val message = FirmwareDistributionReceiversList(
            totalCount = 2u,
            firstIndex = 0u,
            receivers = listOf(
                FirmwareDistributionReceiversList.ReceiverStatus(
                    address = 0x0007u,
                    phase = RetrievedUpdatePhase.IDLE,
                    updateStatus = FirmwareUpdateMessageStatus.SUCCESS,
                    transferStatus = BLOBTransferMessageStatus.SUCCESS,
                    transferProgress = 0,
                    imageIndex = 0u,
                ),
                FirmwareDistributionReceiversList.ReceiverStatus(
                    address = 0x0008u,
                    phase = RetrievedUpdatePhase.APPLY_SUCCESS,
                    updateStatus = FirmwareUpdateMessageStatus.SUCCESS,
                    transferStatus = BLOBTransferMessageStatus.SUCCESS,
                    transferProgress = 100,
                    imageIndex = 1u,
                ),
            )
        )
        assertEquals(4 + 2 * 5, message.parameters.size)
        // 헤더: totalCount LE16 + firstIndex LE16
        assertArrayEquals(byteArrayOf(0x02, 0x00, 0x00, 0x00), message.parameters.copyOfRange(0, 4))
    }

    // endregion

    // region CapabilitiesStatus — OOB 플래그 1옥텟

    /**
     * NCS `handle_capabilities_get()` 은 플래그를 `net_buf_simple_add_u8` 로 1 옥텟 쓴다.
     * upstream 은 UInt 리터럴 `0x00u` 를 써서 `UInt.toByteArray()` 기본값(BIG_ENDIAN, 4옥텟)
     * 이 적용돼 메시지가 20옥텟이 됐고, 자기 디코더(offset 16 고정)조차 못 읽었다.
     */
    @Test
    fun `CapabilitiesStatus 는 OOB 미지원 시 정확히 17옥텟`() {
        val message = FirmwareDistributionCapabilitiesStatus(
            maxReceiversCount = 0x000Au,
            maxFirmwareImagesListSize = 0x0002u,
            maxFirmwareImageSize = 0x00010000u,
            maxUploadSpace = 0x00020000u,
            remainingUploadSpace = 0x00020000u,
            supportedUriSchemes = emptyList(),
        )

        assertEquals("2+2+4+4+4+1 = 17 옥텟", 17, message.parameters.size)
        assertEquals("OOB 플래그는 offset 16 의 1옥텟", 0x00.toByte(), message.parameters[16])
    }

    @Test
    fun `CapabilitiesStatus 는 wire 왕복이 보존된다`() {
        val original = FirmwareDistributionCapabilitiesStatus(
            maxReceiversCount = 0x000Au,
            maxFirmwareImagesListSize = 0x0002u,
            maxFirmwareImageSize = 0x00010000u,
            maxUploadSpace = 0x00020000u,
            remainingUploadSpace = 0x0001F000u,
            supportedUriSchemes = emptyList(),
        )
        val decoded = FirmwareDistributionCapabilitiesStatus.init(parameters = original.parameters)

        assertNotNull(decoded)
        assertEquals(0x000A.toUShort(), decoded!!.maxReceiversCount)
        assertEquals(0x0002.toUShort(), decoded.maxFirmwareImagesListSize)
        assertEquals(0x00010000u, decoded.maxFirmwareImageSize)
        assertEquals(0x00020000u, decoded.maxUploadSpace)
        assertEquals(0x0001F000u, decoded.remainingUploadSpace)
        assertTrue(decoded.supportedUriSchemes.isEmpty())
    }

    // endregion

    // region DistributionStatus — 디코더는 던지지 않고 null 을 반환해야 한다

    /**
     * `FirmwareDistributionStatus.init {}` 은 multicast 주소가 아니면 `require()` 로 예외를 던진다.
     * 수신 파싱 경로(`ModelEventHandler.decode()`)에는 try/catch 가 없어서, 이상값을 실은 PDU 하나가
     * RX 코루틴으로 예외를 escape 시킨다. NCS 는 group 이 multicast 인지 검증하지 않으므로
     * 유니캐스트가 실려 올 수 있다.
     */
    @Test
    fun `DistributionStatus 는 비multicast 주소를 예외 대신 null 로 거부`() {
        // status(1) phase(1) group(2) appkey(2) ttl(1) timeout(2) mode|policy(1) slot(2) = 12
        fun pdu(groupLo: Byte, groupHi: Byte) = byteArrayOf(
            0x00,           // status = SUCCESS
            0x00,           // phase = IDLE
            groupLo, groupHi,
            0x00, 0x00,     // app key index
            0x7F,           // ttl
            0x76, 0x00,     // timeout base
            0x05,           // transfer mode | update policy
            0x00, 0x00,     // firmware image index
        )

        // 유니캐스트 0x0001 은 DistributionMulticastAddress 가 아니다 → null (예외 아님)
        assertNull(
            "비multicast 주소는 null 로 거부돼야 한다 (예외를 던지면 RX 코루틴이 죽는다)",
            FirmwareDistributionStatus.init(parameters = pdu(0x01, 0x00))
        )
        // 그룹 주소 0xC000 은 정상
        assertNotNull(FirmwareDistributionStatus.init(parameters = pdu(0x00, 0xC0.toByte())))
        // Unassigned(0x0000) 도 스펙상 허용 (multicast 미사용 의미)
        assertNotNull(FirmwareDistributionStatus.init(parameters = pdu(0x00, 0x00)))
    }

    // endregion

    // region in-band Upload 인코더 — 우리 경로는 아니지만 자기모순 제거 확인

    /**
     * NCS `handle_upload_start()`:
     * `ttl(u8) | timeout(le16) | blobId(le64) | size(le32) | metaLen(u8) | meta[] | fwid[]`
     * → 고정부 16옥텟 (`BT_MESH_LEN_MIN(16)`).
     * upstream 인코더는 metaLen 옥텟을 빼먹고 meta/fwid 순서도 뒤바꿔, 자기 디코더가
     * (params[15]=metaLen 을 가정) 자기 출력을 못 읽었다.
     */
    @Test
    fun `UploadStart 인코더는 metadata 길이 옥텟을 포함하고 fwid 를 뒤에 둔다`() {
        val message = FirmwareDistributionUploadStart(
            ttl = 0x7Fu,
            timeoutBase = 0x0002u,
            blobId = 0x1122334455667788uL,
            firmwareSize = 0x00001000u,
            metadata = byteArrayOf(0xAAu.toByte(), 0xBBu.toByte()),
            firmwareId = FirmwareId(companyIdentifier = 0x0059u, version = byteArrayOf(0x01, 0x02)),
        )

        assertArrayEquals(
            byteArrayOf(
                0x7F,                                                       // ttl
                0x02, 0x00,                                                 // timeout base LE16
                0x88u.toByte(), 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,   // blob id LE64
                0x00, 0x10, 0x00, 0x00,                                     // firmware size LE32
                0x02,                                                       // metadata length
                0xAAu.toByte(), 0xBBu.toByte(),                             // metadata
                0x59, 0x00, 0x01, 0x02,                                     // fwid: CID LE16 + version
            ),
            message.parameters
        )
        assertTrue("펌웨어 BT_MESH_LEN_MIN(16)", message.parameters.size >= 16)
    }

    /**
     * NCS `handle_upload_start_oob()`: `uriLen(u8) | uri[uriLen] | fwid[]`.
     * upstream 인코더는 선행 uriLen 옥텟을 빼먹었다.
     */
    @Test
    fun `UploadOOBStart 인코더는 선행 URI 길이 옥텟을 포함한다`() {
        val message = FirmwareDistributionUploadOOBStart(
            url = java.net.URI("https://a.io/f").toURL(),
            currentFirmwareId = FirmwareId(companyIdentifier = 0x0059u),
        )

        assertEquals("첫 옥텟은 URI 길이", 14, message.parameters[0].toInt())
        assertArrayEquals(
            "https://a.io/f".encodeToByteArray(),
            message.parameters.copyOfRange(1, 15)
        )
        assertArrayEquals(byteArrayOf(0x59, 0x00), message.parameters.copyOfRange(15, 17))
    }

    // endregion
}
