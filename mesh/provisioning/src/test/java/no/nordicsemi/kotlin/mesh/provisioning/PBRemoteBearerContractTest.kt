@file:OptIn(kotlin.uuid.ExperimentalUuidApi::class)

package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import no.nordicsemi.kotlin.data.toUuid
import no.nordicsemi.kotlin.mesh.bearer.BearerError
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.bearer.provisioning.ProvisioningBearer
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningExtendedScanReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanReport
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.oob.OobInformation
import no.nordicsemi.kotlin.mesh.provisioning.bearer.PBRemoteBearer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.Uuid

/**
 * PB-Remote bearer 의 **계약** 회귀 가드 (simdo-fork, 2026-08-12).
 *
 * 링크 상태머신 자체는 실 메시 트래픽(또는 전체 fake network harness)이 있어야 검증 가능하므로
 * 여기서는 다루지 않는다. 대신 `ProvisioningManager` **무수정 재사용**의 전제가 되는
 * 계약만 못박는다:
 *
 *  - `ProvisioningManager.init` 은 `require(bearer.supports(PduType.PROVISIONING_PDU))` 로
 *    베어러를 거른다. 여기서 걸리면 `BearerError.PduTypeNotSupported` 로 즉시 실패한다.
 *  - `ProvisioningManager.provision()` 은 `require(bearer.isOpen)` 로 시작한다.
 *
 * 즉 이 테스트가 깨지면 PB-Remote 로 프로비저닝을 시작하는 것 자체가 불가능해진다.
 */
class PBRemoteBearerContractTest {

    private val uuidBytes = byteArrayOf(
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10
    )
    private val testUuid: Uuid = uuidBytes.toUuid()
    private val serverAddress: UShort = 0x0002u

    private fun newBearer(
        uuid: Uuid? = testUuid,
        nppi: NodeProvisioningProtocolInterfaceProcedure? = null,
    ) = PBRemoteBearer(
        manager = MeshNetworkManager(
            storage = NoopStorage,
            secureProperties = NoopSecureProperties,
            ioDispatcher = Dispatchers.Unconfined,
        ),
        server = serverAddress,
        uuid = uuid,
        nppiProcedure = nppi,
    )

    @Test
    fun `PBRemoteBearer 는 ProvisioningBearer 라서 ProvisioningManager 를 고치지 않아도 된다`() {
        val bearer: ProvisioningBearer = newBearer()
        assertNotNull(bearer)
    }

    @Test
    fun `PBRemoteBearer 는 provisioning PDU 만 지원한다`() {
        val bearer = newBearer()
        assertArrayEqualsTypes(arrayOf(PduTypes.ProvisioningPdu), bearer.supportedTypes)
        // ProvisioningManager.init 의 require 가 보는 값
        assertTrue(bearer.supports(PduType.PROVISIONING_PDU))
        assertFalse(bearer.supports(PduType.NETWORK_PDU))
        assertFalse(bearer.supports(PduType.MESH_BEACON))
        assertFalse(bearer.supports(PduType.PROXY_CONFIGURATION))
    }

    @Test
    fun `PBRemoteBearer 는 open 전에 닫힌 상태다`() {
        val bearer = newBearer()
        assertFalse(bearer.isOpen)
        assertTrue(bearer.state.value is BearerEvent.Closed)
    }

    @Test
    fun `PBRemoteBearer 는 open 전에 send 하면 Closed 를 던진다`() = runBlocking {
        val bearer = newBearer()
        assertThrows(BearerError.Closed::class.java) {
            runBlocking { bearer.send(pdu = byteArrayOf(0x00, 0x05), type = PduType.PROVISIONING_PDU) }
        }
        Unit
    }

    @Test
    fun `PBRemoteBearer 는 지원하지 않는 PDU 타입을 거부한다`() {
        val bearer = newBearer()
        assertThrows(BearerError.PduTypeNotSupported::class.java) {
            runBlocking { bearer.send(pdu = byteArrayOf(0x00), type = PduType.NETWORK_PDU) }
        }
    }

    @Test
    fun `PBRemoteBearer 는 open 하지 않은 채 close 해도 예외가 없다`() = runBlocking {
        newBearer().close()
    }

    @Test
    fun `PBRemoteBearer 는 uuid 와 NPPI 중 정확히 하나만 허용한다`() {
        // 둘 다 없음
        assertThrows(IllegalArgumentException::class.java) { newBearer(uuid = null, nppi = null) }
        // 둘 다 있음
        assertThrows(IllegalArgumentException::class.java) {
            newBearer(
                uuid = testUuid,
                nppi = NodeProvisioningProtocolInterfaceProcedure.DEVICE_KEY_REFRESH
            )
        }
        // NPPI 단독은 허용 (Node Composition Refresh 등)
        assertNotNull(
            newBearer(
                uuid = null,
                nppi = NodeProvisioningProtocolInterfaceProcedure.NODE_COMPOSITION_REFRESH
            )
        )
    }

    // region Scan Report → UnprovisionedDevice

    @Test
    fun `ScanReport 는 UnprovisionedDevice 로 바로 변환된다`() {
        val report = RemoteProvisioningScanReport(
            rssi = -60,
            uuid = testUuid,
            oobInformationRaw = 0x0004u // QR Code
        )
        val device = report.toUnprovisionedDevice(name = "fallback")
        assertEquals(testUuid, device.uuid)
        assertEquals("fallback", device.name)
        assertEquals(OobInformation.QrCode, device.oobInformation)
    }

    @Test
    fun `복합 OOB 비트필드는 변환 시 None 으로 낮춰지고 예외가 나지 않는다`() {
        val report = RemoteProvisioningScanReport(
            rssi = -60,
            uuid = testUuid,
            oobInformationRaw = 0x8004u // On Device | QR Code — 단일 플래그가 아님
        )
        val device = report.toUnprovisionedDevice()
        assertEquals(OobInformation.None, device.oobInformation)
        assertEquals(testUuid, device.uuid)
    }

    @Test
    fun `ExtendedScanReport 의 localName 이 기기 이름이 된다`() {
        val name = "NEO-42"
        val ad = byteArrayOf((name.length + 1).toByte(), 0x09) + name.toByteArray(Charsets.UTF_8)
        val encoded = byteArrayOf(0x00) + uuidBytes + byteArrayOf(0x00, 0x00) + ad
        val report = RemoteProvisioningExtendedScanReport.init(encoded)!!

        val device = report.toUnprovisionedDevice(name = "fallback")
        assertEquals(name, device.name)
        assertEquals(testUuid, device.uuid)
    }

    @Test
    fun `ExtendedScanReport 에 이름이 없으면 fallback 이름을 쓴다`() {
        val report = RemoteProvisioningExtendedScanReport(
            status = RemoteProvisioningMessageStatus.SUCCESS,
            uuid = testUuid
        )
        assertEquals("fallback", report.toUnprovisionedDevice(name = "fallback").name)
    }

    // endregion

    private fun assertArrayEqualsTypes(expected: Array<PduTypes>, actual: Array<PduTypes>) {
        assertEquals(expected.toList(), actual.toList())
    }

    private object NoopStorage : Storage {
        override suspend fun load(): ByteArray = byteArrayOf()
        override suspend fun save(network: ByteArray) = Unit
    }

    private object NoopSecureProperties : SecurePropertiesStorage {
        override suspend fun ivIndex(uuid: Uuid) = IvIndex(index = 0u, isIvUpdateActive = false)
        override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) = Unit
        override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt = 1u
        override suspend fun storeNextSequenceNumber(
            uuid: Uuid,
            address: UnicastAddress,
            sequenceNumber: UInt,
        ) = Unit

        override suspend fun resetSequenceNumber(uuid: Uuid, address: UnicastAddress) = Unit
        override suspend fun lastSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storeLastSeqAuthValue(
            uuid: Uuid,
            source: UnicastAddress,
            lastSeqAuth: ULong,
        ) = Unit

        override suspend fun previousSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? = null
        override fun storePreviousSeqAuthValue(
            uuid: Uuid,
            source: UnicastAddress,
            seqAuth: ULong,
        ) = Unit

        override suspend fun storeLocalProvisioner(uuid: Uuid, localProvisionerUuid: Uuid) = Unit
        override suspend fun localProvisioner(uuid: Uuid): String? = null
    }
}
