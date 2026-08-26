package no.nordicsemi.kotlin.mesh.provisioning

import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.Storage
import no.nordicsemi.kotlin.mesh.core.model.IvIndex
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * simdo-patch (2026-08-26) — `MeshNetworkManager.create()` 를 테스트에서 쓰기 위한 최소 저장소.
 *
 * `MeshNetwork` 의 생성자는 `internal` 이라 `mesh:provisioning` 테스트에서 직접 만들 수 없다.
 * 공개 경로는 `MeshNetworkManager.create(...)` 뿐이고, 그것이 `Storage` /
 * `SecurePropertiesStorage` 를 요구한다.
 */
internal class InMemoryStorage : Storage {
    var bytes: ByteArray = ByteArray(0)
    override suspend fun load(): ByteArray = bytes
    override suspend fun save(network: ByteArray) {
        bytes = network
    }
}

@OptIn(ExperimentalUuidApi::class)
internal class InMemorySecureProperties : SecurePropertiesStorage {
    private val ivIndexes = mutableMapOf<Uuid, IvIndex>()
    private val sequenceNumbers = mutableMapOf<Pair<Uuid, UnicastAddress>, UInt>()
    private val lastSeqAuth = mutableMapOf<Pair<Uuid, UnicastAddress>, ULong>()
    private val previousSeqAuth = mutableMapOf<Pair<Uuid, UnicastAddress>, ULong>()
    private val localProvisioners = mutableMapOf<Uuid, String>()

    override suspend fun ivIndex(uuid: Uuid): IvIndex = ivIndexes[uuid] ?: IvIndex()

    override suspend fun storeIvIndex(uuid: Uuid, ivIndex: IvIndex) {
        ivIndexes[uuid] = ivIndex
    }

    override suspend fun nextSequenceNumber(uuid: Uuid, address: UnicastAddress): UInt =
        sequenceNumbers[uuid to address] ?: 0u

    override suspend fun storeNextSequenceNumber(
        uuid: Uuid,
        address: UnicastAddress,
        sequenceNumber: UInt,
    ) {
        sequenceNumbers[uuid to address] = sequenceNumber
    }

    override suspend fun resetSequenceNumber(uuid: Uuid, address: UnicastAddress) {
        sequenceNumbers.remove(uuid to address)
    }

    override suspend fun lastSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? =
        lastSeqAuth[uuid to source]

    override fun storeLastSeqAuthValue(uuid: Uuid, source: UnicastAddress, lastSeqAuth: ULong) {
        this.lastSeqAuth[uuid to source] = lastSeqAuth
    }

    override suspend fun previousSeqAuthValue(uuid: Uuid, source: UnicastAddress): ULong? =
        previousSeqAuth[uuid to source]

    override fun storePreviousSeqAuthValue(
        uuid: Uuid,
        source: UnicastAddress,
        seqAuth: ULong,
    ) {
        previousSeqAuth[uuid to source] = seqAuth
    }

    override suspend fun storeLocalProvisioner(uuid: Uuid, localProvisionerUuid: Uuid) {
        localProvisioners[uuid] = localProvisionerUuid.toString()
    }

    override suspend fun localProvisioner(uuid: Uuid): String? = localProvisioners[uuid]
}
