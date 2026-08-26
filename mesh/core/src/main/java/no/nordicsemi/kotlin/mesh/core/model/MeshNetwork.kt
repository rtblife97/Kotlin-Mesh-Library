@file:Suppress("MemberVisibilityCanBePrivate", "unused", "PropertyName")

package no.nordicsemi.kotlin.mesh.core.model

import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import no.nordicsemi.kotlin.mesh.core.SecurePropertiesStorage
import no.nordicsemi.kotlin.mesh.core.exception.AddressAlreadyInUse
import no.nordicsemi.kotlin.mesh.core.exception.AddressNotInAllocatedRanges
import no.nordicsemi.kotlin.mesh.core.exception.CannotRemove
import no.nordicsemi.kotlin.mesh.core.exception.DoesNotBelongToNetwork
import no.nordicsemi.kotlin.mesh.core.exception.DuplicateKeyIndex
import no.nordicsemi.kotlin.mesh.core.exception.GroupAlreadyExists
import no.nordicsemi.kotlin.mesh.core.exception.GroupInUse
import no.nordicsemi.kotlin.mesh.core.exception.IvIndexTooSmall
import no.nordicsemi.kotlin.mesh.core.exception.KeyInUse
import no.nordicsemi.kotlin.mesh.core.exception.KeyIndexOutOfRange
import no.nordicsemi.kotlin.mesh.core.exception.NoAddressesAvailable
import no.nordicsemi.kotlin.mesh.core.exception.NoGroupRangeAllocated
import no.nordicsemi.kotlin.mesh.core.exception.NoNetworkKeysAdded
import no.nordicsemi.kotlin.mesh.core.exception.NoSceneNumberAvailable
import no.nordicsemi.kotlin.mesh.core.exception.NoSceneRangeAllocated
import no.nordicsemi.kotlin.mesh.core.exception.NoUnicastRangeAllocated
import no.nordicsemi.kotlin.mesh.core.exception.NodeAlreadyExists
import no.nordicsemi.kotlin.mesh.core.exception.OverlappingProvisionerRanges
import no.nordicsemi.kotlin.mesh.core.exception.ProvisionerAlreadyExists
import no.nordicsemi.kotlin.mesh.core.exception.SceneAlreadyExists
import no.nordicsemi.kotlin.mesh.core.exception.SceneInUse
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.model.serialization.UuidSerializer
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.ApplicationKeysConfig
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.DeviceKeyConfig
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.GroupsConfig
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.NetworkConfiguration
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.NetworkKeysConfig
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.NodesConfig
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.ProvisionersConfig
import no.nordicsemi.kotlin.mesh.core.model.serialization.config.ScenesConfig
import no.nordicsemi.kotlin.mesh.core.util.NetworkIdentity
import no.nordicsemi.kotlin.mesh.core.util.NodeIdentity
import no.nordicsemi.kotlin.mesh.crypto.Crypto
import java.lang.Integer.min
import kotlin.time.ExperimentalTime
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * MeshNetwork representing a Bluetooth mesh network.
 *
 * @property uuid                   128-bit Universally Unique Identifier (Uuid), which allows
 *                                  differentiation among multiple mesh networks.
 * @property name                   Human-readable name for the mesh network.
 * @property timestamp              Represents the last time the Mesh Object has been modified. The
 *                                  timestamp is based on Coordinated Universal Time.
 * @property partial                Indicates if this Mesh Configuration Database is part of a
 *                                  larger database.
 * @property networkKeys            List of network keys that includes information about network
 *                                  keys used in the mesh network.
 * @property applicationKeys        List of app keys that includes information about app keys used
 *                                  in the mesh network.
 * @property provisioners           List of known Provisioners and ranges of addresses that have
 *                                  been allocated to these Provisioners.
 * @property nodes                  List of nodes that includes information about mesh nodes in the
 *                                  mesh network.
 * @property groups                 List of groups that includes information about groups configured
 *                                  in the mesh network.
 * @property scenes                 List of scenes that includes information about scenes configured
 *                                  in the mesh network.
 * @property networkExclusions      List of excluded addresses per IvIndex.
 * @property ivIndex                IV Index of the network received via the last Secure Network
 *                                  Beacon and its current state.
 * @property localProvisioner       Main provisioner of the network which is the first provisioner
 *                                  in the list of provisioners.
 * @constructor                     Creates a mesh network.
 */
@ConsistentCopyVisibility
@OptIn(ExperimentalTime::class, ExperimentalUuidApi::class)
@Serializable
data class MeshNetwork internal constructor(
    @Serializable(with = UuidSerializer::class)
    @SerialName(value = "meshUUID")
    val uuid: Uuid = Uuid.random(),
    @SerialName(value = "meshName")
    private var _name: String,
    @SerialName("provisioners")
    internal var _provisioners: MutableList<Provisioner> = mutableListOf(),
    @SerialName("netKeys")
    internal var _networkKeys: MutableList<NetworkKey> = mutableListOf(),
    @SerialName("appKeys")
    internal var _applicationKeys: MutableList<ApplicationKey> = mutableListOf(),
    @SerialName("nodes")
    // simdo-fork (2026-06-08, Phase 3) — `_nodes` 를 internal→private 로 봉인.
    // [nodesMonitor] 단일 guardian 무결성을 **타입 시스템(컴파일러)으로 강제**한다: 다른 파일(Group/Scene/
    // Provisioner)에서의 raw 순회를 컴파일 에러로 차단 → guarded accessor([nodes] 스냅샷 / [node] / [withNodesLock])
    // 경유만 허용. (intra-class 접근은 private 라도 컴파일되므로 MeshNetwork.kt 내 apply()/serialize 보조 경로는
    // 그대로 — 이들은 deserialize/import 단일스레드 경로이며 hot writer 와 비동시.)
    private var _nodes: MutableList<Node> = mutableListOf(),
    @SerialName("groups")
    internal var _groups: MutableList<Group> = mutableListOf(),
    @SerialName("scenes")
    internal var _scenes: MutableList<Scene> = mutableListOf(),
    @SerialName("networkExclusions")
    internal var _networkExclusions: MutableList<ExclusionList> = mutableListOf(),
    @SerialName("timestamp")
    internal var _timestamp: Instant = Instant.fromEpochMilliseconds(System.currentTimeMillis())
) {
    var name: String
        get() = _name
        set(value) {
            require(value.isNotBlank()) { "Name cannot be empty!" }
            onChange(oldValue = _name, newValue = value) { updateTimestamp() }
            _name = value
        }

    val timestamp: Instant
        get() = _timestamp

    var partial: Boolean = false
        internal set(value) {
            onChange(oldValue = field, newValue = value) { updateTimestamp() }
            field = value
        }

    val provisioners: List<Provisioner>
        get() = _provisioners

    val networkKeys: List<NetworkKey>
        get() = _networkKeys

    val applicationKeys: List<ApplicationKey>
        get() = _applicationKeys

    /**
     * simdo-fork (2026-06-07, P6 M=2) — `_nodes` getter-vs-mutation race 의 non-suspend 가드.
     *
     * [cdbMutex] 는 suspend 코루틴 Mutex 라 일반 property getter([nodes])에서 잡을 수 없다. 한편
     * config 병렬화에서 N 동시 완료 + 동시 provisioning `add(node)` 가 `_nodes` 를 mutate 하는 동안
     * [nodes] getter 를 iterate 하는 consumer(앱 `MeshNodeManager` 등)가 있으면
     * ConcurrentModificationException 이 난다([defect_meshnetwork_nodes_unsynchronized_cme_crash]).
     *
     * 이 monitor 는 `_nodes` 의 **구조적 변이([add]/[remove]) 와 스냅샷([nodes] getter)** 만 직렬화한다.
     * (Node 내부 state 변이는 per-Node 객체라 무관.) suspend [cdbMutex] 보호와 직교 — cdbMutex 는
     * 고수준 CDB write/traversal 트랜잭션, 이 monitor 는 `_nodes` 리스트 컨테이너 자체의 무결성.
     */
    @Transient
    private val nodesMonitor = Any()

    val nodes: List<Node>
        get() = synchronized(nodesMonitor) { java.util.ArrayList(_nodes) }

    /**
     * simdo-fork (2026-06-08, P6) — [block] 을 [nodesMonitor] 아래에서 동기 실행한다.
     *
     * kotlinx 직렬화는 getter([nodes]) 가 아니라 backing `_nodes`(@SerialName("nodes")) 를 **직접**
     * 순회하므로([nodes] 방어복사 무용), serialize 트래버설을 이 헬퍼로 감싸 [add]/[remove] 구조 변이와
     * 상호배제한다(autosave 스레드 CME 봉인). non-suspend·동기 — 순수 in-memory 인코딩만 감싼다.
     */
    internal fun <T> withNodesLock(block: () -> T): T = synchronized(nodesMonitor) { block() }

    val groups: List<Group>
        get() = _groups

    val scenes: List<Scene>
        get() = _scenes

    internal val networkExclusions: List<ExclusionList>
        get() = _networkExclusions

    @Transient
    var ivIndex = IvIndex()
        internal set

    /**
     * simdo-fork (2026-06-06) — Fork-3 확장(P4: 병렬 provisioning CDB mutation race).
     *
     * 이 network 의 mutable CDB collection(`_nodes`/`_netKeys`/`_appKeys`/`_groups`/model subscribe 등)에
     * 대한 **모든** in-memory write/traversal 을 직렬화하는 canonical lock. Fork-3(2026-06-05)이
     * [no.nordicsemi.kotlin.mesh.core.layers.NetworkManager] 에 두었던 `cdbMutex` 를 mutated 대상인
     * MeshNetwork 로 hoist 한다 — 그래야 config-status RX 직렬화(NetworkManager 가 본 lock 에 위임)와
     * **병렬 provisioning 의 `add(node)`**([no.nordicsemi.kotlin.mesh.provisioning.ProvisioningManager])이
     * **같은 lock 인스턴스**를 공유한다. (provisioning 모듈은 NetworkManager 를 모르지만 MeshNetwork 는
     * 안다 → 결합 없이 공유 성립.)
     *
     * @Transient — 직렬화 제외. 네트워크 swap(import) 시 새 MeshNetwork 가 자기 lock 을 갖는다(다른
     * CDB = 다른 lock = 정확).
     *
     * **주의**(Fork-3 불변식 보존): send / RTT 대기 / reply(PDU 송신) 는 이 lock 밖에 둔다 →
     * 병렬 in-flight throughput 보존. 본 lock 은 순수 in-memory CDB write/traversal 만 감싼다.
     * kotlinx Mutex 는 비재진입이므로 lock 안에서 다시 [withCdbLock] 류를 호출하지 않는다.
     */
    // public: mesh:provisioning 모듈(별 Gradle 모듈)의 ProvisioningManager 가 add(node) 를 이 lock 으로
    // 감싸야 하는데 internal 은 모듈 경계를 못 넘는다. add(node)/remove(uuid) 가 public 인 것과 정합.
    @Transient
    val cdbMutex: kotlinx.coroutines.sync.Mutex = kotlinx.coroutines.sync.Mutex()

    /**
     * [block] 을 [cdbMutex] 아래에서 실행 — CDB write/traversal 직렬화 진입점.
     * provisioning add 처럼 MeshNetwork 만 아는 호출부가 NetworkManager 우회 없이 같은 lock 을 쓰게 한다.
     */
    suspend fun <T> withCdbLock(block: suspend () -> T): T =
        cdbMutex.withLock { block() }

    val localProvisioner: Provisioner?
        get() = _provisioners.firstOrNull()

    val primaryNetworkKey: NetworkKey?
        get() = _networkKeys.find { it.isPrimary }

    /**
     * THe next available network key index, or null if the index 4095 is already in use.
     *
     * Note: This method searches for any available key index that is not used,looking for gaps in
     * the key indexes. If there are no gaps, the next available key index will be the first one
     * after the last one.
     */
    val nextAvailableNetworkKeyIndex: KeyIndex?
        get() {
            if (_networkKeys.isEmpty()) return 0u
            for (index in 0..4095) {
                val keyIndex = index.toUShort()
                if (!_networkKeys.any { it.index == keyIndex }) return keyIndex
            }
            return null
        }

    /**
     * Returns the next available application key index that can be used
     * when construction an application key.
     *
     * Note: This method searches for any available key index that is not used,looking for gaps in
     * the key indexes. If there are no gaps, the next available key index will be the first one
     * after the last one.
     */
    val nextAvailableApplicationKeyIndex: KeyIndex?
        get() {
            if (_applicationKeys.isEmpty()) return 0u
            for (index in 0..4095) {
                val appKeyIndex = index.toUShort()
                if (!_applicationKeys.any { it.index == appKeyIndex }) return appKeyIndex
            }
            return null
        }

    @Transient
    internal var _localElements = mutableListOf(Element(location = Location.MAIN))
        set(elements) {
            // Ensures the indexes are correct
            elements.forEachIndexed { index, element ->
                element.index = index
                element.parentNode = localProvisioner?.node
            }
            field = elements
            // Ensure there is enough address space for all the Elements that are nto taken by other
            // Nodes and are in the local Provisioner's address range. If required, cut the element
            // array.
            localProvisioner?.let { provisioner ->
                provisioner.node?.let { node ->
                    var availableElements = elements
                    val availableElementsCount = provisioner.maxElementCount(
                        address = node.primaryUnicastAddress
                    )
                    if (availableElementsCount < elements.size) {
                        availableElements = elements
                            .dropLast(n = elements.size - availableElementsCount)
                            .toMutableList()
                    }
                    // Assign the Elements to the Provisioner's node
                    node.set(elements = availableElements)
                }
            }
        }

    internal val localElements: List<Element>
        get() = _localElements


    /**
     * Convenience constructor to create a network for tests
     *
     * @param name The name of the network
     */
    internal constructor(name: String) : this(name = name, uuid = Uuid.random())

    /**
     * Convenience constructor to create a network.
     *
     * @param name The name of the network
     * @param uuid The Uuid of the network
     */
    internal constructor(name: String, uuid: Uuid = Uuid.random()) : this(
        uuid = uuid,
        _name = name
    )

    /**
     * Updates timestamp to the current time in milliseconds.
     */
    internal fun updateTimestamp() {
        this._timestamp = Instant.fromEpochMilliseconds(System.currentTimeMillis())
    }

    /**
     * Sets new value of IV Index and IV Update flag.
     *
     * This method allows setting the IV Index of the mesh network when the provisioner is not
     * connected to the network and did not receive the Secure Network beacon, for example to
     * provision a Node.
     *
     * Otherwise, if the local Node is connecting to the mesh network using GATT Proxy, it will
     * obtain the current IV Index automatically just after connection using the Secure Network
     * beacon, in which case calling this method is not necessary.
     *
     * Note: If there are no Nodes in the network except the Provisioner, it is not possible to
     *       revert IV Index to smaller value (at least not using the public API). If you set an IV
     *       Index that's too high, the app will not be able to communicate with the mesh network.
     *       Always use the current IV Index of the mesh network.
     *
     *       Once this method is called, ensure to call [SecurePropertiesStorage.storeIvIndex]
     *
     * @param index            32-bit integer value.
     * @param isIvUpdateActive true if the IV Update is active, false otherwise.
     * @throws IvIndexTooSmall if the IV Index value is lower than the current.
     */
    @Throws(IvIndexTooSmall::class)
    fun setIvIndex(index: UInt, isIvUpdateActive: Boolean = false) {
        require(isIvIndexUpdateAllowed(index = index, updateActive = isIvUpdateActive)) {
            throw IvIndexTooSmall()
        }
        val newIvIndex = IvIndex(index = index, isIvUpdateActive = isIvUpdateActive)
        if (ivIndex == newIvIndex) {
            // No change in IV Index, no need to update
            return
        }
        ivIndex = newIvIndex
    }

    /**
     * Checks if the IV Index can be updated.
     *
     * The IV Index can be updated only when the network has no Nodes
     *
     * @param index          The new IV Index to be set.
     * @param updateActive   True if the IV Update is active, false otherwise.
     * @return True if the IV Index can be updated, false otherwise.
     */
    fun isIvIndexUpdateAllowed(
        index: UInt = ivIndex.index,
        updateActive: Boolean = ivIndex.isIvUpdateActive,
    ): Boolean {
        val newIvIndex = IvIndex(index = index, isIvUpdateActive = updateActive)
        // The IV Index can be changed only when the network has no Nodes
        // except the local Provisioner.
        val onlyProvisioner = nodes.none { !it.isLocalProvisioner }
        return onlyProvisioner || newIvIndex >= ivIndex
    }

    /**
     * Returns a provisioner with the given Uuid.
     *
     * @param uuid Uuid of the provisioner.
     * @return Provisioner with the given Uuid or null otherwise
     */
    fun provisioner(uuid: Uuid): Provisioner? = provisioners.firstOrNull { it.uuid == uuid }

    /**
     * Checks if a provisioner with the given Uuid already exists in the network.
     *
     * @param provisioner provisioner to check.
     * @return true if the provisioner exists.
     */
    internal fun has(provisioner: Provisioner) = provisioner(uuid = provisioner.uuid) != null

    /**
     * Adds the given [Provisioner] to the list of provisioners in the network.
     *
     * @param provisioner Provisioner to be added.
     * @throws [ProvisionerAlreadyExists] if the provisioner already exists.
     * @throws [DoesNotBelongToNetwork] if the provisioner does not belong to this network.
     * @throws [NoAddressesAvailable] if no address is available to be assigned.
     * @throws [OverlappingProvisionerRanges] if the given provisioner has any overlapping address
     * ranges with an existing provisioner.
     */
    @Throws(
        ProvisionerAlreadyExists::class,
        DoesNotBelongToNetwork::class,
        NoAddressesAvailable::class,
        OverlappingProvisionerRanges::class
    )
    fun add(provisioner: Provisioner) {
        add(
            provisioner = provisioner,
            address = nextAvailableUnicastAddress(elementCount = 1, provisioner = provisioner)
                ?: throw NoAddressesAvailable()
        )
    }

    /**
     * Adds the given Provisioner with the given address to the list of provisioners in the network.
     *
     * @param provisioner Provisioner to be added.
     * @throws [ProvisionerAlreadyExists] if the provisioner already exists.
     * @throws [DoesNotBelongToNetwork] if the provisioner does not belong to this network.
     * @throws [OverlappingProvisionerRanges] if the given provisioner has any overlapping address
     * ranges with an existing provisioner.
     */
    @Throws(OverlappingProvisionerRanges::class)
    fun add(provisioner: Provisioner, address: UnicastAddress?) {
        require(provisioner(uuid = provisioner.uuid) == null) { throw ProvisionerAlreadyExists() }
        require(provisioner.network == null || provisioner.network?.uuid == uuid) {
            throw DoesNotBelongToNetwork()
        }

        for (other in _provisioners) {
            require(!provisioner.hasOverlappingRanges(other)) {
                throw OverlappingProvisionerRanges()
            }
        }

        address?.apply {
            // Is the given address inside provisioner's address range?
            require(provisioner._allocatedUnicastRanges.any { it.contains(this.address) }) {
                throw AddressNotInAllocatedRanges()
            }
            // No other node uses the same address?
            // simdo-fork (2026-06-08, P6) — _nodes raw 순회를 nodesMonitor 아래로(동시 add/remove 와 배타).
            require(!synchronized(nodesMonitor) { _nodes.any { it.containsElementWithAddress(this) } }) {
                throw AddressAlreadyInUse()
            }
        }

        // Is it already added?
        require(!has(provisioner)) { return }

        // is there a node with the provisioner's uuid
        // simdo-fork (2026-06-08, P6) — _nodes raw 순회를 nodesMonitor 아래로(동시 add/remove 와 배타).
        require(synchronized(nodesMonitor) { _nodes.none { it.uuid == provisioner.uuid } }) {
            throw NodeAlreadyExists()
        }

        // Add the provisioner's node
        address?.let { unicastAddress ->
            val node = Node(
                provisioner = provisioner,
                unicastAddress = unicastAddress,
                netKeys = networkKeys,
                appKeys = applicationKeys
            ).apply {
                if (provisioners.isEmpty()) {
                    add(elements = localElements)
                    companyIdentifier = 0x00E0u // Google
                    replayProtectionCount = maxUnicastAddress
                    name = provisioner.name
                } else {
                    add(element = Element.primaryElement)
                }
            }
            add(node)
        }
        // And finally, add the Provisioner.
        provisioner.network = this
        _provisioners.add(provisioner).also { updateTimestamp() }
    }

    /**
     * Removes the provisioner at the given index.
     * Note: It is not possible to remove the last provisioner.
     *
     * @param index The position of the provisioner to be removed.
     * @return Provisioner that was removed.
     * @throws CannotRemove if there is only one provisioner.
     */
    @Throws(CannotRemove::class)
    internal fun removeProvisioner(index: Int): Provisioner {
        require(_provisioners.size > 1) { throw CannotRemove() }

        val localProvisionerRemoved = index == 0
        val provisioner = _provisioners
            .removeAt(index = index)
            .also {
                remove(uuid = it.uuid)
                it.network = null
            }

        // If the old local Provisioner has been removed, and a new one has been set in its place,
        // it needs the properties to be updated.
        if (localProvisionerRemoved) {
            localProvisioner?.node?.apply {
                assignNetKeys(networkKeys)
                assignAppKeys(applicationKeys)
                companyIdentifier = 0x00E0u // Google
                replayProtectionCount = maxUnicastAddress
                // The Element adding has to be done this way. Some Elements may get cut
                // by the property observer when Element addresses overlap other Node's
                // addresses.
                val elements = localElements
                _localElements = elements.toMutableList()
            }
        }
        updateTimestamp()
        return provisioner
    }

    /**
     * Removes the given provisioner from the list of provisioners in the network.
     *
     * @param provisioner Provisioner to be removed.
     * @throws DoesNotBelongToNetwork if the provisioner does not belong to this network.
     * @throws CannotRemove if there is only one provisioner.
     */
    @Throws(DoesNotBelongToNetwork::class, CannotRemove::class)
    fun remove(provisioner: Provisioner) {
        require(provisioner.network?.uuid == uuid) { throw DoesNotBelongToNetwork() }
        removeProvisioner(_provisioners.indexOf(provisioner))
    }

    /**
     * Removes the provisioner with the given Uuid from the list of provisioners in the network.
     *
     * @param uuid Uuid of the provisioner to be removed.
     * @throws DoesNotBelongToNetwork if provisioner does not belong to this network.
     * @throws CannotRemove if there is only one provisioner.
     * @throws NoSuchElementException if a provisioner with the given Uuid was not found.
     */
    @Throws(DoesNotBelongToNetwork::class, CannotRemove::class)
    fun removeProvisionerWithUuid(uuid: Uuid) {
        provisioner(uuid)?.let { provisioner ->
            remove(provisioner)
        }
    }

    /**
     * Moves the provisioner from the given 'from' index to the specified 'to' index.
     *
     * @param from      Current index of the provisioner.
     * @param to        Destination index, the provisioner must be moved to.
     * @return Provisioner that was removed.
     * @throws DoesNotBelongToNetwork if the provisioner does not belong to this network.
     * @throws CannotRemove if there is only one provisioner.
     */
    @Throws(DoesNotBelongToNetwork::class)
    internal fun moveProvisioner(from: Int, to: Int) {
        require(from >= 0 && from < _provisioners.size) {
            throw IllegalArgumentException("Invalid 'from' index!")
        }
        require(to >= 0 && to < _provisioners.size) {
            throw IllegalArgumentException("Invalid 'to' index!")
        }
        require(from != to) { return }
        try {
            val oldLocalProvisioner = if (from == 0 || to == 0) localProvisioner else null
            val provisioner = _provisioners.removeAt(index = from)

            // The target index must be modified if the Provisioner is being moved below, as it was
            // removed and other Provisioners were already moved to fill the space.
            val newToIndex = if (to > from + 1) to - 1 else to
            if (newToIndex < _provisioners.size)
                _provisioners.add(index = to, element = provisioner)
            else _provisioners.add(element = provisioner)
            updateTimestamp()
            // If a local Provisioner was moved, it's Composition Data must be cleared, as most
            // probably it will be exported to another phone, which will have its own manufacturer,
            // Elements, etc.
            //
            // simdo-fork (2026-08-25) — `defaultTTL = null` 제거 (양쪽 블록).
            //
            // 이 블록의 의도는 **Composition Data Page 0**(Mesh Profile 1.0.1 §4.2.1: CID/PID/VID/
            // Elements) 를 지우는 것이다 — 그 값들은 "어느 폰이 이 Provisioner 노드를 들고 있는가"에
            // 종속이라 export/import 시 무의미해지기 때문. 그런데 `defaultTTL` 은 Composition Data 가
            // 아니라 **Foundation 계층 configuration state**(§4.2.7 Default TTL, Config Default TTL
            // Get/Set §4.3.2.24-25) 다. 운영자가 "이 망은 홉이 깊다"고 판단해 고른 네트워크 정책이며,
            // 폰이 바뀌어도 유지되어야 한다(주소 범위·키 인덱스가 유지되는 것과 같은 이유).
            //
            // 실측 회귀(신동 광산, 2026-08-25): CDB 의 provisioner 12개가 전부 defaultTTL=40 인데
            // (nRF Mesh Android 앱에서 수동 설정) 앱이 자기 provisioner 를 local 로 올리려고
            // `move(to = 0)` 하는 순간 구 local·신 local 양쪽의 40 이 지워졌다. 송신 TTL 우선순위는
            // `initialTtl ?: localProvisioner.node.defaultTTL ?: networkParameters.defaultTtl` 이므로
            // 값이 지워지면 조용히 라이브러리 기본값 5 로 떨어진다 = 최대 5홉(≈750 m). 실측 21~23홉
            // (3,377 m) 갱도에서는 원격 노드 제어/설정이 구조적으로 불가능해진다.
            //
            // ∴ Composition Data 3필드 + Elements 초기화는 그대로 두고 `defaultTTL` 만 보존한다.
            if (newToIndex == 0 || from == 0) {
                oldLocalProvisioner?.node?.apply {
                    companyIdentifier = null
                    productIdentifier = null
                    versionIdentifier = null
                    // After exporting and importing the mesh network configuration on
                    // another phone, that phone will update the local Elements array.
                    // As the final Elements count is unknown at this place, just add
                    // the required Element.
                    elements.forEach { element ->
                        element.parentNode = null
                        element.index = 0
                    }
                    set(elements = listOf(Element.primaryElement))
                }
            }
            // If a Provisioner was moved to index 0 it becomes the new local Provisioner. The local
            // Provisioner is, by definition, aware of all Network and Application Keys currently
            // existing in the network.
            if (newToIndex == 0 || from == 0) {
                localProvisioner?.node?.apply {
                    companyIdentifier = 0x00E0u // Google
                    productIdentifier = null
                    versionIdentifier = null
                    // simdo-fork (2026-08-25) — `defaultTTL = null` 제거. 위 블록의 근거 참조.
                    // 새로 local 이 되는 Provisioner 의 CDB defaultTTL(예: 40)이 여기서 지워지면
                    // 그 즉시 송신 TTL 이 networkParameters 기본값으로 떨어진다.
                    // After exporting and importing the mesh network configuration on
                    // another phone, that phone will update the local Elements array.
                    // As the final Elements count is unknown at this place, just add
                    // the required Element.
                    val elements = _localElements
                    _localElements = elements
                }
            }
        } catch (e: Exception) {
            throw IllegalStateException("Error while moving provisioner!", e)
        }
    }

    /**
     * Moves the given provisioner to the specified index.
     *
     * @param provisioner   Provisioner to be removed.
     * @param to            Destination index, the provisioner must be moved to.
     * @return Provisioner that was removed.
     * @throws DoesNotBelongToNetwork if the provisioner does not belong to this network.
     * @throws CannotRemove if there is only one provisioner.
     */
    @Throws(DoesNotBelongToNetwork::class)
    fun move(provisioner: Provisioner, to: Int) {
        require(provisioner.network?.uuid == uuid) { throw DoesNotBelongToNetwork() }
        _provisioners.indexOf(provisioner).takeIf { it > -1 }?.let { from ->
            moveProvisioner(from = from, to = to)
        }
    }

    /**
     * Disables the configuration capabilities by un-assigning provisioner's address. Un-assigning
     * an address will delete the provisioner's node. This results in the provisioner not being
     * able to send or receive mesh messages in the mesh network. However, the provisioner will
     * still retain it's provisioning capabilities.
     *
     * @param provisioner Provisioner of whose configurations are to be disabled.
     */
    fun disableConfigurationCapabilities(provisioner: Provisioner) {
        remove(provisioner.uuid)
    }

    /**
     * Returns the network key with a given key index.
     *
     * @param index Index of the network key.
     * @return Network key.
     * @throws NoSuchElementException if a key for a given key index ws not found.
     */
    fun networkKey(index: KeyIndex) = networkKeys.firstOrNull { key ->
        key.index == index
    }

    /**
     * Adds the given [NetworkKey] to the list of network keys in the network.
     *
     * @param name      Network key name.
     * @param key       128-bit key to be added.
     * @param index     Network key index.
     * @throws [KeyIndexOutOfRange] if the key index is not within 0 - 4095.
     * @throws [DuplicateKeyIndex] if the key index is already in use.
     */
    @Throws(KeyIndexOutOfRange::class, DuplicateKeyIndex::class)
    fun add(
        name: String,
        key: ByteArray = Crypto.generateRandomKey(),
        index: KeyIndex? = null,
    ): NetworkKey {
        if (index != null) {
            // Check if the network key index is not already in use to avoid duplicates.
            require(networkKey(index = index) == null) { throw DuplicateKeyIndex() }
        }
        return NetworkKey(
            index = (index ?: nextAvailableNetworkKeyIndex) ?: throw KeyIndexOutOfRange(),
            _name = name,
            _key = key
        ).also { add(key = it) }
    }

    /**
     * Adds the given [NetworkKey] to the list of network keys in the network.
     *
     * This method will also add the network key to the local Provisioner's node,
     *
     * @param key Network key to be added.
     */
    internal fun add(key: NetworkKey) {
        key.network = this
        // Add the new network key to the network keys and sort them by index.
        _networkKeys
            .apply { add(element = key) }
            .sortBy { it.index }
        updateTimestamp()
        // Make the local Provisioner aware of the new key.
        localProvisioner?.node?.add(key = key)
    }

    /**
     * Removes a given [NetworkKey] from the list of network keys in the mesh network.
     *
     * @param key Network key to be removed.
     * @throws [DoesNotBelongToNetwork] if the key does not belong to this network.
     * @throws [KeyInUse] if the key is known to any node in the network or bound to any application
     *                    key in this network.
     */
    @Throws(DoesNotBelongToNetwork::class, KeyInUse::class)
    fun remove(key: NetworkKey, force: Boolean = false) {
        removeNetworkKeyWithIndex(index = key.index, force = force)
    }

    /**
     * Removes a given [NetworkKey] from the list of network keys in the mesh network.
     *
     * @param index KeyIndex of the network key to be removed.
     * @param force If true, the network key will be removed even if it is in use.
     */
    fun removeNetworkKeyWithIndex(index: KeyIndex, force: Boolean = false) {
        removeNetworkKeyAtIndex(
            index = networkKeys.indexOfFirst { it.index == index },
            force = force
        )
    }

    /**
     * Removes a Network Key at the given index from the list of network keys in the mesh network.
     *
     * @param index index of the network key in the list of network keys.
     * @param force If true, the network key will be removed even if it is in use.
     */
    fun removeNetworkKeyAtIndex(index: Int, force: Boolean = false) {
        // Return as no op if the key does not exist
        val key = networkKeys.getOrNull(index) ?: return
        require(force || key.network?.uuid == uuid) { throw DoesNotBelongToNetwork() }
        require(force || !key.isInUse) { throw KeyInUse() }
        _networkKeys.removeAt(index = index)
        // Remove the key from the local node
        localProvisioner?.node?.removeNetKey(index = key.index)
        updateTimestamp()
    }

    /**
     * Returns the application key with a given key index.
     *
     * @param index Index of the application key.
     * @return Application key.
     * @throws NoSuchElementException if a key for a given key index ws not found.
     */
    fun applicationKey(index: KeyIndex) = applicationKeys.firstOrNull { key -> key.index == index }

    /**
     * Adds the given [ApplicationKey] to the list of network keys in the network.
     *
     * @param name Application key name.
     * @param key 128-bit key to be added.
     * @param index Application key index.
     * @param boundNetworkKey Network key to which the application key must be bound to.
     * @throws [KeyIndexOutOfRange] if the key index is not within 0 - 4095.
     * @throws [DuplicateKeyIndex] if the key index is already in use.
     */
    @Throws(KeyIndexOutOfRange::class, DuplicateKeyIndex::class, IllegalArgumentException::class)
    fun add(
        name: String,
        key: ByteArray = Crypto.generateRandomKey(),
        index: KeyIndex? = null,
        boundNetworkKey: NetworkKey,
    ): ApplicationKey {
        // Check if the network key belongs to the same network.
        require(boundNetworkKey.network?.uuid == uuid) {
            throw IllegalArgumentException(
                "Network key ${boundNetworkKey.name} does not belong to network $name!"
            )
        }
        if (index != null) {
            // Check if the application key index is not already in use to avoid duplicates.
            require(applicationKey(index = index) == null) { throw DuplicateKeyIndex() }
        }
        return ApplicationKey(
            index = (index ?: nextAvailableApplicationKeyIndex) ?: throw KeyIndexOutOfRange(),
            _name = name,
            _key = key
        ).apply {
            boundNetKeyIndex = boundNetworkKey.index
        }.also { add(key = it) }
    }

    /**
     * Adds the given [ApplicationKey] to the list of application keys in the network.
     *
     * This method will also add the network key to the local Provisioner's node,
     *
     * @param key Application Key to be added.
     */
    internal fun add(key: ApplicationKey) {
        key.network = this
        // Add the new network key to the network keys and sort them by index.
        _applicationKeys
            .apply { add(element = key) }
            .sortBy { it.index }
        updateTimestamp()
        // Make the local Provisioner aware of the new key.
        localProvisioner?.node?.add(key = key)
    }

    /**
     * Removes a given [ApplicationKey] from the list of application keys in the mesh network.
     *
     * @param key Application key to be removed.
     * @throws [DoesNotBelongToNetwork] if the key does not belong to this network.
     * @throws [KeyInUse] if the key is known to any node in the network.
     */
    @Throws(DoesNotBelongToNetwork::class, KeyInUse::class)
    fun remove(key: ApplicationKey, force: Boolean = false) {
        removeApplicationKeyWithIndex(index = key.index, force = force)
    }

    /**
     * Removes an Application Key with the given [KeyIndex].
     *
     * @param index KeyIndex of the Application Key to be removed.
     * @param force If true, the Application Key will be removed even if it is in use.
     * @throws [DoesNotBelongToNetwork] if the key does not belong to this network.
     * @throws [KeyInUse] if the key is known to any node in the
     */
    @Throws(DoesNotBelongToNetwork::class, KeyInUse::class)
    fun removeApplicationKeyWithIndex(index: KeyIndex, force: Boolean = false) {
        removeApplicationKeyAtIndex(
            index = applicationKeys.indexOfFirst { it.index == index },
            force = force
        )
    }

    /**
     * Removes an Application Key at the given index from the list of Application Keys in the mesh
     * network.
     *
     * @param index index of the Application Key in the list of Application Keys.
     * @param force If true, the Application Key will be removed even if it is in use.
     * @throws [DoesNotBelongToNetwork] if the key does not belong to this network.
     * @throws [KeyInUse] if the key is known to any node in the
     */
    @Throws(DoesNotBelongToNetwork::class, KeyInUse::class)
    internal fun removeApplicationKeyAtIndex(index: Int, force: Boolean = false) {
        // Return as no op if the key does not exist
        val key = applicationKeys.getOrNull(index) ?: return
        require(force || key.network?.uuid == uuid) { throw DoesNotBelongToNetwork() }
        require(force || !key.isInUse) { throw KeyInUse() }
        _applicationKeys.removeAt(index = index)
        // Remove the key from the local node
        localProvisioner?.node?.removeAppKey(index = key.index)
        updateTimestamp()
    }

    /**
     * Returns the provisioner's node or null, if the provisioner is not a part of the network or
     * does not have an address assigned.
     *
     * @param provisioner Provisioner whose node is to be returned.
     * @return Null if the provisioner is not a part of the network or if the provisioner does not
     *         have an address assigned.
     */
    fun node(provisioner: Provisioner) = try {
        require(provisioner.network?.uuid == uuid) { throw DoesNotBelongToNetwork() }
        require(has(provisioner = provisioner)) { return null }
        node(uuid = provisioner.uuid)
    } catch (e: DoesNotBelongToNetwork) {
        null
    }

    /**
     * Returns the provisioned node containing an element with the given address.
     *
     * @param address Address of the element.
     * @return Node if an element with the given address was found, null otherwise.
     */
    fun node(address: Address) = try {
        node(MeshAddress.create(address))
    } catch (e: IllegalArgumentException) {
        null
    }

    /**
     * Returns the provisioned node containing an element with the given mesh address.
     *
     * @param address Mesh Address of the element.
     * @return Node if an element with the given address was found, null otherwise.
     */
    // simdo-fork (2026-06-08, P6) — getter([nodes], 매 호출 ArrayList 전체 복사) 우회. RX hot path 가
    // 매 inbound PDU 의 src 를 이 lookup 으로 해소하므로 O(N) alloc 압박을 없애고, 동시에 [_nodes] 직접
    // find 를 [nodesMonitor] 아래로 넣어 [add]/[remove] 구조 변이와 상호배제한다(find 중 CME 봉인).
    fun node(address: MeshAddress) = address.takeIf { it is UnicastAddress }?.let { addr ->
        synchronized(nodesMonitor) {
            _nodes.firstOrNull { it.containsElementWithAddress(addr as UnicastAddress) }
        }
    }

    /**
     * Returns the node with the given uuid.
     *
     * @param uuid matching Uuid.
     * @return Node
     */
    // simdo-fork (2026-06-08, P6) — getter 우회 + [nodesMonitor] 아래 [_nodes] 직접 find (위 [node] 참조).
    fun node(uuid: Uuid) = synchronized(nodesMonitor) { _nodes.find { it.uuid == uuid } }

    /**
     * Returns the node with the given node identity.
     *
     * @param nodeIdentity Node identity.
     * @return Node or null otherwise.
     */
    // simdo-fork (2026-06-08, P6) — getter 우회 + [nodesMonitor] 아래 [_nodes] 직접 find (위 [node] 참조).
    fun node(nodeIdentity: NodeIdentity) =
        synchronized(nodesMonitor) { _nodes.find { nodeIdentity.matches(it) } }

    /**
     * simdo-fork (2026-06-08, Phase 3) — deserialize 직후 parent 참조 복원을 MeshNetwork 내부로 이동.
     *
     * 종전엔 [no.nordicsemi.kotlin.mesh.core.model.serialization.MeshNetworkSerializer.deserialize] 가
     * decode 직후 `_nodes`(외 `_networkKeys`/`_groups`/… backing field)를 raw 순회하며 `node.network=this`
     * 등 parent 링크를 재설정했다. `_nodes` 를 private 로 봉인하면 그 cross-file 순회가 컴파일되지 않으므로,
     * 이 wiring 을 MeshNetwork 의 멤버로 끌어와 intra-class 접근(private 허용)으로 만든다. 의미 동일 —
     * deserialize 단일스레드 경로라 lock 불필요(hot writer 와 비동시), [_nodes] 무결성도 그대로 보존.
     */
    internal fun rewireAfterDeserialize() {
        _networkKeys.forEach { it.network = this }
        _applicationKeys.forEach { it.network = this }
        _groups.forEach { it.network = this }
        _scenes.forEach { it.network = this }
        _provisioners.forEach { it.network = this }
        _nodes.forEach { node ->
            node.network = this
            node.elements.forEach { element ->
                element.parentNode = node
                element.models.forEach { model ->
                    model.parentElement = element
                }
            }
        }
        _networkExclusions.forEach { it.network = this }
    }

    /**
     * Adds a given [Node] to the list of nodes in the mesh network.
     *
     * @param node                         Node to be added to the network.
     * @throws NodeAlreadyExists           If the node already exists.
     * @throws NoAddressesAvailable        If the node is not assigned with an address.
     * @throws NoNetworkKeysAdded          If the node does not contain a network key.
     * @throws DoesNotBelongToNetwork      If the network key in the node does not match the keys in
     *                                     the network.
     */
    @Throws(
        NodeAlreadyExists::class,
        NoAddressesAvailable::class,
        NoNetworkKeysAdded::class,
        DoesNotBelongToNetwork::class
    )
    fun add(node: Node) = synchronized(nodesMonitor) {
        // simdo-fork (2026-06-08, P6) — precheck(node()/isAddressAvailable 가 _nodes 를 raw iterate)부터
        // insert·updateTimestamp 까지 **전체**를 nodesMonitor 로 묶는다. 종전엔 insert(:910)만 monitor
        // 라 precheck 의 _nodes 순회가 동시 remove/serialize 와 CME 났고, find-then-add 도 비원자였다.
        // 내부 node()/isAddressAvailable 도 nodesMonitor 를 잡지만 JVM monitor 는 재진입이라 deadlock 없음.
        // Ensure the node does not exist already.
        require(node(uuid = node.uuid) == null) { throw NodeAlreadyExists() }
        // Verify if the address range is available for the new Node.
        require(isAddressAvailable(address = node.primaryUnicastAddress, node = node)) {
            throw NoAddressesAvailable()
        }
        // Ensure the Network Key exists.
        require(node.netKeys.isNotEmpty()) { throw NoNetworkKeysAdded() }
        // Make sure the network contains a Network Key with the same Key Index.
        require(_networkKeys.any { it.index == node.netKeys.first().index }) {
            throw DoesNotBelongToNetwork()
        }
        _nodes.add(node.also { it.network = this })
        updateTimestamp()
    }

    /**
     * Removes a given node from the list of nodes in the mesh network.
     *
     * @param node Node to be removed.
     */
    fun remove(node: Node) {
        remove(uuid = node.uuid)
    }

    /**
     * Removes a node with the given Uuid from the mesh network.
     *
     * @param uuid Uuid of the node to be removed.
     */
    fun remove(uuid: Uuid) {
        // simdo-fork (2026-06-08, P6 + Phase 3) — find + remove + cascade 전체를 한 nodesMonitor 블록으로
        // 원자화한다. 종전엔 find 가 lock 밖(raw _nodes 순회 → 동시 add/serialize 와 CME)이고 remove 만 lock
        // 안이라 find-then-remove 가 비원자였다. P6 fix 가 find+remove 를 monitor 로 묶었으나, **후속 cascade
        // (`_scenes`/`_networkExclusions` mutation)는 lock 밖**이었다. 그런데 [MeshNetworkSerializer.serialize]
        // 는 nodesMonitor 아래에서 `_nodes` 뿐 아니라 `_scenes`/`_networkExclusions`/`_groups` 등 **모든**
        // 직렬화 대상 컬렉션을 iterate 한다 → cascade 의 `_networkExclusions.add`/`_scenes…remove` 가 그
        // 직렬화 순회와 동시에 돌면 `_networkExclusions`(또는 `_scenes`)에서 CME 가 났다(Phase 3 가 봉인하는
        // nodesMonitor 단일 guardian 의 누수 윈도). cascade 도 같은 monitor 아래로 넣어 serialize 와 배타화한다
        // (전부 짧은 in-memory 변이·non-suspend, monitor 재진입이라 deadlock 없음).
        synchronized(nodesMonitor) {
            _nodes.find { it.uuid == uuid }?.also { _nodes.remove(it) }?.let { node ->
                // Remove unicast addresses of all node's elements from the scene
                _scenes.forEach { it.remove(node.addresses) }
                // When a Node is removed from the network, the unicast addresses that were used
                // cannot be assigned to another node until the IV index is incremented by 2. This
                // effectively resets the Sequence number used by all the nodes in the network.
                _networkExclusions.add(ExclusionList(ivIndex.index).apply { exclude(node) })
                // As the node is removed from the network and is no longer part of the network,
                // clear its network reference.
                node.network = null
                updateTimestamp()
            }
        }
    }

    /**
     * simdo-patch (2026-08-26) — Node Provisioning Protocol Interface (NPPI) 절차의 **종결 규칙**.
     *
     * MshPRT 1.1 §3.11.8 의 세 절차는 provisioning 프로토콜을 그대로 재사용하지만(같은 PDU 흐름,
     * 같은 보안 수준) **끝났을 때 하는 일이 다르다**: 노드를 새로 만들어 넣는 것이 아니라 이미
     * 망에 있는 노드를 *제자리에서* 갱신한다. 그래서 [remove] + [add] 조합으로는 구현할 수 없다 —
     * [remove] 는 옛 주소를 `networkExclusions` 에 넣어 같은 주소로의 재-[add] 를 스스로 막고
     * (자충수), 노드의 AppKey 바인딩 · Publication · Subscription · NetKey/AppKey 목록을 전부
     * 잃는다. NPPI 가 보존하기로 약속한 바로 그 상태다.
     *
     * ### 절차별 종결 규칙
     *
     * | 절차 | Device Key | Unicast Address | Element 수 | Composition Data | NetKey/AppKey · 바인딩 · pub/sub |
     * |---|---|---|---|---|---|
     * | 0x00 `DEVICE_KEY_REFRESH` | **교체** | 유지 | 유지 | 유지 | **보존** |
     * | 0x01 `NODE_ADDRESS_REFRESH` | **교체** | **변경** | Capabilities 기준 | 개수가 바뀌면 무효화 | **보존** |
     * | 0x02 `NODE_COMPOSITION_REFRESH` | **교체** | 유지 | Capabilities 기준 | **무효화**(재독해 필요) | **보존** |
     *
     * 근거:
     * - Bluetooth SIG, *Mesh Remote Provisioning* — Device Key Refresh: "Existing data such as the
     *   node's element addresses and its lists of NetKeys and AppKeys are unaffected." /
     *   Node Address Refresh: "Existing data such as the node's list of NetKeys and AppKeys are
     *   unaffected." / Node Composition Refresh: "The Composition Data state of the node is updated
     *   by the procedure, but other states are left unchanged."
     * - Zephyr(NCS v3.4.0) 레퍼런스 구현이 정확히 이 동작을 한다: `provisioner.c prov_node_add()` →
     *   `bt_mesh_cdb_node_key_import(node, new_dev_key)` + `bt_mesh_cdb_node_update(node, addr,
     *   elem_count)`. 삭제·재생성 없음, exclusion 없음.
     * - Element 개수의 권위는 이번 세션의 Provisioning Capabilities PDU 다. Nordic
     *   `include/zephyr/bluetooth/mesh/main.h:536-547`: "Refreshing the device key and node address.
     *   **Composition data may change, including the number of elements.**"
     *
     * ### 주소 교체(0x01) 시 옛 주소를 exclusion list 에 넣는 이유
     *
     * 노드는 주소가 바뀌는 순간 **시퀀스 번호를 0 으로 리셋**한다
     * (`main.c bt_mesh_reprovision()`: `if (addr != primary) bt_mesh.seq = 0`). 망의 다른 노드들은
     * 옛 주소에 대한 SeqAuth 를 Replay Protection List 에 그대로 들고 있으므로, 옛 주소를 곧바로
     * 다른 노드에 배정하면 그 노드의 메시지가 RPL 에서 조용히 버려진다. [remove] 가 같은 이유로
     * 같은 일을 한다 — [ExclusionList] KDoc 참조.
     *
     * ### Device Key 는 즉시 교체하지만 노드는 한동안 **두 키를 모두 받는다**
     *
     * MshPRT 1.1 §3.6.4.2 / Zephyr `app_keys.c:557-568`: 0x00 · 0x02 에서 노드는 새 Device Key 를
     * *후보* 로만 보관하고 **새 키로 복호화에 처음 성공한 순간** 옛 키를 영구히 대체한다
     * (0x01 은 절차 완료 즉시 활성화 — `provisionee.c reprovision_complete()`). 따라서 상위 stack 은
     * NPPI 직후 device-key 로 암호화되는 메시지를 한 번 보내야 한다(예:
     * `ConfigCompositionDataGet(page = 0)` — Composition Refresh 라면 어차피 필요하다).
     * CDB 의 키를 곧바로 새 키로 바꾸는 것은 Zephyr CDB 와 동일한 선택이다.
     *
     * @param node           갱신 대상 노드. 이 망에 속해 있어야 한다.
     * @param procedure      방금 완료한 NPPI 절차.
     * @param deviceKey      provisioning 으로 새로 도출된 Device Key.
     * @param unicastAddress 절차 종료 후 노드의 primary element 주소.
     * @param elementCount   이번 세션 Provisioning Capabilities 의 Number of Elements.
     * @param security       이번 세션에서 도출된 보안 수준.
     * @throws DoesNotBelongToNetwork 노드가 이 망에 없을 때.
     * @throws AddressAlreadyInUse    절차가 요구하는 주소 변경 규칙을 어겼을 때.
     */
    @Throws(DoesNotBelongToNetwork::class, AddressAlreadyInUse::class)
    fun applyNodeProvisioningProtocolInterfaceResult(
        node: Node,
        procedure: NodeProvisioningProtocolInterfaceProcedure,
        deviceKey: ByteArray,
        unicastAddress: UnicastAddress,
        elementCount: Int,
        security: Security,
    ): Unit = synchronized(nodesMonitor) {
        require(_nodes.any { it === node }) { throw DoesNotBelongToNetwork() }
        require(elementCount > 0) { "Number of Elements must be at least 1" }

        val previousAddresses = node.addresses
        val previousRange = node.unicastRange
        val addressChanged = unicastAddress != node.primaryUnicastAddress

        when (procedure) {
            // §3.11.8.4 — Device Key Refresh: 주소도 composition 도 바뀌지 않는다. 노드도 같은
            // 요구를 강제한다: `provisionee.c refresh_is_valid()` 의
            // `valid_addr = (prov_link.addr == bt_mesh_primary_addr())`.
            NodeProvisioningProtocolInterfaceProcedure.DEVICE_KEY_REFRESH -> {
                require(!addressChanged) { throw AddressAlreadyInUse() }
                require(elementCount == node.elementsCount) {
                    "Device Key Refresh must not change the number of Elements"
                }
            }

            // §3.11.8.5 — Node Address Refresh: 주소가 **반드시** 바뀐다. 노드는 새 주소가 옛
            // element 범위 밖일 것을 요구한다(`refresh_is_valid()`:
            // `addr < old_addr || addr >= old_addr + elem_count`). 우리는 한 걸음 더 나아가 범위
            // 전체의 비중첩을 요구한다 — 부분 중첩은 옛/새 주소가 동시에 유효해 보이는 구간을
            // 만들고, 그 구간을 exclusion list 에 넣으면 새 주소까지 같이 막힌다.
            NodeProvisioningProtocolInterfaceProcedure.NODE_ADDRESS_REFRESH -> {
                require(addressChanged) { throw AddressAlreadyInUse() }
                require(
                    !UnicastRange(
                        address = unicastAddress,
                        elementsCount = elementCount,
                    ).overlaps(other = previousRange)
                ) { throw AddressAlreadyInUse() }
            }

            // §3.11.8.6 — Node Composition Refresh: composition 만 바뀌고 주소는 유지된다.
            // 노드 쪽 검사도 Device Key Refresh 와 동일하다.
            NodeProvisioningProtocolInterfaceProcedure.NODE_COMPOSITION_REFRESH -> {
                require(!addressChanged) { throw AddressAlreadyInUse() }
            }
        }

        // --- 공통: Device Key 교체 (세 절차 모두) ---
        node._deviceKey = deviceKey
        node.security = security

        // --- Element 개수 반영 (0x01 / 0x02 에서만 바뀔 수 있다) ---
        val elementCountChanged = elementCount != node.elementsCount
        if (elementCountChanged) {
            node.resizeElements(count = elementCount)
        }

        // --- 주소 이동 (0x01) ---
        if (addressChanged) {
            node._primaryUnicastAddress = unicastAddress
            // 옛 범위는 IV Index 가 2 오를 때까지 재사용 금지. remove() 와 같은 근거.
            _networkExclusions.add(
                ExclusionList(ivIndex = ivIndex.index).apply {
                    network = this@MeshNetwork
                    previousAddresses.forEach { exclude(address = it) }
                }
            )
        }

        // --- Composition Data 무효화 ---
        // 0x02 는 정의상 composition 이 바뀐다. 0x01 은 "바뀔 수 있다" 이므로 Element 개수가 실제로
        // 달라졌을 때만 무효화한다(순수 재배치라면 CDB 의 Model 목록은 그대로 유효하다).
        if (procedure == NodeProvisioningProtocolInterfaceProcedure.NODE_COMPOSITION_REFRESH ||
            elementCountChanged
        ) {
            node.invalidateCompositionData()
        }

        updateTimestamp()
    }

    /**
     * Adds a given [Group] to the list of groups in the mesh network.
     *
     * @param group Group to be removed.
     * @throws [DoesNotBelongToNetwork] If the group does not belong to the network.
     * @throws [GroupAlreadyExists] If the group already exists.
     */
    @Throws(GroupAlreadyExists::class, DoesNotBelongToNetwork::class)
    fun add(group: Group) {
        require(group(address = group.address.address) == null) { throw GroupAlreadyExists() }
        require(group.network == null) { throw GroupInUse() }
        _groups.add(group.also { it.network = this }).also { updateTimestamp() }
    }

    /**
     * Returns a group for a given address or null
     *
     * @param address address
     * @return a Group for a given address or null.
     */
    fun group(address: Address) = groups.firstOrNull { it.address.address == address }

    /**
     * Removes a given [Group] from the list of groups in the mesh network.
     *
     * @param group Group to be removed.
     * @throws [DoesNotBelongToNetwork] If the group does not belong to the network.
     * @throws [GroupInUse] If the group is already in use.
     */
    @Throws(DoesNotBelongToNetwork::class, GroupInUse::class)
    fun remove(group: Group): Boolean {
        require(group.network?.uuid == uuid) { throw DoesNotBelongToNetwork() }
        require(!group.isUsed) { throw GroupInUse() }
        return remove(address = group.address)
    }

    /**
     * Removes a group with the given address from the network.
     *
     * @param address address of the group to be removed.
     */
    fun remove(address: GroupAddress) = _groups.find {
        it.address == address
    }?.let { group ->
        _groups.remove(group).also {
            updateTimestamp()
        }
    } == true

    /**
     * Removes a group with the given address from the network.
     *
     * @param address address of the group to be removed.
     */
    fun remove(address: PrimaryGroupAddress) = _groups.find {
        it.address == address
    }?.let { group ->
        _groups.remove(group).also {
            updateTimestamp()
        }
    } == true

    /**
     * Returns the Scene key with a given scene number.
     *
     * @param number Scene number of the scene.
     * @return Scene.
     * @throws NoSuchElementException if a scene for a given scene number ws not found.
     */
    fun scene(number: SceneNumber) = scenes.firstOrNull { scene ->
        scene.number == number
    }

    /**
     * Adds a given Scene with the given name and the scene number to the mesh network.
     *
     * @param name   Name of the scene.
     * @param number Scene number.
     * @throws [SceneAlreadyExists] If the scene already exists.
     */
    @Throws(SceneAlreadyExists::class)
    fun add(name: String, number: SceneNumber): Scene {
        require(scene(number = number) == null) { throw SceneAlreadyExists() }
        return Scene(_name = name, number = number).apply {
            network = this@MeshNetwork
        }.also { scene ->
            _scenes
                .apply { add(element = scene) }
                .sortBy { it.number }
            updateTimestamp()
        }
    }

    /**
     * Adds a given Scene with the given name to the mesh network for a given provisioner
     *
     * @param name        Name of the scene.
     * @param provisioner Provisioner for whom the scene is being added.
     * @throws [NoSceneNumberAvailable] If there is no scene number available for the provisioner.
     * @throws [SceneAlreadyExists] If the scene already exists.
     */
    @Throws(NoSceneNumberAvailable::class, SceneAlreadyExists::class)
    fun add(name: String, provisioner: Provisioner): Scene {
        val nextSceneNumber =
            nextAvailableScene(provisioner = provisioner) ?: throw NoSceneNumberAvailable()
        return add(name = name, number = nextSceneNumber)
    }

    /**
     * Adds a given [Scene] to the list of scenes in the mesh network.
     *
     * @param scene Scene to be added.
     * @throws [DoesNotBelongToNetwork] If the scene does not belong to the network.
     * @throws [SceneAlreadyExists] If the scene already exists.
     */
    @Throws(DoesNotBelongToNetwork::class, SceneAlreadyExists::class)
    internal fun add(scene: Scene) {
        require(scene(number = scene.number) == null) { throw SceneAlreadyExists() }
        require(scene.network?.uuid == uuid) { throw DoesNotBelongToNetwork() }
        _scenes.add(scene.also { it.network = this }).also { updateTimestamp() }
    }

    /**
     * Removes a given [Scene] from the list of groups in the mesh network.
     *
     * @param scene Scene to be removed.
     * @throws [DoesNotBelongToNetwork] If the scene does not belong to the network.
     * @throws [SceneInUse] If the scene is already in use.
     */
    @Throws(DoesNotBelongToNetwork::class, SceneInUse::class)
    fun remove(scene: Scene) {
        require(scene.network?.uuid == uuid) { throw DoesNotBelongToNetwork() }
        require(!scene.isInUse) { throw SceneInUse() }
        _scenes.remove(scene).also { updateTimestamp() }
    }

    /**
     * Removes a scene with the given scene number from the network.
     *
     * @param sceneNumber Scene number of the scene to be removed.
     * @throws [DoesNotBelongToNetwork] If the scene does not belong to the network.
     * @throws [SceneInUse] If the scene is already in use.
     */
    @Throws(DoesNotBelongToNetwork::class, SceneInUse::class)
    fun remove(sceneNumber: SceneNumber) {
        scene(number = sceneNumber)?.let { scene ->
            remove(scene = scene)
        }
    }

    /**
     * Checks if the address range is available for use.
     *
     * @param range    Unicast range to check.
     * @param ignoring Node whose own address occupancy must be ignored, or `null`.
     *                 **NPPI 전용** — Node Provisioning Protocol Interface 절차
     *                 (MshPRT 1.1 §3.11.8)는 이미 망에 있는 노드를 *제자리에서* 갱신하므로,
     *                 대상 노드 자신이 그 주소를 쓰고 있다는 사실이 "사용 중"으로 판정되면
     *                 Device Key Refresh / Composition Refresh 가 시작조차 못 한다
     *                 (두 절차는 주소를 **유지**해야 한다). 신규 장치 프로비저닝 경로는
     *                 `null` 을 그대로 쓴다 = 종전 동작 그대로.
     * @return true if the given address range is available for use or false otherwise.
     */
    // simdo-fork (2026-06-08, P6) — _nodes raw 순회를 nodesMonitor 아래로. add() 가 이 함수를 호출할 때
    // 이미 monitor 를 잡고 있어 재진입(JVM monitor reentrant)으로 안전. _networkExclusions 는 _nodes 와
    // 무관한 별 컬렉션이라 monitor 밖 평가도 무방하나, 단일 식이라 한 블록에 둔다.
    @JvmOverloads
    fun isAddressRangeAvailable(range: UnicastRange, ignoring: Node? = null) =
        synchronized(nodesMonitor) {
            _nodes.none { it !== ignoring && it.containsElementsWithAddress(range) }
            // networkExclusions 는 **일부러** self-exclusion 대상이 아니다. 망에 살아 있는 노드의
            // 주소가 exclusion list 에 들어 있다면 그건 CDB 불일치이고, 그 상태로 NPPI 를 강행하면
            // 다른 노드가 곧 그 주소를 배정받아 충돌한다. 실패하는 편이 옳다.
        } && !_networkExclusions.contains(range, ivIndex)

    /**
     * Checks if the address is available to be assigned to a node with the given number of
     * elements or false otherwise.
     *
     * @param address         Possible address of the primary element of the node.
     * @param elementCount    Element count.
     * @param ignoring        Node whose own occupancy is ignored — see
     *                        [isAddressRangeAvailable].
     * @return true if the address is available to be assigned to a node with given number of
     *         elements or false otherwise.
     */
    @JvmOverloads
    fun isAddressAvailable(address: UnicastAddress, elementCount: Int, ignoring: Node? = null) =
        isAddressRangeAvailable(
            range = UnicastRange(
                address = address, elementsCount = elementCount
            ),
            ignoring = ignoring,
        )

    /**
     * Checks if the address is available to be assigned to a node with the given number of
     * elements.
     *
     * @param address         Possible address of the primary element of the node.
     * @param node            Node
     * @return true if the address is assignable to the given `node or false otherwise.
     */
    fun isAddressAvailable(address: UnicastAddress, node: Node) =
        isAddressAvailable(address = address, elementCount = node.elementsCount)

    /**
     * Returns the next available unicast address from the provisioner's range that can be assigned
     * to a new node based on the given number of elements. The zeroth element is identified by the
     * node's Unicast Address. Each following element is  identified by a subsequent Unicast
     * Address.
     * @param offset       Unicast address offset.
     * @param elementCount Number of elements in the node.
     * @param provisioner  Provisioner that's provisioning the node.
     * @return the next available Unicast Address that can be assigned to the node or null if there
     *         are no addresses available in the allocated range.
     * @throws NoUnicastRangeAllocated if the provisioner has no address range allocated.
     */
    @Throws(NoUnicastRangeAllocated::class)
    fun nextAvailableUnicastAddress(
        offset: UnicastAddress = UnicastAddress(minUnicastAddress),
        elementCount: Int,
        provisioner: Provisioner,
    ): UnicastAddress? {
        require(provisioner._allocatedUnicastRanges.isNotEmpty()) { throw NoUnicastRangeAllocated() }

        val excludedAddresses = networkExclusions.excludedAddresses(ivIndex).map { it }

        val usedAddresses = (excludedAddresses + nodes
            .flatMap { it.elements }
            .map { it.unicastAddress })
            .sortedBy { it.address }

        provisioner._allocatedUnicastRanges.forEach { range ->
            var address = range.lowAddress

            if (range.contains(offset.address) && address < offset) address = offset

            for (index in usedAddresses.indices) {
                val usedAddress = usedAddresses[index]

                // Skip nodes with addresses below the range.
                if (address > usedAddress) continue

                if (address + elementCount - 1 < usedAddress) return address

                address = usedAddress + 1

                // If the new address is outside the range, go to the next one.
                if (address + elementCount - 1 > range.highAddress) break
            }
            // If the range has available space, return the address.
            if (address + elementCount - 1 <= range.highAddress) return address
        }
        return null
    }

    /**
     * Returns the next available Group from the Provisioner's range that can be assigned to
     * a new Group.
     *
     * @param provisioner Provisioner, whose range is to be used for address generation.
     * @return The next available group address that can be assigned to a new Scene, or null, if
     *         there are no more available numbers in the allocated range.
     * @throws [NoGroupRangeAllocated] if no scene range is allocated to the provisioner.
     */
    @Throws(NoGroupRangeAllocated::class)
    fun nextAvailableGroup(provisioner: Provisioner): GroupAddress? {
        require(provisioner._allocatedGroupRanges.isNotEmpty()) { throw NoGroupRangeAllocated() }
        val sortedGroups = _groups.sortedBy { it.address.address }

        // Iterate through all scenes just once, while iterating over ranges.
        var index = 0
        provisioner._allocatedGroupRanges.forEach { groupRange ->
            var groupAddress = groupRange.lowAddress

            // Iterate through scene objects that weren't checked yet.
            val currentIndex = index
            for (i in currentIndex until sortedGroups.size) {
                val group = sortedGroups[i]
                index += 1
                // Skip scenes with number below the range.
                if (groupAddress > group.address) continue

                // If we found a space before the current node, return the scene number.
                if (groupAddress < group.address) return groupAddress

                // Else, move the address to the next available address.
                groupAddress = (group.address as GroupAddress) + 1

                // If the new scene number is outside the range, go to the next one.
                if (groupAddress > groupRange.highAddress) break
            }

            // If the range has available space, return the address.
            if (groupAddress <= groupRange.highAddress) return groupAddress
        }
        // No group address was found :(
        return null
    }

    /**
     * Returns the next available Scene number from the Provisioner's range that can be assigned to
     * a new Scene.
     *
     * @param provisioner Provisioner, whose range is to be used for address generation.
     * @return The next available Scene number that can be assigned to a new Scene, or null, if
     *         there are no more available numbers in the allocated range.
     * @throws [NoSceneRangeAllocated] if no scene range is allocated to the provisioner.
     */
    @Throws(NoSceneRangeAllocated::class)
    fun nextAvailableScene(provisioner: Provisioner = provisioners.first()): SceneNumber? {
        require(provisioner._allocatedSceneRanges.isNotEmpty()) { throw NoSceneRangeAllocated() }
        val sortedScenes = _scenes.sortedBy { it.number }

        // Iterate through all scenes just once, while iterating over ranges.
        var index = 0
        provisioner._allocatedSceneRanges.forEach { range ->
            var scene = range.firstScene

            // Iterate through scene objects that weren't checked yet.
            val currentIndex = index
            for (i in currentIndex until sortedScenes.size) {
                val sceneObject = sortedScenes[i]
                index += 1
                // Skip scenes with number below the range.
                if (scene > sceneObject.number) continue

                // If we found a space before the current node, return the scene number.
                if (scene < sceneObject.number) return scene

                // Else, move the address to the next available address.
                scene = (sceneObject.number + 1u).toUShort()

                // If the new scene number is outside the range, go to the next one.
                if (scene > range.lastScene) break
            }

            // If the range has available space, return the address.
            if (scene <= range.lastScene) return scene
        }
        // No scene number was found :(
        return null
    }

    /**
     * Next available unicast address range for a given range size.
     *
     * @param rangeSize Size of the address range.
     * @return Next available unicast address range or null if there are no available ranges.
     */
    fun nextAvailableUnicastAddressRange(rangeSize: Int) = getNextAvailableRange(
        size = rangeSize,
        bound = UnicastRange(
            UnicastAddress(minUnicastAddress),
            UnicastAddress(maxUnicastAddress)
        ),
        ranges = provisioners.flatMap { it.allocatedUnicastRanges }.sortedBy { it.low }
    )?.let {
        it as UnicastRange
    }

    /**
     * Next available unicast address range for a given range size.
     *
     * @param rangeSize Size of the address range.
     * @return Next available group address range or null if there are no available ranges.
     */
    fun nextAvailableGroupAddressRange(rangeSize: Int) = getNextAvailableRange(
        size = rangeSize,
        bound = GroupRange(
            GroupAddress(minGroupAddress),
            GroupAddress(maxGroupAddress)
        ),
        ranges = provisioners.flatMap { it.allocatedGroupRanges }.sortedBy { it.low }
    )?.let {
        it as GroupRange
    }

    /**
     * Next available unicast address range for a given range size.
     *
     * @param rangeSize Size of the address range.
     * @return Next available group address range or null if there are no available ranges.
     */
    fun nextAvailableSceneRange(rangeSize: Int) = getNextAvailableRange(
        size = rangeSize,
        bound = SceneRange(firstScene = minSceneNumber, lastScene = maxSceneNumber),
        ranges = provisioners.flatMap { it.allocatedSceneRanges }.sortedBy { it.low }
    )

    /**
     * Returns the next available range or null otherwise.
     *
     * @param size Size of the range to be allocated.
     * @param bound Allocated range that will be bound to this provisioner.
     * @param ranges Allocated ranges.
     */
    private fun getNextAvailableRange(
        size: Int, bound: Range, ranges: List<Range>,
    ): Range? {
        var bestRange: Range? = null
        var lastUpperBound = (bound.low - 1u).toInt()

        // Go through all ranges looking for a gaps.
        for (range in ranges) {
            // If there is a space available before this range, return it.
            if (lastUpperBound + size < range.low.toInt())
                return createRange(
                    bound = bound,
                    low = (lastUpperBound + 1).toUShort(),
                    high = (lastUpperBound + size).toUShort()
                )

            // If the space exists, but it's not as big as requested, compare
            // it with the best range so far and replace if it's bigger.
            val availableSize = range.low.toInt() - lastUpperBound - 1
            if (availableSize > 0) {
                val newRange = createRange(
                    bound = bound,
                    low = (lastUpperBound + 1).toUShort(),
                    high = (lastUpperBound + availableSize).toUShort()
                )

                if (bestRange == null || newRange.diff > bestRange.diff) {
                    bestRange = newRange
                }
            }
            lastUpperBound = range.high.toInt()
        }
        // If we didn't return earlier, check after the last range and if the requested size
        // hasn't been found, return the best found.
        val availableSize = bound.high.toInt() - lastUpperBound
        val bestSize = bestRange?.diff?.toInt() ?: 0
        return if (availableSize > bestSize) {
            createRange(
                bound = bound,
                low = (lastUpperBound + 1).toUShort(),
                high = (lastUpperBound + min(size, availableSize)).toUShort()
            )
        } else bestRange // The gap of requested size hasn't been found. Return the best found.
    }

    /**
     * Creates [UnicastRange], [GroupRange] or a [SceneRange] based on the given [bound] type.
     *
     * @param bound Address range bound.
     * @param low Low address.
     * @param high High address.
     * @return a [UnicastRange], [GroupRange] or a [SceneRange].
     */
    private fun createRange(bound: Range, low: UShort, high: UShort) = when (bound) {
        is UnicastRange -> UnicastRange(UnicastAddress(low), UnicastAddress(high))
        is GroupRange -> GroupRange(GroupAddress(low), GroupAddress(high))
        is SceneRange -> SceneRange(low, high)
    }

    internal fun apply(config: NetworkConfiguration) = when (config) {
        is NetworkConfiguration.Full -> this
        is NetworkConfiguration.Partial -> {
            partial = true
            // List of Network Keys to export.
            filter(config.networkKeysConfig)
            // List of Application Keys to export.
            filter(config.applicationKeysConfig)
            // List of nodes to export.
            filter(config.nodesConfig)
            // List of provisioners to export.
            filter(config.provisionersConfig)

            // Excludes the nodes unknown to network keys.
            // TODO what will happen to the provisioner if it's node is excluded due to an
            //      unknown network key although a provisioner knows all the network keys.
            filterNodesUnknownToNetworkKeys()
            // Exclude app keys that are bound but not in the selected application key list.
            filterUnselectedApplicationKeys()
            filter(config.groupsConfig)
            // List of Scenes to export.
            filter(config.scenesConfig)
            this
        }
    }

    /**
     * Includes network keys for a partial export with the given configuration.
     *
     * @param config Network key configuration.
     */
    private fun filter(config: NetworkKeysConfig) {
        if (config is NetworkKeysConfig.Some) {
            // Filter the network keys matching the configuration.
            _networkKeys = _networkKeys.filter { key ->
                key in config.keys
            }.toMutableList()

            // Excludes nodes that does not contain selected network keys.
            _nodes = _nodes.filter { node ->
                networkKeys.map { it.index }.any { keyIndex ->
                    keyIndex !in node.netKeys.map { it.index }
                }
            }.toMutableList()
        }
    }

    /**
     * Includes application keys for a partial export with the given configuration.
     *
     * @param config Application key configuration.
     */
    private fun filter(config: ApplicationKeysConfig) {
        if (config is ApplicationKeysConfig.Some) {
            // List of application keys set in the configuration, but we must only export the
            // keys that are bound to that network key.
            _applicationKeys = _applicationKeys.filter { applicationKey ->
                applicationKey.boundNetworkKey in networkKeys
            }.toMutableList()
        }
    }

    /**
     * Filters nodes for a partial export with the given configuration.
     *
     * @param config Node configuration.
     */
    private fun filter(config: NodesConfig) {
        when (config) {
            is NodesConfig.All -> if (config.deviceKeyConfig == DeviceKeyConfig.EXCLUDE_KEY) {
                _nodes = _nodes.map { node ->
                    node.copy(_deviceKey = null)
                }.toMutableList()
            } else _nodes

            is NodesConfig.Some -> {
                val withDeviceKey = _nodes.filter { node ->
                    node in config.withDeviceKey
                }.toMutableList()

                val withoutDeviceKey = _nodes.filter { node ->
                    node in config.withoutDeviceKey
                }.map { node ->
                    node.copy(_deviceKey = null)
                }.toMutableList()

                _nodes.clear()
                _nodes = (withDeviceKey + withoutDeviceKey).toMutableList()

                // Add any missing provisioner nodes if they were not selected when selecting
                // nodes.
                // TODO should the device key be included for such a node?
                provisioners.forEach { provisioner ->
                    if (!has(provisioner)) {
                        provisioner.node?.let { add(it) }
                    }
                }
                nodes
            }
        }
    }

    /**
     * Filters provisioners for a partial export with the given configuration.
     *
     * @param config Provisioners configuration.
     */
    private fun filter(config: ProvisionersConfig) {
        if (config is ProvisionersConfig.Some || config is ProvisionersConfig.One) {
            // First Let's exclude provisioners that are not selected.
            _provisioners = _provisioners.filter { provisioner ->
                node(provisioner = provisioner) != null
            }.toMutableList()
        }
        // The above process will exclude provisioners that does not have an address as they
        // will not have corresponding nodes. The following step would re-add the selected
        // provisioners that might have been excluded.
        _provisioners = _provisioners.filter { provisioner ->
            when (config) {
                is ProvisionersConfig.Some -> provisioner in config.provisioners
                is ProvisionersConfig.One -> provisioner == config.provisioner
                is ProvisionersConfig.All -> true
            }
        }.toMutableList()
    }

    /**
     * Filters duplicate provisioners.
     *
     * @param provisioners List of provisioners to filter
     */
    private fun filterDuplicateProvisioners(provisioners: List<Provisioner>) =
        provisioners.filter { provisioner -> !has(provisioner) }

    /**
     * Includes groups for a partial export with the given configuration.
     *
     * @param config Groups configuration.
     */
    private fun filter(config: GroupsConfig) {
        if (config is GroupsConfig.Related) {
            _groups = _groups.filter { group ->
                group.isUsed
            }.toMutableList()
        } else if (config is GroupsConfig.Some) {
            _groups.forEach { group ->
                _nodes.filter { node ->
                    node !in group.nodes()
                }.forEach { node ->
                    node.elements.forEach { element ->
                        element.models.forEach { model ->
                            if (model.publish?.address is GroupAddress) {
                                model._publish = null
                            }
                            model._subscribe =
                                model.subscribe.filterIsInstance<GroupAddress>().toMutableList()
                        }
                    }
                }
            }
        }
    }

    /**
     * Includes scenes for a partial export with the given configuration.
     *
     * @param config Scenes configuration.
     */
    private fun filter(config: ScenesConfig) {
        if (config is ScenesConfig.Some) {
            _scenes = _scenes.filter { scene ->
                scene in config.scenes
            }.toMutableList()
        }
        // Let's exclude unselected nodes from the list of addresses in scenes.
        _scenes.forEach { scene ->
            scene.addresses.filter { address ->
                address !in _nodes.map { node ->
                    node.primaryUnicastAddress
                }
            }
        }
    }

    /**
     * Excludes nodes that does not contain selected network keys.
     */
    private fun filterNodesUnknownToNetworkKeys() {
        _nodes = _nodes.filter { node ->
            networkKeys.map { it.index }.any { keyIndex ->
                keyIndex in node.netKeys.map { it.index }
            }
        }.toMutableList()
    }

    /**
     * Excludes unselected application keys from models for a partial export.
     */
    private fun filterUnselectedApplicationKeys() {
        _nodes.forEach { node ->
            node.elements.forEach { element ->
                element.models.forEach { model ->
                    model.bind.filter { keyIndex ->
                        keyIndex !in _applicationKeys.map { key ->
                            key.index
                        }
                    }.forEach { keyIndex ->
                        if (model.publish?.index == keyIndex) {
                            model._publish = null
                        }
                    }
                }
            }
        }
    }

    /**
     * This method may be used to match the Node Identity or Private Node Identity beacons.
     *
     * @param nodeIdentity Node identity.
     * @return Node matching the given node identity or null otherwise.
     */
    fun matches(nodeIdentity: NodeIdentity) = node(nodeIdentity) != null

    /**
     * Checks if the given Network Identity beacon matches with any of the network keys in the
     * network.
     *
     * @param networkId Network ID.
     * @return true if matches or false otherwise.
     */
    fun matches(networkId: NetworkIdentity) = networkKeys.any {
        networkId.matches(it)
    }

    /**
     * Checks if the given Network ID matches with any of the network keys in the network.
     *
     * @param networkId Network ID.
     * @return true if matches or false otherwise.
     */
    fun matches(networkId: ByteArray) = networkKeys.any {
        it.networkId.contentEquals(networkId) || it.oldNetworkId.contentEquals(networkId)
    }

    /**
     * Returns an element with the given address.
     *
     * @param elementAddress Address of the element.
     * @return Element if found or null otherwise.
     */
    fun element(elementAddress: Address) = nodes.flatMap { it.elements }
        .firstOrNull { it.unicastAddress.address == elementAddress }

    /**
     * Checks if the given provisioner is a part of the network.
     *
     * @param provisioner Provisioner to check.
     * @return true if the provisioner is a part of the network or false otherwise.
     */
    fun contains(provisioner: Provisioner) = provisioner(uuid = provisioner.uuid) != null

    /**
     * Restores the local provisioner for a given mesh network.
     *
     * @param storage Secure properties storage where the local provisioner is saved.
     * @return returns true if the local property
     */
    suspend fun restoreLocalProvisioner(storage: SecurePropertiesStorage) = provisioners
        .firstOrNull {
            it.uuid.toString() == storage.localProvisioner(uuid = uuid)
        }?.let {
            move(provisioner = it, to = 0)
            true
        } ?: false

    companion object {
        /**
         *  Invoked when an observable property is changed.
         *
         *  @param oldValue Old value of the property.
         *  @param newValue New value to be assigned.
         *  @param action Lambda to be invoked if the [newValue] is not the same as [oldValue].
         */
        fun <T> onChange(oldValue: T, newValue: T, action: () -> Unit) {
            if (newValue != oldValue) action()
        }
    }
}


