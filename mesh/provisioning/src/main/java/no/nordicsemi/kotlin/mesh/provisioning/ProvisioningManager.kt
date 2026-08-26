@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import no.nordicsemi.kotlin.data.toByteArray
import no.nordicsemi.kotlin.mesh.bearer.BearerError
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.provisioning.ProvisioningBearer
import no.nordicsemi.kotlin.mesh.core.exception.NoLocalProvisioner
import no.nordicsemi.kotlin.mesh.core.exception.NoUnicastRangeAllocated
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.model.UnicastRange
import no.nordicsemi.kotlin.mesh.crypto.Algorithms
import no.nordicsemi.kotlin.mesh.logger.LogCategory
import no.nordicsemi.kotlin.mesh.logger.Logger
import no.nordicsemi.kotlin.mesh.provisioning.bearer.PBRemoteBearer
import no.nordicsemi.kotlin.mesh.provisioning.bearer.send
import kotlin.uuid.ExperimentalUuidApi

/**
 * Provisioning manager is responsible for provisioning new devices to a mesh network.
 *
 * It also drives the three **Node Provisioning Protocol Interface (NPPI)** procedures of
 * MshPRT 1.1 §3.11.8, which re-run the very same provisioning protocol against a node that is
 * *already* in the network — see the secondary constructor and
 * [nodeProvisioningProtocolInterfaceProcedure].
 *
 * @property unprovisionedDevice          Unprovisioned device to be provisioned.
 * @property meshNetwork                  Mesh network to which the device will be provisioned.
 * @property bearer                       Bearer used to send provisioning PDUs.
 * @property configuration                Provisioning configuration used to provision the device.
 * @property suggestedUnicastAddress      Suggested unicast address to be assigned to the device.
 * @property logger                       Logger for the provisioning manager.
 */
class ProvisioningManager(
    private val unprovisionedDevice: UnprovisionedDevice,
    private val meshNetwork: MeshNetwork,
    val bearer: ProvisioningBearer,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO
) {
    private val scope = CoroutineScope(SupervisorJob() + ioDispatcher)
    lateinit var configuration: ProvisioningParameters
    var logger: Logger? = null

    var suggestedUnicastAddress: UnicastAddress? = null
        private set

    /**
     * The NPPI procedure this manager is running, or `null` when it is provisioning a brand new
     * device (the classic path).
     */
    var nodeProvisioningProtocolInterfaceProcedure: NodeProvisioningProtocolInterfaceProcedure? =
        null
        private set

    /** Node being refreshed in place. Non-null exactly when the manager is in NPPI mode. */
    private var nodeProvisioningProtocolInterfaceTarget: Node? = null

    init {
        // Ensures that the mesh network has at least one provisioner added and a unicast address
        // range is allocated.
        meshNetwork.localProvisioner?.let {
            require(it.allocatedUnicastRanges.isNotEmpty()) {
                logger?.e(LogCategory.PROVISIONING) { "No unicast ranges allocated" }
                throw NoUnicastRangeAllocated()
            }
        } ?: run {
            logger?.e(LogCategory.PROVISIONING) { "No local provisioner" }
            throw NoLocalProvisioner()
        }

        // Ensures the provided bearer supports provisioning PDUs.
        require(bearer.supports(PduType.PROVISIONING_PDU)) {
            logger?.e(LogCategory.PROVISIONING) {
                "Bearer does not support provisioning pdu"
            }
            throw BearerError.PduTypeNotSupported()
        }
    }

    /**
     * simdo-patch (2026-08-26) — Creates a manager that runs one of the three **Node Provisioning
     * Protocol Interface** procedures (MshPRT 1.1 §3.11.8) against a node that is *already*
     * provisioned into [meshNetwork].
     *
     * All three procedures speak exactly the same provisioning protocol as a first-time
     * provisioning — Invite → Capabilities → Start → Public Keys → Confirmation → Random →
     * Data → Complete, with the same security level. What differs is
     *
     * - the **content constraints** on the Provisioning Data PDU (same IV Index, a NetKey the node
     *   already knows, and an address that follows the per-procedure rule), and
     * - the **termination**: the node is updated in place, not recreated.
     *   See [MeshNetwork.applyNodeProvisioningProtocolInterfaceResult] for the full rule table.
     *
     * The link must be opened by a [no.nordicsemi.kotlin.mesh.provisioning.bearer.PBRemoteBearer]
     * constructed with the *same* `nppiProcedure` — the procedure travels on the wire in the
     * Remote Provisioning Link Open message, and the target node decides how to behave from that
     * single octet. A mismatch here would make the provisioner and the node disagree about what
     * just happened, so it is rejected up front.
     *
     * ```
     * val bearer = PBRemoteBearer(
     *     manager = meshNetworkManager,
     *     server = node.primaryUnicastAddress.address,   // the node refreshes *itself*
     *     nppiProcedure = NodeProvisioningProtocolInterfaceProcedure.DEVICE_KEY_REFRESH,
     * )
     * try {
     *     bearer.open()
     *     ProvisioningManager(node, procedure, network, bearer)
     *         .provision(attentionTimer = 0u)
     *         .collect { … }
     * } finally {
     *     bearer.close()
     * }
     * // 0x00 / 0x02 는 노드가 새 Device Key 를 후보로만 들고 있다. 곧바로 device-key 메시지를
     * // 한 번 보내 활성화시킬 것 (MshPRT 1.1 §3.6.4.2).
     * ```
     *
     * @param node          Node to refresh. Must belong to [meshNetwork] and must not be the local
     *                      Provisioner (its Device Key is generated locally, not over the air).
     * @param procedure     NPPI procedure to run.
     * @param meshNetwork   Mesh network the node belongs to.
     * @param bearer        Bearer with the Remote Provisioning link already targeted at [node].
     * @param ioDispatcher  Dispatcher for internal coroutines.
     */
    @OptIn(ExperimentalUuidApi::class)
    constructor(
        node: Node,
        procedure: NodeProvisioningProtocolInterfaceProcedure,
        meshNetwork: MeshNetwork,
        bearer: ProvisioningBearer,
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    ) : this(
        unprovisionedDevice = UnprovisionedDevice(name = node.name, uuid = node.uuid),
        meshNetwork = meshNetwork,
        bearer = bearer,
        ioDispatcher = ioDispatcher,
    ) {
        require(meshNetwork.nodes.any { it === node }) {
            throw InvalidNodeProvisioningProtocolInterfaceState(
                reason = "Node ${node.uuid} does not belong to this mesh network"
            )
        }
        require(!node.isLocalProvisioner) {
            throw InvalidNodeProvisioningProtocolInterfaceState(
                reason = "NPPI cannot be run against the local Provisioner's own Node"
            )
        }
        // 베어러가 PB-Remote 라면 wire 에 실려 나간 절차와 반드시 같아야 한다. 다른 종류의
        // 베어러(테스트용 fake 등)는 검사하지 않는다 — 절차를 실어 보낼 방법 자체가 없으므로.
        (bearer as? PBRemoteBearer)?.let { remote ->
            require(remote.nppiProcedure == procedure) {
                throw InvalidNodeProvisioningProtocolInterfaceState(
                    reason = "Bearer was opened for ${remote.nppiProcedure}, " +
                            "but the manager was asked to run $procedure"
                )
            }
        }
        nodeProvisioningProtocolInterfaceProcedure = procedure
        nodeProvisioningProtocolInterfaceTarget = node
    }

    /**
     * Starts the provisioning process with the given attention timer.
     *
     * @param attentionTimer Attention timer value in seconds.
     * @return Flow of provisioning states that could be used to observe and continue/cancel the
     *         provisioning process.
     * @throws UnsupportedDevice If the device does not support the required algorithms.
     * @throws NoAddressAvailable If the device does not have any unicast address available.
     * @throws InvalidAddress If the device has an invalid unicast address.
     * @throws InvalidPdu If the device has sent an invalid PDU.
     * @throws InvalidPublicKey If the device has sent an invalid public key.
     * @throws InvalidOobValueFormat If the device has sent an invalid OOB value.
     * @throws RemoteError If the device has sent an error.
     * @throws ProvisioningError If the provisioning process failed.
     * @throws BearerError.Closed If the bearer is closed.
     */
    @OptIn(ExperimentalUuidApi::class)
    @Throws(
        UnsupportedDevice::class,
        NoAddressAvailable::class,
        InvalidAddress::class,
        InvalidPdu::class,
        InvalidPublicKey::class,
        InvalidOobValueFormat::class,
        RemoteError::class,
        ProvisioningError::class,
        BearerError.Closed::class
    )
    fun provision(attentionTimer: UByte) = flow {
        try {
            // Is there bearer open?
            require(bearer.isOpen) {
                logger?.e(LogCategory.PROVISIONING) { "Bearer closed" }
                throw BearerError.Closed()
            }
            // Emit the current state.
            emit(value = ProvisioningState.RequestingCapabilities)

            // Initialize Provisioning data.
            val provisioningData = ProvisioningData()

            // Sends the provisioning invite and awaits for the capabilities.
            val capabilities = awaitCapabilities(
                invite = ProvisioningRequest.Invite(attentionTimer),
                provisioningData = provisioningData
            )
            configuration = nodeProvisioningProtocolInterfaceTarget?.let { target ->
                ProvisioningParameters.defaultForNodeProvisioningProtocolInterface(
                    capabilities = capabilities,
                    meshNetwork = meshNetwork,
                    node = target,
                    procedure = nodeProvisioningProtocolInterfaceProcedure!!,
                )
            } ?: ProvisioningParameters.defaultFrom(
                capabilities = capabilities,
                meshNetwork = meshNetwork,
            )
            suggestedUnicastAddress = configuration.unicastAddress

            // We use a mutex here to wait for the user to either start or cancel the provisioning.
            val mutex = Mutex(locked = true)
            // Emit to the user that the capabilities have been received.
            emit(
                value = ProvisioningState.CapabilitiesReceived(
                    capabilities = capabilities,
                    defaultParameters = configuration,
                    start = { configuration ->
                        this@ProvisioningManager.configuration = configuration
                        mutex.unlock()
                    },
                    cancel = { mutex.unlock() },
                )
            )
            mutex.lock()

            // Checks if the device supports the required algorithms
            require(capabilities.algorithms.any { Algorithms.algorithms.contains(element = it) }) {
                throw UnsupportedDevice()
            }

            // simdo-patch (2026-08-26) — NPPI 는 Provisioning Data 의 NetKey 가 노드가 **이미
            // 아는** subnet 의 송신 키여야 한다. 사용자가 CapabilitiesReceived.start(...) 로
            // 파라미터를 갈아끼울 수 있으므로 여기서(=최종 확정 후) 검사한다.
            // 근거: Zephyr `provisionee.c refresh_is_valid()` —
            // `if (!sub || bt_mesh_key_compare(netkey, &sub->keys[TX].net)) return false;`
            nodeProvisioningProtocolInterfaceTarget?.let { target ->
                require(target.networkKeys.any { it.index == configuration.networkKey.index }) {
                    logger?.e(LogCategory.PROVISIONING) {
                        "NPPI: Network Key ${configuration.networkKey.index} is unknown to the node"
                    }
                    throw InvalidNodeProvisioningProtocolInterfaceState(
                        reason = "Network Key index ${configuration.networkKey.index} is not " +
                                "known to node ${target.primaryUnicastAddress}"
                    )
                }
                // Device Key Refresh 는 composition 을 바꾸지 않는다(§3.11.8.4). Element 개수가
                // 달라졌다면 그건 Address Refresh(§3.11.8.5) 나 Composition Refresh(§3.11.8.6)
                // 로 처리해야 할 상황이다.
                //
                // ★ 이 검사를 **Provisioning Data 를 보내기 전**에 하는 것이 중요하다. 종결
                //   시점에 실패하면 노드는 이미 새 Device Key 를 받아 두었는데 CDB 는 옛 키를
                //   들고 있게 되어 관리자가 노드를 놓칠 수 있다. 여기서 중단하면 노드 상태는
                //   전혀 바뀌지 않은 채 링크만 닫힌다.
                require(
                    nodeProvisioningProtocolInterfaceProcedure !=
                            NodeProvisioningProtocolInterfaceProcedure.DEVICE_KEY_REFRESH ||
                            capabilities.numberOfElements == target.elementsCount
                ) {
                    logger?.e(LogCategory.PROVISIONING) {
                        "NPPI: element count changed (${target.elementsCount} → " +
                                "${capabilities.numberOfElements}) — use Address or Composition " +
                                "Refresh instead"
                    }
                    throw InvalidNodeProvisioningProtocolInterfaceState(
                        reason = "Device Key Refresh cannot change the number of Elements " +
                                "(${target.elementsCount} → ${capabilities.numberOfElements})"
                    )
                }
            }

            // Is the Unicast address valid?
            require(
                isUnicastAddressValid(
                    unicastAddress = configuration.unicastAddress,
                    numberOfElements = capabilities.numberOfElements,
                )
            ) {
                logger?.e(LogCategory.PROVISIONING) { "Unicast address is not valid" }
                throw InvalidAddress()
            }

            // Try generating Private and Public keys. This may fail if the given algorithm is not
            // supported.
            provisioningData.generateKeys(algorithm = configuration.algorithm)

            emit(value = ProvisioningState.Provisioning)
            provisioningData.prepare(
                networkKey = configuration.networkKey,
                ivIndex = meshNetwork.ivIndex,
                unicastAddress = configuration.unicastAddress,
            )

            ProvisioningRequest.Start(configuration = configuration).also { start ->
                logger?.v(LogCategory.PROVISIONING) { "Sending $start" }
                send(request = start).also { provisioningData.accumulate(data = it) }
            }

            // If the device's Public Key was obtained OOB, we are now ready to calculate the device's
            // Shared Secret, if not we need send the provisioner public key and wait for the device's
            // Public Key.
            val key = when (configuration.publicKey) {
                is PublicKey.OobPublicKey -> (configuration.publicKey as PublicKey.OobPublicKey).key
                else -> {
                    awaitProvisioneePublicKey(
                        request = ProvisioningRequest.PublicKey(provisioningData.provisionerPublicKey),
                        provisioningData = provisioningData
                    )
                }
            }
            provisioningData.apply {
                onDevicePublicKeyReceived(
                    key = key,
                    usingOob = configuration.publicKey is PublicKey.OobPublicKey
                )
                accumulate(data = key)
            }

            requestAuthentication(
                method = configuration.authMethod,
                sizeInBytes = provisioningData.algorithm.length shr 3,
                onAuthValueReceived = provisioningData::onAuthValueReceived,
                mutex = mutex
            )?.also { action ->
                emit(value = ProvisioningState.AuthActionRequired(action = action))
                when (action) {
                    is AuthAction.DisplayNumber, is AuthAction.DisplayAlphaNumeric -> {
                        mutex.unlock()
                        awaitInputComplete()
                        emit(value = ProvisioningState.InputComplete)
                    }

                    is AuthAction.ProvideStaticKey,
                    is AuthAction.ProvideNumeric,
                    is AuthAction.ProvideAlphaNumeric,
                        -> mutex.lock()
                }
            }

            val confirmation = awaitConfirmation(
                request = ProvisioningRequest.Confirmation(
                    confirmation = provisioningData.provisionerConfirmation
                )
            )
            provisioningData.onDeviceConfirmationReceived(confirmation = confirmation)

            val random = awaitRandom(
                request = ProvisioningRequest.Random(
                    random = provisioningData.provisionerRandom
                )
            )
            provisioningData.onDeviceRandomReceived(random = random)

            require(provisioningData.checkIfConfirmationsMatch()) {
                logger?.e(LogCategory.PROVISIONING) { "Confirmations do not match" }
                emit(value = ProvisioningState.Failed(error = ConfirmationFailed()))
                throw ConfirmationFailed()
            }

            val data = ProvisioningRequest.Data(
                encryptedDataWithMic = provisioningData.encryptedProvisioningDataWithMic
            )
            logger?.v(LogCategory.PROVISIONING) { "Sending $data" }
            send(request = data)

            awaitComplete().also {
                emit(value = ProvisioningState.Complete)

                // simdo-patch (2026-08-26) — NPPI 종결. 노드를 새로 만들지 않고 제자리에서
                // 갱신한다. 절차별 "무엇이 바뀌고 무엇이 보존되는가" 표와 spec 인용은
                // MeshNetwork.applyNodeProvisioningProtocolInterfaceResult KDoc 에 있다.
                //
                // 아래 신규 장치 경로처럼 cdbMutex 로 감싸는 이유도 같다: 병렬 provisioning /
                // config-status RX / CDB 직렬화가 같은 컬렉션을 건드린다.
                val nppiTarget = nodeProvisioningProtocolInterfaceTarget
                if (nppiTarget != null) {
                    val procedure = nodeProvisioningProtocolInterfaceProcedure!!
                    logger?.i(LogCategory.PROVISIONING) {
                        "NPPI $procedure complete for ${nppiTarget.primaryUnicastAddress} " +
                                "→ ${configuration.unicastAddress} " +
                                "(${capabilities.numberOfElements} elements)"
                    }
                    meshNetwork.withCdbLock {
                        meshNetwork.applyNodeProvisioningProtocolInterfaceResult(
                            node = nppiTarget,
                            procedure = procedure,
                            deviceKey = provisioningData.deviceKey,
                            unicastAddress = configuration.unicastAddress,
                            elementCount = capabilities.numberOfElements,
                            security = provisioningData.security,
                        )
                    }
                    return@also
                }

                // If the node was reprovisioned we need to remove it from the network and add it
                // again.
                // In version 0.9.1 and before the behavior of reprovisioning node was as follows
                // - If the same node was reprovisioned, the library threw an error after
                //   provisioning was complete without adding it to the network
                // However, post 0.9.1 the library was explicitly remove any existing nodes upon
                // reprovisioning and adds the new node to the network.
                val node = Node(
                    name = unprovisionedDevice.name,
                    uuid = unprovisionedDevice.uuid,
                    deviceKey = provisioningData.deviceKey,
                    unicastAddress = configuration.unicastAddress,
                    elementCount = capabilities.numberOfElements,
                    assignedNetworkKey = configuration.networkKey,
                    security = provisioningData.security
                )
                // simdo-fork (2026-06-06) — Fork-3 확장(P4: 병렬 provisioning CDB mutation race).
                // 병렬 provision 시 N 개 ProvisioningManager 가 동시에 같은 MeshNetwork 의 _nodes(ArrayList)에
                // remove/add 하면 CME + lost-update. 또한 config-status RX(AccessLayer)·export(serialize)
                // 트래버설과도 같은 collection 을 건드린다. MeshNetwork.cdbMutex(= config-RX 가 위임하는
                // 그 lock, P4 hoist)로 remove+add 를 직렬화한다. send/PDU 송신은 이 lock 밖이라(여기는
                // 순수 in-memory CDB write) throughput 영향 없음. Mutex 비재진입 — 이 블록 내부에서 다른
                // withCdbLock 재진입 없음(remove/add 는 MeshNetwork 자체 메서드, lock 미인지).
                meshNetwork.withCdbLock {
                    meshNetwork.remove(uuid = node.uuid)
                    meshNetwork.add(node = node)
                }
            }

        } catch (error: RemoteError) {
            emit(ProvisioningState.Failed(error))
        }
    }

    /**
     * Waits for the capabilities response from the device.
     *
     * @param invite           Provisioning invite to send.
     * @param provisioningData The provisioning data to accumulate the received PDUs.
     * @throws InvalidPdu If the received PDU is invalid.
     * @throws NoAddressAvailable If the device does not have any unicast addresses available.
     */
    @Throws(InvalidPdu::class, NoAddressAvailable::class)
    private suspend fun awaitCapabilities(
        invite: ProvisioningRequest.Invite,
        provisioningData: ProvisioningData,
    ): ProvisioningCapabilities {
        logger?.v(LogCategory.PROVISIONING) { "Sending $invite" }
        send(request = invite)
            .also { provisioningData.accumulate(data = it) }
        val response = ProvisioningResponse.from(pdu = awaitBearerPdu().data)
            .apply {
                logger?.v(LogCategory.PROVISIONING) { "Received $this" }
                require(this is ProvisioningResponse.Capabilities) {
                    logger?.e(LogCategory.PROVISIONING) {
                        "Provisioning failed with error: ${InvalidPdu()}"
                    }
                    throw InvalidPdu()
                }
                provisioningData.accumulate(data = pdu.sliceArray(indices = 1 until pdu.size))
            } as ProvisioningResponse.Capabilities
        return response.capabilities
    }

    /**
     * Waits for the device's Public Key.
     *
     * @param request                Provisioner's Public Key.
     * @param provisioningData   Provisioning data to accumulate the received PDUs.
     * @throws RemoteError If the device returns an error.
     * @throws InvalidPdu If the received PDU is invalid.
     * @throws InvalidPublicKey If the device's Public Key is invalid.
     */
    @Throws(InvalidPdu::class, RemoteError::class, InvalidPublicKey::class)
    private suspend fun awaitProvisioneePublicKey(
        request: ProvisioningRequest.PublicKey,
        provisioningData: ProvisioningData,
    ): ByteArray {
        logger?.v(LogCategory.PROVISIONING) { "Sending $request" }
        send(request).also { provisioningData.accumulate(data = it) }
        val response = ProvisioningResponse.from(pdu = awaitBearerPdu().data).also { response ->
            logger?.v(LogCategory.PROVISIONING) { "Received $response" }
            if (response is ProvisioningResponse.Failed) {
                logger?.e(LogCategory.PROVISIONING) {
                    "Provisioning failed with error: ${response.error}"
                }
                throw RemoteError(response.error)
            }
            require(response is ProvisioningResponse.PublicKey) {
                throw InvalidPdu()
            }
            // Errata E1650 added an extra validation step to ensure the received public key is
            // the same as the provisioner's public key.
            require(!response.key.contentEquals(request.publicKey)) {
                throw InvalidPublicKey()
            }
        } as ProvisioningResponse.PublicKey
        return response.key
    }

    /**
     * Waits for the user to provide the authentication value.
     *
     * @param method                  Authentication method.
     * @param sizeInBytes             Size of auth value in bytes based on the algorithm
     * @param onAuthValueReceived     Lambda to be invoked upon receiving/generating auth value
     * @param mutex                   Mutex to unlock when the user has provided the authentication
     *                                value.
     * @throws InvalidOobValueFormat If the user provided an invalid
     */
    @Throws(InvalidOobValueFormat::class)
    private fun requestAuthentication(
        method: AuthenticationMethod,
        sizeInBytes: Int,
        onAuthValueReceived: (authValue: ByteArray) -> Unit,
        mutex: Mutex,
    ): AuthAction? = when (method) {
        AuthenticationMethod.NoOob -> {
            onAuthValueReceived(ByteArray(sizeInBytes) { 0x00 })
            null
        }

        AuthenticationMethod.StaticOob -> AuthAction.ProvideStaticKey(length = sizeInBytes) {
            require(it.size == sizeInBytes) {
                throw InvalidOobValueFormat()
            }
            onAuthValueReceived(it)
            mutex.unlock()
        }

        is AuthenticationMethod.OutputOob -> when (method.action) {
            OutputAction.OUTPUT_ALPHANUMERIC ->
                AuthAction.ProvideAlphaNumeric(method.length) {
                    val input = it.toByteArray(charset = Charsets.US_ASCII)
                    val authValue = input + ByteArray(size = sizeInBytes - input.size)
                    onAuthValueReceived(
                        authValue.sliceArray(0 until sizeInBytes)
                    )
                    mutex.unlock()
                }
            // BLINK,BEEP,VIBRATE,OUTPUT_NUMERIC
            else -> AuthAction.ProvideNumeric(method.length, method.action) {
                val input = it.toByteArray()
                val authValue = ByteArray(size = sizeInBytes - input.size) + input
                onAuthValueReceived(authValue)
                mutex.unlock()
            }
        }

        is AuthenticationMethod.InputOob -> when (method.action) {
            InputAction.INPUT_ALPHANUMERIC -> {
                AuthAction.DisplayAlphaNumeric(
                    text = AuthenticationMethod.randomAlphaNumeric(
                        length = method.length.toInt()
                    )
                ).also {
                    val input = it.text.toByteArray(charset = Charsets.US_ASCII)
                    val authValue = input + ByteArray(size = sizeInBytes - input.size)
                    onAuthValueReceived(authValue)
                }
            }
            // PUSH, TWIST, INPUT_NUMERIC
            else -> AuthAction.DisplayNumber(
                number = AuthenticationMethod.randomInt(
                    length = method.length.toInt()
                ).toUInt(),
                action = method.action
            ).also {
                val input = it.number.toByteArray()
                val authValue = ByteArray(size = sizeInBytes - input.size) + input
                onAuthValueReceived(authValue)
            }
        }
    }

    /**
     * Waits for the input complete response from the device.
     *
     * @throws RemoteError If the device returns an error.
     */
    @Throws(RemoteError::class)
    private suspend fun awaitInputComplete(): ProvisioningResponse.InputComplete {
        val response = ProvisioningResponse.from(
            pdu = awaitBearerPdu().data
        ).apply {
            logger?.v(LogCategory.PROVISIONING) { "Received $this" }
            if (this is ProvisioningResponse.Failed) {
                logger?.e(LogCategory.PROVISIONING) {
                    "Provisioning failed with error: $error"
                }
                throw RemoteError(error)
            }
        } as ProvisioningResponse.InputComplete
        return response
    }

    /**
     * Waits for the confirmation response from the device.
     *
     * @param request  Confirmation value to send to the device.
     * @throws RemoteError If the device returns an error.
     */
    @Throws(RemoteError::class)
    private suspend fun awaitConfirmation(
        request: ProvisioningRequest.Confirmation,
    ): ByteArray {
        logger?.v(LogCategory.PROVISIONING) { "Sending $request" }
        send(request)
        val response = ProvisioningResponse.from(
            pdu = awaitBearerPdu().data
        ).also {
            logger?.v(LogCategory.PROVISIONING) { "Received $it" }
            if (it is ProvisioningResponse.Failed) {
                logger?.e(LogCategory.PROVISIONING) {
                    "Provisioning failed with error: $it.error"
                }
                throw RemoteError(it.error)
            }
        } as ProvisioningResponse.Confirmation
        // Errata E1650 added an extra validation step to ensure the received public key is
        // the same as the provisioner's public key.
        require(!response.confirmation.contentEquals(request.confirmation)) {
            throw InvalidConfirmation()
        }
        return response.confirmation
    }

    /**
     * Waits for the provisioning random response from the device.
     *
     * @param request  Random value to send to the device.
     * @throws RemoteError If the device returns an error.
     */
    @Throws(RemoteError::class)
    private suspend fun awaitRandom(request: ProvisioningRequest.Random): ByteArray {
        logger?.v(LogCategory.PROVISIONING) { "Sending $request" }
        send(request)
        return (ProvisioningResponse.from(
            pdu = awaitBearerPdu().data
        ).apply {
            logger?.v(LogCategory.PROVISIONING) { "Received $this" }
            if (this is ProvisioningResponse.Failed) {
                logger?.e(LogCategory.PROVISIONING) {
                    "Provisioning failed with error: $error"
                }
                throw RemoteError(error)
            }
        } as ProvisioningResponse.Random).random
    }

    /**
     * Waits for the provisioning complete response from the device.
     *
     * @throws RemoteError If the device returns an error.
     */
    @Throws(RemoteError::class)
    private suspend fun awaitComplete() = ProvisioningResponse.from(
        pdu = awaitBearerPdu().data
    ).apply {
        logger?.v(LogCategory.PROVISIONING) { "Received $this" }
        if (this is ProvisioningResponse.Failed) {
            logger?.e(LogCategory.PROVISIONING) {
                "Provisioning failed with error: $error"
            }
            throw RemoteError(error)
        }
    } as ProvisioningResponse.Complete

    private fun isPublicKeyValid(provisionerPublicKey: ByteArray, devicePublicKey: ByteArray) {
        require(!provisionerPublicKey.contentEquals(devicePublicKey)) {
            throw InvalidPublicKey()
        }
    }

    /**
     * Checks if the unicast address valid.
     *
     * simdo-patch (2026-08-26) — NPPI 인지. 절차별 규칙은
     * [MeshNetwork.applyNodeProvisioningProtocolInterfaceResult] 의 표와 같다.
     *
     * | 모드 | 요구 조건 |
     * |---|---|
     * | 신규 장치 | 범위가 비어 있고 **local Provisioner 의 할당 범위 안** |
     * | NPPI 0x00 / 0x02 | 주소가 노드의 현재 주소와 **동일**, 범위가 비어 있음(**자기 자신 제외**) |
     * | NPPI 0x01 | 주소가 **바뀌고** 옛 범위와 겹치지 않으며, 범위가 비어 있고(자기 자신 제외) local Provisioner 의 할당 범위 안 |
     *
     * 자기 자신을 제외하지 않으면 0x00/0x02 는 **시작조차 못 한다** — 대상 노드가 바로 그
     * 주소를 쓰고 있기 때문이다. 이것이 NPPI 를 막고 있던 1번 blocker 였다.
     *
     * 0x00/0x02 에서 "local Provisioner 의 할당 범위 안" 을 **요구하지 않는** 이유: 노드는
     * 자기 주소를 그대로 유지하며, 그 주소를 배정한 것은 (multi-provisioner 망에서는) 다른
     * Provisioner 일 수 있다. 우리 범위를 요구하면 남이 프로비저닝한 노드의 Device Key 를
     * 영영 갱신할 수 없게 되는데, 프로토콜은 그런 제약을 두지 않는다.
     *
     * @param unicastAddress     Unicast address to be checked.
     * @param numberOfElements   Number of elements in the node.
     * @return true if the address is valid, false otherwise.
     */
    fun isUnicastAddressValid(unicastAddress: UnicastAddress, numberOfElements: Int): Boolean {
        val range = UnicastRange(address = unicastAddress, elementsCount = numberOfElements)
        val target = nodeProvisioningProtocolInterfaceTarget
            ?: return meshNetwork.localProvisioner?.let { provisioner ->
                meshNetwork.isAddressRangeAvailable(range = range) &&
                        provisioner.hasAllocatedRange(range = range)
            } ?: false

        // 대상 노드 자신의 점유는 제외하고 나머지 노드·exclusion 과만 비교한다.
        if (!meshNetwork.isAddressRangeAvailable(range = range, ignoring = target)) return false

        return when (nodeProvisioningProtocolInterfaceProcedure) {
            // §3.11.8.5 — 주소가 반드시 바뀌고 옛 범위와 겹치지 않아야 한다. 노드도 검사한다
            // (`refresh_is_valid()`: `addr < old_addr || addr >= old_addr + elem_count`).
            // 새 주소는 우리가 배정하는 것이므로 할당 범위 검사를 유지한다.
            NodeProvisioningProtocolInterfaceProcedure.NODE_ADDRESS_REFRESH ->
                unicastAddress != target.primaryUnicastAddress &&
                        !range.overlaps(other = target.unicastRange) &&
                        meshNetwork.localProvisioner?.hasAllocatedRange(range = range) == true

            // §3.11.8.4 / §3.11.8.6 — 주소 유지.
            else -> unicastAddress == target.primaryUnicastAddress
        }
    }

    /**
     * Sends the provisioning request to the device over the Bearer specified in the
     * constructor.
     *
     * @param request Provisioning request to be sent.
     */
    private suspend fun send(request: ProvisioningRequest): ByteArray {
        bearer.send(request)
        return request.pdu.let { it.sliceArray(1 until it.size) }
    }

    /**
     * Awaits and returns the first Provisioning PDU received over the Bearer.
     *
     * @return First Provisioning PDU received over the Bearer.
     */
    private suspend fun awaitBearerPdu(): Pdu = bearer.pdus.first {
        it.type == PduType.PROVISIONING_PDU
    }
}