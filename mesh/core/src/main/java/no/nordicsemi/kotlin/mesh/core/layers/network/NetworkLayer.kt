@file:Suppress("MemberVisibilityCanBePrivate", "unused", "UNUSED_PARAMETER")

package no.nordicsemi.kotlin.mesh.core.layers.network

import kotlinx.coroutines.sync.Mutex
import no.nordicsemi.kotlin.mesh.bearer.BearerError
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.gatt.GattBearer
import no.nordicsemi.kotlin.mesh.core.ProxyFilter
import no.nordicsemi.kotlin.mesh.core.layers.NetworkManager
import no.nordicsemi.kotlin.mesh.core.layers.ReceivedMessage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.AccessMessage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.ControlMessage
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.LowerTransportPdu
import no.nordicsemi.kotlin.mesh.core.layers.lowertransport.SegmentedMessage
import no.nordicsemi.kotlin.mesh.core.messages.proxy.FilterStatus
import no.nordicsemi.kotlin.mesh.core.messages.proxy.ProxyConfigurationMessage
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.GroupAddress
import no.nordicsemi.kotlin.mesh.core.model.KeyDistribution
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.core.model.UsingNewKeys
import no.nordicsemi.kotlin.mesh.core.model.VirtualAddress
import no.nordicsemi.kotlin.mesh.core.model.boundTo
import no.nordicsemi.kotlin.mesh.core.model.maxUnicastAddress
import no.nordicsemi.kotlin.mesh.logger.LogCategory
import no.nordicsemi.kotlin.mesh.logger.Logger
import kotlin.concurrent.timer
import kotlin.time.Clock
import kotlin.time.ExperimentalTime
import kotlin.uuid.ExperimentalUuidApi

/**
 * Network Layer of the mesh networking stack
 *
 * @property networkManager Network manager containing the different layers of the mesh networking
 *                          stack.
 * @constructor Constructs the network layer.
 */
internal class NetworkLayer(private val networkManager: NetworkManager) {
    private val meshNetwork: MeshNetwork
        get() = networkManager.meshNetwork
    private val logger: Logger?
        get() = networkManager.logger
    private val mutex = Mutex()
    private val secureProperties
        get() = networkManager.securePropertiesStorage
    internal var proxyNetworkKey: NetworkKey? = null
    // simdo-patch (2026-09-09): was `mutableMapOf<ByteArray, Any?>()`. ByteArray keys compare by
    // identity, so the cache never hit and never evicted. See NetworkMessageCache for the design.
    internal val networkMessageCache = NetworkMessageCache()

    /**
     * This method handles the received PDU of given type and passes it to Upper Transport Layer.
     *
     * simdo-fork (2026-09-29, 동시 GATT 설정 1단계) — [viaDefaultBearer]: 이 PDU 가 기본 베어러([NetworkManager.bearer])로
     * 왔는가. 목적지별로 등록한 베어러([NetworkManager.registerBearer])로 온 PDU 는 false. 프록시 필터 상태
     * ([proxyNetworkKey], [ProxyFilter]) 는 기본 링크 하나의 것이라, 등록 링크로 온 Secure Network Beacon 이 기본 링크의 필터
     * 초기화를 부르거나 등록 링크로 온 Proxy Configuration PDU 가 기본 링크의 필터 응답으로 소비되면 안 된다. IV Index·키
     * 갱신 판정과 메시 메시지 처리는 어느 링크로 왔든 같다(망 상태는 하나). 등록이 없으면 모든 PDU 가 기본 베어러로 오므로
     * 종전과 동작이 같다.
     *
     * @param incomingPdu  Data received.
     * @param type         PDU type.
     * @param viaDefaultBearer 기본 베어러로 왔는가 (기본 true — 루프백·종전 호출자).
     */
    suspend fun handle(incomingPdu: ByteArray, type: PduType, viaDefaultBearer: Boolean = true): ReceivedMessage? {
        // Discard provisioning pdus as they are handled by the provisioning manager
        if (type == PduType.PROVISIONING_PDU) return null

        // Secure Network Beacons can repeat whenever the device connects to a new Proxy.
        if (type != PduType.MESH_BEACON) {
            // Ensure the PDU has not been handled already (Network Message Cache, stage 1:
            // byte-identical repeats such as Network Transmit copies).
            require(!networkMessageCache.isDuplicateRawPdu(incomingPdu)) {
                logger?.d(LogCategory.NETWORK) { "PDU already handled" }
                return null
            }
        }

        // Try decoding the pdu.
        when (type) {
            PduType.NETWORK_PDU -> {
                val networkPdu = NetworkPduDecoder.decode(
                    pdu = incomingPdu,
                    pduType = type,
                    meshNetwork = meshNetwork
                )
                return if (networkPdu != null) {
                    // Network Message Cache, stage 2 (Section 3.4.6.4): relay copies carry a
                    // decremented TTL and therefore different wire bytes, so they pass stage 1.
                    // Identify them by SRC + IV Index + SEQ after decoding.
                    require(!networkMessageCache.isDuplicateSequence(networkPdu)) {
                        logger?.d(LogCategory.NETWORK) {
                            "Duplicate Network PDU (src: " +
                                    "${networkPdu.source.address.toHexString()}, " +
                                    "seq: ${networkPdu.sequence}) found in Network Message Cache"
                        }
                        return null
                    }
                    logger?.i(LogCategory.NETWORK) { "$networkPdu received" }
                    networkManager.lowerTransportLayer.handle(networkPdu = networkPdu)?.let {
                        // simdo-patch: capture sequence/ivIndex/ttl from the Network PDU.
                        ReceivedMessage(
                            source = networkPdu.source,
                            destination = networkPdu.destination,
                            message = it,
                            sequence = networkPdu.sequence,
                            ivIndex = networkPdu.ivIndex,
                            ttl = networkPdu.ttl,
                        )
                    }
                } else {
                    logger?.w(LogCategory.NETWORK) { "Failed to decrypt network pdu" }
                    null
                }
            }
            // This condition always return null because mesh beacons are not responded to by the
            // lib as of now.
            PduType.MESH_BEACON -> {
                NetworkBeaconPduDecoder.decode(pdu = incomingPdu, meshNetwork = meshNetwork)?.let {
                    logger?.i(LogCategory.NETWORK) {
                        "$it received, authenticated using key: ${it.networkKey}"
                    }
                    // TODO possible late init property initialization error
                    try {
                        handle(networkBeacon = it, viaDefaultBearer = viaDefaultBearer)
                    } catch (e: Exception) {
                        logger?.e(LogCategory.NETWORK) { "Failed to handle beacon $e" }
                    }
                    return null
                }
                UnprovisionedDeviceBeaconDecoder.decode(pdu = incomingPdu)?.let {
                    logger?.i(LogCategory.NETWORK) { "$it received" }
                    handle(beacon = it)
                    return null
                }
                logger?.w(LogCategory.NETWORK) { "Failed to decrypt mesh beacon pdu" }
                return null
            }

            PduType.PROXY_CONFIGURATION -> {
                // simdo-fork (2026-09-29) — 필터 설정은 기본 링크에만 보낸다. 등록 링크로 온 Proxy Configuration PDU 는 기본 링크의
                // 필터 응답 대기([NetworkManager.awaitProxyMessageResponse] — 송신자 구분 없이 첫 PDU) 로 소비되면 안 된다.
                if (!viaDefaultBearer) {
                    logger?.w(LogCategory.PROXY) { "Proxy Configuration PDU on a per-destination bearer ignored" }
                    return null
                }
                return NetworkPduDecoder.decode(
                    pdu = incomingPdu,
                    pduType = type,
                    meshNetwork = meshNetwork
                )?.let {
                    logger?.i(LogCategory.NETWORK) { "$it received" }
                    handle(proxyPdu = it)
                } ?: run {
                    logger?.w(LogCategory.NETWORK) { "Unable to decode network pdu" }
                    null
                }
            }

            else -> return null
        }
    }

    /**
     * This method tries to send the Lower Transport Message of given type to the given destination
     * address. If the local Provisioner does not exist, or does not have Unicast Address assigned,
     * this method does nothing.
     *
     * @param pdu       Lower Transport PDU to be sent.
     * @param type      PDU type.
     * @param ttl       Initial TTL (Time To Live) value of the message.
     * @throws BearerError.Closed when the bearer is closed.
     */
    @Throws(BearerError.Closed::class)
    suspend fun send(pdu: LowerTransportPdu, type: PduType, ttl: UByte) {
        // Compared to the iOS implementation, we check if a message is an AccessMessage and is not
        // a SegmentedMessage. This allows us to reuse the sequence number from the upper transport
        // layer for non-segmented Access Messages. However, if the PDU is a SegmentedMessage, each
        // segment will have its own sequence number incremented.
        val sequence = when {
            pdu is AccessMessage && pdu !is SegmentedMessage -> pdu.sequence
            else -> nextSequenceNumber(address = pdu.source as UnicastAddress)
        }
        val networkPdu = NetworkPduDecoder.encode(
            lowerTransportPdu = pdu,
            pduType = type,
            sequence = sequence,
            ttl = ttl
        )
        logger?.i(LogCategory.NETWORK) {
            "Sending $networkPdu (encrypted using ${networkPdu.key.name})"
        }
        // Loopback interface
        if (shouldLoopback(networkPdu = networkPdu)) {
            networkManager.handle(incomingPdu = networkPdu.pdu, type = type)
            // Messages sent with TTL = 1 will only be sent locally.
            require(ttl != 1.toUByte()) { return }
            if (isLocalUnicastAddress(address = networkPdu.destination.address)) {
                // No need to send messages targeting local Unicast Addresses.
                return
            }
            // If the message was sent locally, don't report Bearer closed error.
            try {
                // simdo-fork (2026-06-07, P6) — dst 로 라우팅. 미등록이면 default bearer.
                networkManager.bearerFor(destination = networkPdu.destination.address)
                    ?.send(pdu = networkPdu.pdu, type = type)
            } catch (e: Exception) {
                // Ignore the error because the message was sent locally.
            }
        } else {
            // Messages sent with TTL = 1 will only be sent locally.
            require(ttl != 1.toUByte()) { return }
            // simdo-fork (2026-09-29) — 등록 링크로 나가는가를 송신 전에 한 번 정한다. 등록 링크가 닫혀 있어도 기본 링크의 필터 키를
            // 지우지 않는다 (종전: 어느 베어러의 Closed 든 지워 다음 기본 링크 Beacon 에 필터 재설정이 나갔다 — 동시 설정 D3).
            val destination = networkPdu.destination.address
            val viaRegistered = networkManager.hasRegisteredBearer(destination = destination)
            try {
                // simdo-fork (2026-06-07, P6) — dst 로 라우팅. 미등록(group/proxy/평상시)이면 default bearer.
                networkManager.bearerFor(destination = destination)
                    ?.send(pdu = networkPdu.pdu, type = type)
                    ?: throw BearerError.Closed()
            } catch (e: Exception) {
                if (e is BearerError.Closed && !viaRegistered) {
                    proxyNetworkKey = null
                }
                throw e
            }
        }

        // Unless a GATT Bearer is used, the Network PDUs should be sent multiple times if
        // Network Transmit has been set for the local Provisioner's Node
        if (type == PduType.NETWORK_PDU && networkManager.bearer is GattBearer) {
            meshNetwork.localProvisioner?.node?.networkTransmit
                ?.takeIf { it.count > 1 }
                ?.let { networkTransmit ->
                    var count = networkTransmit.count
                    timer(period = networkTransmit.intervalAsMilliseconds) {
                        // networkManager.transmitter?.send(pdu = networkPdu.pdu, type = type)
                        count -= 1
                        if (count == 0)
                            cancel()
                    }
                }
        }
    }

    /**
     * Sends the Proxy Configuration Message. The Proxy Filter object will be notified about the
     * success or a failure.
     *
     * @param message The Proxy Configuration message to be sent.
     */
    suspend fun send(message: ProxyConfigurationMessage): ProxyConfigurationMessage? {
        proxyNetworkKey?.let { networkKey ->
            val source = meshNetwork.localProvisioner?.node?.primaryUnicastAddress
                ?: UnicastAddress(address = maxUnicastAddress)
            logger?.i(LogCategory.PROXY) {
                "Sending $message from: ${source.toHexString()} to 0000"
            }
            val pdu = ControlMessage.init(
                message = message,
                source = source,
                networkKey = networkKey,
                ivIndex = meshNetwork.ivIndex
            )
            logger?.i(LogCategory.NETWORK) { "Sending $pdu" }

            try {
                send(pdu = pdu, type = PduType.PROXY_CONFIGURATION, ttl = pdu.ttl)
                networkManager.proxy.onManagerDidDeliverMessage(message = message)
                return networkManager.awaitProxyMessageResponse()
                    ?.message as ProxyConfigurationMessage
            } catch (exception: Exception) {
                if (exception is BearerError.Closed) {
                    proxyNetworkKey = null
                }
                networkManager.proxy.onManagerFailedToDeliverMessage(
                    message = message,
                    error = exception
                )
            }
        } ?: networkManager.proxy.onManagerFailedToDeliverMessage(
            message = message,
            error = BearerError.Closed()
        )
        return null
    }

    /**
     * Returns the next outgoing sequence number for the given local source address.
     *
     * @param address Local source address.
     */
    @OptIn(ExperimentalUuidApi::class)
    suspend fun nextSequenceNumber(address: UnicastAddress) =
        secureProperties.nextSequenceNumber(uuid = meshNetwork.uuid, address = address)

    /**
     * This method handles the Unprovisioned Device beacon. The current implementation does nothing,
     * as remote provisioning is currently not supported.
     *
     * @param beacon Received Unprovisioned Device beacon.
     *
     */
    private fun handle(beacon: UnprovisionedDeviceBeacon) {
        // TODO Handle unprovisioned device beacon
    }

    /**
     * This method handles PDUs containing network state.
     *
     * As of Mesh Protocol 1.1 these are Secure Network beacons and Private beacons. These beacons
     * will set the IV Index and IV Update Active flag and change the Key Refresh Phase based on the
     * information specified in them.
     */
    @OptIn(ExperimentalTime::class, ExperimentalUuidApi::class)
    private suspend fun handle(networkBeacon: NetworkBeaconPdu, viaDefaultBearer: Boolean = true) {
        // The network key the beacon was authenticated with.
        val networkKey = networkBeacon.networkKey

        if (meshNetwork.primaryNetworkKey != null && networkKey.isSecondary) {
            logger?.w(LogCategory.NETWORK) {
                "Discarding beacon for secondary network (key index: ${networkKey.index})"
            }

            if (proxyNetworkKey == null) {
                if (viaDefaultBearer) updateProxyFilter(networkKey)
                return
            }
        }

        val lastIvIndex = secureProperties.ivIndex(uuid = meshNetwork.uuid)
        val lastTransitionDate = lastIvIndex.transitionDate
        val isIvRecoveryActive = lastIvIndex.isIvUpdateActive

        val isIvTestModeActive = networkManager.networkParameters.ivUpdateTestMode
        val flag = networkManager.networkParameters.allowIvIndexRecoveryOver42

        if (networkBeacon.canOverWrite(
                target = lastIvIndex,
                updatedAt = lastTransitionDate,
                isIvRecoveryActive = isIvRecoveryActive,
                isIvTestModeActive = isIvTestModeActive,
                ivRecoveryOver42Allowed = flag
            )
        ) {
            meshNetwork.ivIndex = networkBeacon.ivIndex
            if (meshNetwork.ivIndex.index > lastIvIndex.index + 1u) {
                // simdo-fork (2026-09-30) — IV Index Recovery 로 건너뛴 경우를 따로 남긴다 (앱 판정 로그 대조용).
                logger?.w(LogCategory.NETWORK) {
                    "IV Index Recovery: ${lastIvIndex.index} → ${meshNetwork.ivIndex.index} (network ${meshNetwork.uuid})"
                }
            } else if (meshNetwork.ivIndex.index > lastIvIndex.index) {
                logger?.i(LogCategory.NETWORK) { "Applying ${meshNetwork.ivIndex}" }
            }
            meshNetwork.let {
                if (it.localProvisioner?.node != null &&
                    it.ivIndex.transmitIvIndex > lastIvIndex.transmitIvIndex
                ) {
                    logger?.i(LogCategory.NETWORK) { "Resetting local sequence numbers to 0" }
                    secureProperties.resetSequenceNumber(
                        uuid = meshNetwork.uuid,
                        address = it.localProvisioner!!.node!!._primaryUnicastAddress
                    )
                }
            }

            // iOS Lib stores iv index, transition date and the recovery flag separately.
            // According to the sample app implementation the whole iv index is stored after the
            // if statement below.
            if (lastIvIndex != meshNetwork.ivIndex) {
                meshNetwork.ivIndex = meshNetwork.ivIndex.copy(transitionDate = Clock.System.now())
                    .apply {
                        ivRecoveryFlag =
                            (index > (lastIvIndex.index + 1u)) &&
                                    !networkBeacon.ivIndex.isIvUpdateActive
                    }
            }
            // Store the last IV Index
            secureProperties.storeIvIndex(uuid = meshNetwork.uuid, ivIndex = meshNetwork.ivIndex)

            // If the Key Refresh procedure is in progress, and the new Network Key has already been
            // set, the key refresh flag indicates switching to phase 2
            if (networkKey.phase is KeyDistribution &&
                networkBeacon.validForKeyRefreshProcedure &&
                !networkBeacon.keyRefreshFlag
            ) networkKey.phase = UsingNewKeys

            // if the Key Refresh Procedure is in Phase 2, and the key refresh flag is set to false.
            if (networkKey.phase is UsingNewKeys &&
                networkBeacon.validForKeyRefreshProcedure &&
                networkBeacon.keyRefreshFlag
            ) {
                // Revoke the old network key
                networkKey.oldKey = null // This will set the phase to NormalOperation
                // ...and old application keys bound to it
                meshNetwork.applicationKeys.boundTo(networkKey).forEach { it.oldKey = null }
            }

        } else if (networkBeacon.ivIndex != lastIvIndex.previous) {
            val numberOfHoursSinceData = "${(Clock.System.now() - lastTransitionDate).inWholeHours}"
            logger?.w(LogCategory.NETWORK) {
                "Discarding beacon (${networkBeacon.ivIndex}, " +
                        "last ${lastIvIndex}, changed $numberOfHoursSinceData hours ago, " +
                        "test mode: ${networkManager.networkParameters.ivUpdateTestMode})"
            }
            return
        } // else,

        // The beacon was sent by a Node with a previous IV Index, that was not yet transition to
        // the one the local node has. Such an IV Index is still valid, at least for sometime.
        // simdo-fork (2026-09-29) — 필터 상태는 기본 링크의 것. 등록 링크의 Beacon 은 IV 판정에만 쓴다(위), 필터 초기화는 부르지 않는다
        // (종전: 기본 링크가 없거나 재연결 중이면 등록 링크 Beacon 이 "방금 연결" 로 보여 필터 설정을 기본 베어러로 보냈다 — D3).
        if (viaDefaultBearer) updateProxyFilter(networkKey)
    }

    /**
     * Updates the information about the Network Key known to the current Proxy Server. The Network
     * Key is required to send proxy Configuration Messages that can be decoded by the connected
     * Proxy.
     *
     * For new Proxy connections this method also initiates the Proxy Filter with preset.
     *
     * @param networkKey The Network Key known to the connected Proxy Server.
     */
    private suspend fun updateProxyFilter(networkKey: NetworkKey) {
        val justConnected = proxyNetworkKey == null

        // Keep the primary Network Key or the most recently received one from the connected Proxy
        // Server. This is to make sure (almost) that the Proxy Configuration messages are sent
        // encrypted with a key known to this Node.
        proxyNetworkKey = networkKey

        if (justConnected) {
            networkManager.proxy.onNewProxyConnected()
        }
    }

    /**
     * Handles the received Proxy Configuration PDU. This method parses the payload and instantiates
     * a message class. The message is passed to the [ProxyFilter] for processing.
     *
     * @param proxyPdu Received Proxy Configuration PDU.
     */
    private suspend fun handle(proxyPdu: NetworkPdu): ReceivedMessage? {
        val payload = proxyPdu.transportPdu
        require(payload.size > 1) { return null }

        val controlMessage = runCatching { ControlMessage.init(proxyPdu) }.getOrElse {
            logger?.w(LogCategory.NETWORK) { "Failed to decrypt proxy PDU: $it" }
            return null
        }
        logger?.i(LogCategory.NETWORK) {
            "$controlMessage received (decrypted using key: ${controlMessage.networkKey.name})"
        }

        return when (controlMessage.opCode) {
            FilterStatus.opCode -> {
                FilterStatus.init(parameters = controlMessage.upperTransportPdu)?.let { message ->
                    logger?.i(LogCategory.PROXY) {
                        "$message received from: ${
                            proxyPdu.source.address.toHexString(
                                format = HexFormat {
                                    number.prefix = "0x"
                                    upperCase = true
                                })
                        }, dest: ${
                            proxyPdu.destination.address.toHexString(
                                format = HexFormat {
                                    number.prefix = "0x"
                                    upperCase = true
                                }
                            )
                        }"
                    }
                    // Look for the proxy Node.
                    val proxyNode = meshNetwork.node(proxyPdu.source as UnicastAddress)
                    networkManager.proxy.handle(message = message, proxy = proxyNode)
                    // simdo-patch: Proxy Configuration PDU also carries seq/ivIndex/ttl via NetworkPdu.
                    ReceivedMessage(
                        source = proxyPdu.source,
                        destination = proxyPdu.destination,
                        message = message,
                        sequence = proxyPdu.sequence,
                        ivIndex = proxyPdu.ivIndex,
                        ttl = proxyPdu.ttl,
                    )
                }
            }

            else -> {
                logger?.w(LogCategory.PROXY) {
                    "Unknown Proxy Configuration message (opCode: ${
                        controlMessage.opCode.toHexString(
                            format = HexFormat {
                                number.prefix = "0x"
                                upperCase = true
                            }
                        )
                    })"
                }
                null
            }
        }
    }

    /**
     * Check whether the given address is an address of an element belonging to the local Node.
     *
     * @param address Address to check.
     * @return `true` if the address belongs to an element in the local Node or `false` otherwise.
     */
    internal fun isLocalUnicastAddress(address: UnicastAddress) = isLocalUnicastAddress(
        address = address.address
    )

    /**
     * Check whether the given address is an address of an element belonging to the local Node.
     *
     * @param address Address to check.
     * @return `true` if the address belongs to an element in the local Node or `false` otherwise.
     */
    internal fun isLocalUnicastAddress(address: Address) =
        meshNetwork.localProvisioner?.node?.containsElementWithAddress(address) == true

    /**
     * Check if the given [NetworkPdu] should loop back for local processing.
     *
     * @param networkPdu Network PDU to check.
     * @return `true` if the PDU should be looped back or `false` otherwise.
     */
    private fun shouldLoopback(networkPdu: NetworkPdu) = networkPdu.destination is GroupAddress ||
            networkPdu.destination is VirtualAddress ||
            networkPdu.destination.takeIf { it is UnicastAddress }?.let { address ->
                isLocalUnicastAddress(address as UnicastAddress)
            } ?: false

}

