package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import no.nordicsemi.kotlin.mesh.bearer.BearerError
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.bearer.provisioning.ProvisioningBearer
import no.nordicsemi.kotlin.mesh.crypto.Algorithm
import no.nordicsemi.kotlin.mesh.crypto.Algorithms
import no.nordicsemi.kotlin.mesh.crypto.Crypto
import no.nordicsemi.kotlin.mesh.crypto.toByteArray
import java.security.PrivateKey

/**
 * simdo-patch (2026-08-26) — 프로비저닝 프로토콜의 **디바이스 쪽**을 실제 crypto 로 연기하는
 * in-memory 베어러.
 *
 * NPPI 종결 규칙은 `provision()` 이 끝까지 돌아야 실행된다. 그런데 라이브러리에는 provisioning
 * 상태머신을 끝까지 태우는 테스트가 하나도 없어서, 종결 코드가 "컴파일은 되지만 한 번도 돈 적
 * 없는" 상태로 남을 위험이 있었다. 이 fake 는 MshPRT 1.1 §5.4 의 provisionee 역할을 그대로
 * 수행한다 — ECDH · confirmation · session key · Provisioning Data 복호까지 전부 진짜 계산이라
 * confirmation 이 어긋나거나 Provisioning Data 가 깨지면 실제로 실패한다.
 *
 * 검증하는 것(= 노드가 실제로 하는 검사, Zephyr `provisionee.c refresh_is_valid()`):
 * - Provisioning Data 의 IV Index 가 [expectedIvIndex] 와 같은가
 * - Network Key 가 [expectedNetworkKey] 와 바이트 동일한가
 * - Unicast Address 가 절차별 규칙에 맞는가 ([expectedAddress] / [forbiddenRange])
 *
 * 이 중 하나라도 어긋나면 Provisioning Failed(0x07 = Cannot Assign Addresses / 0x05 = Invalid
 * Data 에 해당하는 [RemoteProvisioningError])를 돌려준다.
 *
 * @property numberOfElements  Capabilities 로 보고할 Element 개수.
 * @property expectedNetworkKey 노드가 이미 아는 NetKey. `null` 이면 검사하지 않는다(신규 장치).
 * @property expectedIvIndex   노드의 현재 IV Index. `null` 이면 검사하지 않는다.
 * @property expectedAddress   이 주소여야만 수락(NPPI 0x00/0x02). `null` 이면 검사하지 않는다.
 * @property forbiddenRange    이 범위와 겹치면 거부(NPPI 0x01 의 "옛 범위 밖"). `null` 이면 없음.
 */
class FakeProvisioneeBearer(
    private val numberOfElements: Int = 1,
    private val expectedNetworkKey: ByteArray? = null,
    private val expectedIvIndex: UInt? = null,
    private val expectedAddress: UShort? = null,
    private val forbiddenRange: IntRange? = null,
) : ProvisioningBearer {

    private val channel = Channel<Pdu>(capacity = Channel.UNLIMITED)
    private val _state = MutableStateFlow<BearerEvent>(BearerEvent.Closed(BearerError.Closed()))

    override val state: StateFlow<BearerEvent> = _state.asStateFlow()
    override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.ProvisioningPdu)
    override val isOpen: Boolean get() = _state.value is BearerEvent.Opened
    override val pdus: Flow<Pdu> get() = channel.receiveAsFlow()

    /** Device Key the fake device derived. The provisioner must end up with exactly this. */
    var derivedDeviceKey: ByteArray? = null
        private set

    /** Unicast Address carried by the Provisioning Data PDU, as the device decoded it. */
    var receivedUnicastAddress: UShort? = null
        private set

    /** Set when the device rejected the Provisioning Data. */
    var rejection: String? = null
        private set

    /**
     * The 25 decrypted octets of the Provisioning Data PDU, exactly as they came off the wire.
     *
     * MshPRT 1.1 §5.4.2.5:
     * `NetworkKey(16) | NetKeyIndex(2) | Flags(1) | IVIndex(4) | UnicastAddress(2)`.
     */
    var receivedProvisioningData: ByteArray? = null
        private set

    private val algorithm = Algorithm.BTM_ECDH_P256_CMAC_AES128_AES_CCM
    private val authValue = ByteArray(algorithm.length shr 3)

    private lateinit var privateKey: PrivateKey
    private lateinit var publicKeyBytes: ByteArray
    private lateinit var sharedSecret: ByteArray
    private lateinit var deviceRandom: ByteArray
    private var provisionerRandom: ByteArray = byteArrayOf()

    /** MshPRT 1.1 §5.4.2.5 Confirmation Inputs, accumulated in the very same order. */
    private var confirmationInputs = byteArrayOf()

    private val capabilities = ProvisioningCapabilities(
        numberOfElements = numberOfElements,
        algorithms = listOf(Algorithms.BtmEcdhP256CmacAes128AesCcm),
        publicKeyType = emptyList(),
        oobTypes = emptyList(),
        outputOobSize = 0u,
        outputOobActions = emptyList(),
        inputOobSize = 0u,
        inputOobActions = emptyList(),
    )

    override suspend fun open() {
        _state.value = BearerEvent.Opened
    }

    override suspend fun close() {
        _state.value = BearerEvent.Closed(BearerError.Closed())
    }

    override suspend fun send(pdu: ByteArray, type: PduType) {
        require(type == PduType.PROVISIONING_PDU) { "Only provisioning PDUs are supported" }
        when (pdu[0].toInt()) {
            0x00 -> onInvite(pdu)
            0x02 -> confirmationInputs += pdu.sliceArray(indices = 1 until pdu.size)
            0x03 -> onProvisionerPublicKey(pdu)
            0x05 -> onProvisionerConfirmation()
            0x06 -> onProvisionerRandom(pdu)
            0x07 -> onProvisioningData(pdu)
            else -> error("Unexpected provisioning PDU type 0x%02X".format(pdu[0]))
        }
    }

    private suspend fun onInvite(pdu: ByteArray) {
        confirmationInputs += pdu.sliceArray(indices = 1 until pdu.size)
        val response = ProvisioningResponse.Capabilities(capabilities = capabilities)
        confirmationInputs += response.pdu.sliceArray(indices = 1 until response.pdu.size)
        reply(response)
    }

    private suspend fun onProvisionerPublicKey(pdu: ByteArray) {
        confirmationInputs += pdu.sliceArray(indices = 1 until pdu.size)
        Crypto.generateKeyPair(algorithm = algorithm).let {
            privateKey = it.private
            publicKeyBytes = it.public.toByteArray()
        }
        sharedSecret = Crypto.calculateSharedSecret(
            privateKey = privateKey,
            publicKey = pdu.sliceArray(indices = 1 until pdu.size),
        )
        confirmationInputs += publicKeyBytes
        reply(ProvisioningResponse.PublicKey(key = publicKeyBytes))
    }

    private suspend fun onProvisionerConfirmation() {
        deviceRandom = Crypto.generateRandom(sizeInBits = algorithm.length)
        reply(
            ProvisioningResponse.Confirmation(
                confirmation = Crypto.calculateConfirmation(
                    confirmationInputs = confirmationInputs,
                    sharedSecret = sharedSecret,
                    deviceRandom = deviceRandom,
                    authValue = authValue,
                    algorithm = algorithm,
                )
            )
        )
    }

    private suspend fun onProvisionerRandom(pdu: ByteArray) {
        provisionerRandom = pdu.sliceArray(indices = 1 until pdu.size)
        reply(ProvisioningResponse.Random(random = deviceRandom))
    }

    private suspend fun onProvisioningData(pdu: ByteArray) {
        val keys = Crypto.calculateKeys(
            algorithm = algorithm,
            confirmationInputs = confirmationInputs,
            sharedSecret = sharedSecret,
            provisionerRandom = provisionerRandom,
            deviceRandom = deviceRandom,
        )
        val plain = Crypto.decrypt(
            data = pdu.sliceArray(indices = 1 until pdu.size),
            key = keys.first,
            nonce = keys.second,
            micSize = 8,
        ) ?: run {
            rejection = "Provisioning Data did not authenticate"
            reply(ProvisioningResponse.Failed(error = RemoteProvisioningError.DECRYPTION_FAILED))
            return
        }

        receivedProvisioningData = plain
        val netKey = plain.sliceArray(indices = 0 until 16)
        val ivIndex = ((plain[19].toUInt() and 0xFFu) shl 24) or
                ((plain[20].toUInt() and 0xFFu) shl 16) or
                ((plain[21].toUInt() and 0xFFu) shl 8) or
                (plain[22].toUInt() and 0xFFu)
        val address = (((plain[23].toInt() and 0xFF) shl 8) or (plain[24].toInt() and 0xFF))
            .toUShort()

        receivedUnicastAddress = address

        // Zephyr `provisionee.c refresh_is_valid()` 가 하는 검사 그대로.
        expectedIvIndex?.let {
            if (it != ivIndex) {
                rejection = "IV Index mismatch: expected $it, got $ivIndex"
                reply(ProvisioningResponse.Failed(error = RemoteProvisioningError.INVALID_ADDRESS))
                return
            }
        }
        expectedNetworkKey?.let {
            if (!it.contentEquals(netKey)) {
                rejection = "Network Key is not the one this node already knows"
                reply(ProvisioningResponse.Failed(error = RemoteProvisioningError.INVALID_ADDRESS))
                return
            }
        }
        expectedAddress?.let {
            if (it != address) {
                rejection = "Address must stay $it, got $address"
                reply(ProvisioningResponse.Failed(error = RemoteProvisioningError.INVALID_ADDRESS))
                return
            }
        }
        forbiddenRange?.let {
            if (address.toInt() in it) {
                rejection = "Address $address is inside the node's previous range $it"
                reply(ProvisioningResponse.Failed(error = RemoteProvisioningError.INVALID_ADDRESS))
                return
            }
        }

        derivedDeviceKey = keys.third
        reply(ProvisioningResponse.Complete)
    }

    private suspend fun reply(response: ProvisioningResponse) {
        channel.send(element = Pdu(data = response.pdu, type = PduType.PROVISIONING_PDU))
    }
}
