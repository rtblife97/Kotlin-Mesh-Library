@file:Suppress("ClassName", "unused")

package no.nordicsemi.kotlin.mesh.provisioning

/**
 * Set of errors which may be thrown during provisioning a device.
 */
sealed class ProvisioningError : Exception()

/**
 * Thrown when the ProvisioningManager is in an invalid state.
 */
class InvalidState : ProvisioningError()

/**
 * The received PDU is invalid.
 */
class InvalidPdu : ProvisioningError()

/**
 * The received Public Key is invalid or not equal to Provisioner's Public Key.
 */
class InvalidPublicKey : ProvisioningError()

/**
 * The received Public Key is invalid or not equal to Provisioner's Public Key.
 */
class InvalidConfirmation : ProvisioningError()

/**
 * Thrown when the Unprovisioned Device is not supported by the manager.
 */
class UnsupportedDevice : ProvisioningError()

/**
 * Thrown when the provided alphanumeric value could not be converted into bytes using ASCII
 * encoding.
 */
class InvalidOobValueFormat : ProvisioningError()

/**
 * Thrown when no available Unicast Address was found in the Provisioner's range that could be
 * allocated for the device.
 */
class NoAddressAvailable : ProvisioningError()

/**
 * Thrown when the unicast address is invalid.
 */
class InvalidAddress : ProvisioningError()

/**
 * Throws when the Unicast Address has not been set.
 */
class AddressNotSpecified : ProvisioningError()

/**
 * Throws when the Network Key has not been set.
 */
class NetworkKeyNotSpecified : ProvisioningError()

/**
 * Thrown when confirmation value received from the device does not match calculated value.
 * Authentication failed.
 */
class ConfirmationFailed : ProvisioningError()

/**
 * Thrown when the remove device sent a failure indication.
 *
 * @param error The error received from the remote device.
 */
data class RemoteError(val error: RemoteProvisioningError) : ProvisioningError()

/**
 * Thrown when the key pair generation has failed.
 *
 * @param throwable The exception that caused the failure.
 */
data class KeyGenerationFailed(val throwable: Throwable) : ProvisioningError()

/**
 * simdo-patch (2026-08-26) — Node Provisioning Protocol Interface (MshPRT 1.1 §3.11.8) 절차의
 * 사전조건이 깨졌을 때 던진다.
 *
 * NPPI 는 provisioning PDU 흐름은 같지만 **Provisioning Data PDU 의 내용에 추가 제약**이 있고,
 * 그 제약을 어기면 노드는 조용히 실패하거나(`prov_send_fail_msg(PROV_ERR_INVALID_DATA)`)
 * 링크를 닫아버려 원인 파악이 어렵다. Zephyr `provisionee.c refresh_is_valid()` 가 강제하는
 * 세 가지:
 *
 * 1. IV Index 가 노드의 현재 IV Index 와 같을 것,
 * 2. Network Key 가 노드가 **이미 아는** subnet 의 현재 송신 키일 것,
 * 3. Unicast Address 가 절차별 규칙에 맞을 것(0x00/0x02 는 동일, 0x01 은 옛 범위 밖).
 *
 * 3번은 [InvalidAddress] 로 던진다. 이 예외는 1·2번과 대상 노드 자체의 문제를 나타낸다.
 *
 * @param reason 사람이 읽을 수 있는 위반 사유.
 */
data class InvalidNodeProvisioningProtocolInterfaceState(
    val reason: String,
) : ProvisioningError() {
    override val message: String
        get() = reason
}
