package no.nordicsemi.kotlin.mesh.provisioning

import no.nordicsemi.kotlin.mesh.core.exception.NoLocalProvisioner
import no.nordicsemi.kotlin.mesh.core.exception.NoNetworkKeysAdded
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.model.MeshNetwork
import no.nordicsemi.kotlin.mesh.core.model.NetworkKey
import no.nordicsemi.kotlin.mesh.core.model.Node
import no.nordicsemi.kotlin.mesh.core.model.UnicastAddress
import no.nordicsemi.kotlin.mesh.crypto.Algorithm
import no.nordicsemi.kotlin.mesh.crypto.Algorithm.Companion.strongest


/**
 * Configuration class that contains all the necessary information to provision a device.
 *
 * @property unicastAddress    Unicast address to be assigned to the device.
 * @property networkKey        Network key to be used for provisioning.
 * @property algorithm         Algorithm to be used for provisioning.
 * @property publicKey         Public key to be used for provisioning.
 * @property authMethod        Authentication method to be used for provisioning.
 * @throws NoNetworkKeysAdded  Exception thrown when there are no network keys added to the mesh
 *                             network.
 * @throws NoLocalProvisioner  Exception thrown when there is no local provisioner added to the mesh
 *                             network.
 * @throws NoAddressAvailable  Exception thrown when there is no available unicast address.
 */
data class ProvisioningParameters(
    val unicastAddress: UnicastAddress,
    val networkKey: NetworkKey,
    val algorithm: Algorithm,
    val publicKey: PublicKey,
    val authMethod: AuthenticationMethod,
) {
    companion object {
        /**
         * Creates a default [ProvisioningParameters] based on the provided [ProvisioningCapabilities]
         * for a given network and provisioner.
         */
        internal fun defaultFrom(
            capabilities: ProvisioningCapabilities,
            meshNetwork: MeshNetwork,
        ) = ProvisioningParameters(
            unicastAddress = meshNetwork.nextAvailableUnicastAddress(
                elementCount = capabilities.numberOfElements,
                provisioner = meshNetwork.localProvisioner ?: throw NoLocalProvisioner()
            ) ?: throw NoAddressAvailable(),
            networkKey = meshNetwork.networkKeys.firstOrNull() ?: throw NoNetworkKeysAdded(),
            algorithm = capabilities.algorithms.strongest(),
            publicKey = PublicKey.NoOobPublicKey,
            authMethod = capabilities.supportedAuthMethods.first()
        )

        /**
         * simdo-patch (2026-08-26) — Node Provisioning Protocol Interface (MshPRT 1.1 §3.11.8)
         * 절차용 기본 파라미터.
         *
         * 신규 장치 프로비저닝([defaultFrom])과 다른 점 두 가지:
         *
         * 1. **Unicast Address** — Device Key Refresh(0x00) 와 Node Composition Refresh(0x02) 는
         *    주소를 **유지**해야 한다. 노드가 직접 검사한다(Zephyr `provisionee.c
         *    refresh_is_valid()`: `addr == bt_mesh_primary_addr()`). Node Address Refresh(0x01)
         *    만 새 주소를 뽑는다 — [MeshNetwork.nextAvailableUnicastAddress] 는 대상 노드의
         *    현재 주소도 "사용 중" 으로 보므로 자동으로 겹치지 않는 범위가 나온다.
         *    새 Element 개수는 이번 세션 Capabilities 기준이다.
         * 2. **Network Key** — 노드가 **이미 아는** 키여야 한다. 노드는 Provisioning Data 의
         *    NetKey 를 자기 subnet 의 현재 송신 키와 바이트 비교하고, 다르면 실패시킨다.
         *    망의 첫 키를 무조건 쓰면(= [defaultFrom]) 노드가 그 subnet 에 없을 때 조용히 깨진다.
         *
         * @param capabilities 이번 세션의 Provisioning Capabilities.
         * @param meshNetwork  대상 망.
         * @param node         NPPI 대상 노드.
         * @param procedure    실행할 NPPI 절차.
         */
        internal fun defaultForNodeProvisioningProtocolInterface(
            capabilities: ProvisioningCapabilities,
            meshNetwork: MeshNetwork,
            node: Node,
            procedure: NodeProvisioningProtocolInterfaceProcedure,
        ) = ProvisioningParameters(
            unicastAddress = when (procedure) {
                NodeProvisioningProtocolInterfaceProcedure.NODE_ADDRESS_REFRESH ->
                    meshNetwork.nextAvailableUnicastAddress(
                        elementCount = capabilities.numberOfElements,
                        provisioner = meshNetwork.localProvisioner ?: throw NoLocalProvisioner(),
                    ) ?: throw NoAddressAvailable()

                else -> node.primaryUnicastAddress
            },
            networkKey = node.networkKeys.firstOrNull { it.isPrimary }
                ?: node.networkKeys.firstOrNull()
                ?: throw InvalidNodeProvisioningProtocolInterfaceState(
                    reason = "Node at ${node.primaryUnicastAddress} knows no Network Key of " +
                            "this network, so no NPPI procedure can be run against it"
                ),
            algorithm = capabilities.algorithms.strongest(),
            publicKey = PublicKey.NoOobPublicKey,
            authMethod = capabilities.supportedAuthMethods.first()
        )
    }
}