@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package no.nordicsemi.kotlin.mesh.provisioning

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.messages.AdType
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningExtendedScanReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningExtendedScanStart
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanCapabilitiesGet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanCapabilitiesStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanGet
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanStart
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningScanStop
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.Model
import no.nordicsemi.kotlin.mesh.core.oob.OobInformation
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * An event emitted while scanning for unprovisioned devices through a Remote Provisioning
 * Server.
 */
sealed class RemoteProvisioningScanEvent {

    /**
     * The server accepted the Remote Provisioning Scan Start message and is now scanning.
     *
     * @property status    The Scan Status returned by the server. Note that
     *                     [RemoteProvisioningScanStatus.scannedItemsLimit] echoes the limit the
     *                     server actually applied, which may be its own maximum.
     * @property sessionId Monotonically increasing index of the scan session, starting at 0.
     *                     Only meaningful within a single [RemoteProvisioningScanner.scan] or
     *                     [RemoteProvisioningScanner.scanContinuously] collection.
     */
    data class SessionStarted(
        val status: RemoteProvisioningScanStatus,
        val sessionId: Int,
    ) : RemoteProvisioningScanEvent()

    /**
     * An unprovisioned device was heard by the server.
     *
     * Within one session each Device UUID is reported **at most once**; de-duplication across
     * sessions (and any RSSI smoothing) is the caller's responsibility — see
     * [RemoteProvisioningScanner] for why.
     *
     * @property report    The raw Scan Report, including the server-measured
     *                     [RemoteProvisioningScanReport.rssi].
     * @property sessionId Index of the session this report belongs to.
     */
    data class DeviceReported(
        val report: RemoteProvisioningScanReport,
        val sessionId: Int,
    ) : RemoteProvisioningScanEvent()

    /**
     * An Extended Scan Report was received. This normally arrives in response to
     * [RemoteProvisioningScanner.extendedScan], but the server may also emit one while a
     * regular scan is running.
     *
     * @property report    The Extended Scan Report, whose
     *                     [RemoteProvisioningExtendedScanReport.localName] carries the
     *                     advertised device name.
     * @property sessionId Index of the session this report belongs to.
     */
    data class ExtendedDeviceReported(
        val report: RemoteProvisioningExtendedScanReport,
        val sessionId: Int,
    ) : RemoteProvisioningScanEvent()

    /**
     * The scan session has run to completion on the server. When scanning continuously, the
     * next [SessionStarted] follows.
     *
     * @property sessionId    Index of the session that just ended.
     * @property deviceCount  Number of distinct devices reported during that session.
     */
    data class SessionCompleted(
        val sessionId: Int,
        val deviceCount: Int,
    ) : RemoteProvisioningScanEvent()

    /**
     * The server refused to start (or continue) scanning. The flow terminates after this event.
     *
     * @property status    The reported status, or `null` when no Scan Status was received at
     *                     all (the message was dropped, or the server is unreachable).
     * @property sessionId Index of the session that failed to start.
     */
    data class SessionFailed(
        val status: RemoteProvisioningMessageStatus?,
        val sessionId: Int,
    ) : RemoteProvisioningScanEvent()
}

/**
 * Drives the Remote Provisioning Scan and Extended Scan procedures on a single Remote
 * Provisioning Server.
 *
 * ### Why the API is session-shaped
 *
 * The Remote Provisioning Scan procedure is **not** a subscription. A Remote Provisioning
 * Server:
 *  - scans only for the number of seconds given in the Scan Start message (1..255, one octet),
 *  - reports each Device UUID **at most once per session** — the "already reported" flag is
 *    cleared only by a new Scan Start (`rpr_srv.c`),
 *  - stores at most `CONFIG_BT_MESH_RPR_SRV_SCANNED_ITEMS_MAX` devices (32 on our
 *    `neo_mesh_commissioner` dongle) and **silently ignores** further devices once full — the
 *    array is filled in *first heard* order, not by RSSI,
 *  - sends reports serially as segmented, acknowledged messages, one device per message.
 *
 * Consequences for callers:
 *  - a live device list requires restarting the session repeatedly — use [scanContinuously],
 *    or drive [scan] in a loop yourself,
 *  - one RSSI sample per device per session; averaging (e.g. an EMA over sessions) and
 *    proximity ranking belong in the application,
 *  - de-duplication across sessions is by [RemoteProvisioningScanReport.uuid], which is the
 *    same Device UUID later used for [PBRemoteBearer] and stays stable after provisioning,
 *  - short sessions (5..15 s) refresh RSSI faster but cost a Scan Start round trip each time;
 *    long sessions are cheaper but let the 32-device array fill and stall discovery.
 *
 * ### Airtime
 *
 * Every report is a segmented, acknowledged mesh transaction. Do not scan while performing
 * bulk configuration or a firmware distribution over the same subnet.
 *
 * @property manager Mesh network manager used to send and receive the messages.
 * @property server  Unicast address of the **element** hosting the Remote Provisioning Server
 *                   model.
 */
@OptIn(ExperimentalUuidApi::class)
class RemoteProvisioningScanner internal constructor(
    private val transport: MeshMessageTransport,
    val server: Address,
) {

    /**
     * Creates a scanner driving the Remote Provisioning Server at [server].
     *
     * @param manager Mesh network manager used to exchange the messages.
     * @param server  Unicast address of the element hosting the Remote Provisioning Server model.
     */
    constructor(manager: MeshNetworkManager, server: Address) : this(
        transport = MeshNetworkManagerTransport(manager = manager),
        server = server
    )

    /**
     * Creates a scanner targeting the given Remote Provisioning Server model.
     *
     * @param manager Mesh network manager.
     * @param model   A Remote Provisioning Server model, obtained e.g. through
     *                `node.model(Model.REMOTE_PROVISIONING_SERVER_MODEL_ID.toUInt())`.
     * @throws IllegalArgumentException if the model is not a Remote Provisioning Server, or it
     *                                  does not belong to an Element.
     */
    constructor(manager: MeshNetworkManager, model: Model) : this(
        manager = manager,
        server = requireNotNull(
            model.takeIf { it.isRemoteProvisioningServer }?.parentElement?.unicastAddress
        ) {
            "The given model is not a Remote Provisioning Server model bound to an Element"
        }.address
    )

    /**
     * Reads the Remote Provisioning Scan Capabilities state of the server.
     *
     * Use [RemoteProvisioningScanCapabilitiesStatus.maxScannedItems] as the upper bound for
     * [scan]'s `scannedItemsLimit`: a larger value is rejected with
     * [RemoteProvisioningMessageStatus.SCANNING_CANNOT_START].
     *
     * @return The capabilities, or `null` on timeout.
     */
    suspend fun capabilities(): RemoteProvisioningScanCapabilitiesStatus? = transport.send(
        message = RemoteProvisioningScanCapabilitiesGet(),
        destination = server
    ) as? RemoteProvisioningScanCapabilitiesStatus

    /**
     * Reads the current Remote Provisioning Scan state of the server.
     *
     * @return The scan status, or `null` on timeout.
     */
    suspend fun state(): RemoteProvisioningScanStatus? = transport.send(
        message = RemoteProvisioningScanGet(),
        destination = server
    ) as? RemoteProvisioningScanStatus

    /**
     * Terminates the Remote Provisioning Scan procedure on the server.
     *
     * Note that the server flushes one last pending Scan Report before going idle, so a report
     * may still arrive after this call returns.
     *
     * @return The scan status, or `null` on timeout.
     */
    suspend fun stop(): RemoteProvisioningScanStatus? = transport.send(
        message = RemoteProvisioningScanStop(),
        destination = server
    ) as? RemoteProvisioningScanStatus

    /**
     * Runs a **single** Remote Provisioning Scan session and emits its events.
     *
     * The returned flow completes on its own once the session has expired on the server (plus
     * [reportGracePeriod], to let reports that were already in flight arrive). Cancelling the
     * collection sends a Remote Provisioning Scan Stop to the server.
     *
     * @param timeout           Duration of the scan session, 1..255 seconds.
     * @param scannedItemsLimit Maximum number of devices the server should report. `0` (default)
     *                          lets the server use its own maximum.
     * @param uuid              When set, performs a Single Device Scan for this Device UUID.
     * @param reportGracePeriod Extra time to keep listening after the session expired.
     * @return A cold flow of [RemoteProvisioningScanEvent].
     */
    fun scan(
        timeout: Duration = DEFAULT_SESSION_TIMEOUT,
        scannedItemsLimit: UByte = 0u,
        uuid: Uuid? = null,
        reportGracePeriod: Duration = DEFAULT_REPORT_GRACE_PERIOD,
    ): Flow<RemoteProvisioningScanEvent> = scanContinuously(
        sessionTimeout = timeout,
        scannedItemsLimit = scannedItemsLimit,
        uuid = uuid,
        reportGracePeriod = reportGracePeriod,
        sessions = 1
    )

    /**
     * Runs Remote Provisioning Scan sessions back to back until the collection is cancelled,
     * emitting a [RemoteProvisioningScanEvent.SessionCompleted] between rounds.
     *
     * This is the shape most user interfaces want: because the server reports every device once
     * per session, restarting the session is what turns the procedure into a live view and what
     * produces a fresh RSSI sample per device per round.
     *
     * Restarting does **not** require a Scan Stop first — a Scan Start from the same client
     * while scanning is accepted and clears the "already reported" flags. A Scan Stop is only
     * sent when the collection ends.
     *
     * @param sessionTimeout    Duration of each scan session, 1..255 seconds.
     * @param scannedItemsLimit Maximum number of devices the server should report per session.
     * @param uuid              When set, performs a Single Device Scan for this Device UUID.
     * @param reportGracePeriod Extra time to keep listening after each session expires.
     * @param sessions          Number of sessions to run, or `null` (default) to run until
     *                          cancelled.
     * @return A cold flow of [RemoteProvisioningScanEvent].
     */
    fun scanContinuously(
        sessionTimeout: Duration = DEFAULT_SESSION_TIMEOUT,
        scannedItemsLimit: UByte = 0u,
        uuid: Uuid? = null,
        reportGracePeriod: Duration = DEFAULT_REPORT_GRACE_PERIOD,
        sessions: Int? = null,
    ): Flow<RemoteProvisioningScanEvent> = channelFlow {
        // simdo-fork (2026-08-12, 감사 P2-g): sessionId 와 reportedInSession 은 세션 루프
        // 코루틴이 쓰고 observer 코루틴이 읽는다. 캡처된 평범한 `var` 는 두 코루틴 사이에
        // happens-before 가 없어 이벤트의 sessionId·deviceCount 가 어긋날 수 있었다.
        val sessionId = AtomicInteger(0)
        val reportedInSession =
            AtomicReference<MutableSet<Uuid>>(ConcurrentHashMap.newKeySet())

        // Subscribe *before* the first Scan Start, otherwise a fast report could be missed.
        val subscribed = CompletableDeferred<Unit>()
        val observer = launch {
            transport.events(onSubscribed = { subscribed.complete(Unit) })
                .collect { event ->
                    if (event.source != server) return@collect
                    when (val message = event.message) {
                        is RemoteProvisioningScanReport -> {
                            reportedInSession.get().add(message.uuid)
                            send(
                                RemoteProvisioningScanEvent.DeviceReported(
                                    report = message,
                                    sessionId = sessionId.get()
                                )
                            )
                        }

                        is RemoteProvisioningExtendedScanReport -> send(
                            RemoteProvisioningScanEvent.ExtendedDeviceReported(
                                report = message,
                                sessionId = sessionId.get()
                            )
                        )

                        else -> Unit
                    }
                }
        }
        subscribed.await()

        try {
            while (sessions == null || sessionId.get() < sessions) {
                reportedInSession.set(ConcurrentHashMap.newKeySet())
                val status = transport.send(
                    message = RemoteProvisioningScanStart(
                        scannedItemsLimit = scannedItemsLimit,
                        timeout = sessionTimeout,
                        uuid = uuid
                    ),
                    destination = server
                ) as? RemoteProvisioningScanStatus

                if (status == null || !status.isSuccess) {
                    send(
                        RemoteProvisioningScanEvent.SessionFailed(
                            status = status?.status,
                            sessionId = sessionId.get()
                        )
                    )
                    return@channelFlow
                }
                send(
                    RemoteProvisioningScanEvent.SessionStarted(
                        status = status,
                        sessionId = sessionId.get()
                    )
                )

                delay(sessionTimeout + reportGracePeriod)

                send(
                    RemoteProvisioningScanEvent.SessionCompleted(
                        sessionId = sessionId.get(),
                        deviceCount = reportedInSession.get().size
                    )
                )
                sessionId.incrementAndGet()
            }
        } finally {
            observer.cancel()
            // ⚠️ NonCancellable: 취소가 Scan Status 도착 또는 acknowledged-message 타임아웃
            // (`NetworkManager.awaitMeshMessageResponse`) 만큼 지연된다. 유한하지만 즉시는 아니다.
            // 그 대신 서버 스캔이 켜진 채 방치되지 않는다(airtime 보호).
            withContext(NonCancellable) {
                runCatching { stop() }
            }
        }
    }

    /**
     * Runs the Remote Provisioning Extended Scan procedure for a single unprovisioned device
     * and awaits the resulting report.
     *
     * This is the only in-band way to learn the **advertised name** of an unprovisioned device;
     * a plain Scan Report carries nothing but the Device UUID.
     *
     * ⚠️ The server truncates the filter to `CONFIG_BT_MESH_RPR_AD_TYPES_MAX` entries without
     * telling the client (4 on our dongle) — put the AD Types that matter most first. The
     * procedure also requires the provisioning link to be idle; otherwise the report carries
     * [RemoteProvisioningMessageStatus.LIMITED_RESOURCES].
     *
     * @param uuid            Device UUID of the device to inspect.
     * @param adTypeFilter    AD Types to collect, in priority order. Defaults to the device name.
     * @param timeout         Scan window on the server, 1..21 seconds.
     * @param responseTimeout How long to wait for the report. Defaults to [timeout] plus
     *                        [DEFAULT_REPORT_GRACE_PERIOD].
     * @return The report, or `null` if none arrived in time (which is also what happens when
     *         the server rejected the request outright — a malformed Extended Scan Start
     *         produces no response at all).
     */
    suspend fun extendedScan(
        uuid: Uuid,
        adTypeFilter: List<UByte> = listOf(AdType.COMPLETE_LOCAL_NAME),
        timeout: Duration = DEFAULT_EXTENDED_SCAN_TIMEOUT,
        responseTimeout: Duration = timeout + DEFAULT_REPORT_GRACE_PERIOD,
    ): RemoteProvisioningExtendedScanReport? = awaitExtendedScanReport(
        message = RemoteProvisioningExtendedScanStart(
            adTypeFilter = adTypeFilter,
            uuid = uuid,
            timeout = timeout
        ),
        expectedUuid = uuid,
        responseTimeout = responseTimeout
    )

    /**
     * Runs the Remote Provisioning Extended Scan procedure against the Remote Provisioning
     * Server itself, which reports its own Device UUID, OOB information and (if configured) URI.
     *
     * @param adTypeFilter    AD Types to collect.
     * @param responseTimeout How long to wait for the report.
     * @return The report, or `null` if none arrived in time.
     */
    suspend fun extendedScanSelf(
        adTypeFilter: List<UByte> = listOf(AdType.URI),
        responseTimeout: Duration = DEFAULT_REPORT_GRACE_PERIOD + DEFAULT_EXTENDED_SCAN_TIMEOUT,
    ): RemoteProvisioningExtendedScanReport? = awaitExtendedScanReport(
        message = RemoteProvisioningExtendedScanStart(adTypeFilter = adTypeFilter),
        expectedUuid = null,
        responseTimeout = responseTimeout
    )

    private suspend fun awaitExtendedScanReport(
        message: RemoteProvisioningExtendedScanStart,
        expectedUuid: Uuid?,
        responseTimeout: Duration,
    ): RemoteProvisioningExtendedScanReport? = coroutineScope {
        val subscribed = CompletableDeferred<Unit>()
        val report = async {
            withTimeoutOrNull(responseTimeout) {
                transport.events(onSubscribed = { subscribed.complete(Unit) })
                    .first {
                        it.source == server &&
                                it.message is RemoteProvisioningExtendedScanReport &&
                                (expectedUuid == null ||
                                        (it.message as RemoteProvisioningExtendedScanReport).uuid ==
                                        expectedUuid)
                    }
                    .message as RemoteProvisioningExtendedScanReport
            }
        }
        subscribed.await()
        transport.send(message = message, destination = server)
        report.await()
    }

    companion object {

        /** Default scan session length. Short enough to refresh RSSI at a usable rate. */
        val DEFAULT_SESSION_TIMEOUT: Duration = 10.seconds

        /**
         * Extra time the scanner keeps listening after a session expired, so that segmented
         * reports already in flight are not attributed to the next session.
         */
        val DEFAULT_REPORT_GRACE_PERIOD: Duration = 3.seconds

        /** Default Extended Scan window. Must stay within 1..21 seconds. */
        val DEFAULT_EXTENDED_SCAN_TIMEOUT: Duration = 5.seconds
    }
}

/**
 * Converts a Remote Provisioning Scan Report into an [UnprovisionedDevice] that can be handed
 * straight to [ProvisioningManager] together with a
 * [no.nordicsemi.kotlin.mesh.provisioning.bearer.PBRemoteBearer].
 *
 * @param name Name to give the device. Scan Reports carry no name; use
 *             [RemoteProvisioningScanner.extendedScan] to obtain the advertised one.
 * @return The unprovisioned device.
 */
@OptIn(ExperimentalUuidApi::class)
fun RemoteProvisioningScanReport.toUnprovisionedDevice(name: String = ""): UnprovisionedDevice =
    UnprovisionedDevice(
        name = name,
        uuid = uuid,
        oobInformation = oobInformation ?: OobInformation.None
    )

/**
 * Converts a Remote Provisioning Extended Scan Report into an [UnprovisionedDevice], using the
 * advertised local name when one was collected.
 *
 * @param name Name to use when the report carries no local name.
 * @return The unprovisioned device.
 */
@OptIn(ExperimentalUuidApi::class)
fun RemoteProvisioningExtendedScanReport.toUnprovisionedDevice(
    name: String = "",
): UnprovisionedDevice = UnprovisionedDevice(
    name = localName ?: name,
    uuid = uuid,
    oobInformation = oobInformation ?: OobInformation.None
)
