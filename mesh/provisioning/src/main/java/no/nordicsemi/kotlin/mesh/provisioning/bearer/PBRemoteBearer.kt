@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package no.nordicsemi.kotlin.mesh.provisioning.bearer

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import no.nordicsemi.kotlin.mesh.bearer.BearerError
import no.nordicsemi.kotlin.mesh.bearer.BearerEvent
import no.nordicsemi.kotlin.mesh.bearer.Pdu
import no.nordicsemi.kotlin.mesh.bearer.PduType
import no.nordicsemi.kotlin.mesh.bearer.PduTypes
import no.nordicsemi.kotlin.mesh.bearer.provisioning.ProvisioningBearer
import no.nordicsemi.kotlin.mesh.core.MeshNetworkManager
import no.nordicsemi.kotlin.mesh.core.messages.NodeProvisioningProtocolInterfaceProcedure
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkCloseReason
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningLinkState
import no.nordicsemi.kotlin.mesh.core.messages.RemoteProvisioningMessageStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkClose
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkOpen
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningLinkStatus
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUOutboundReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUReport
import no.nordicsemi.kotlin.mesh.core.messages.foundation.remoteprovisioning.RemoteProvisioningPDUSend
import no.nordicsemi.kotlin.mesh.core.model.Address
import no.nordicsemi.kotlin.mesh.core.model.Model
import no.nordicsemi.kotlin.mesh.logger.LogCategory
import no.nordicsemi.kotlin.mesh.logger.Logger
import no.nordicsemi.kotlin.mesh.provisioning.MeshMessageTransport
import no.nordicsemi.kotlin.mesh.provisioning.MeshNetworkManagerTransport
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * Errors specific to the PB-Remote provisioning bearer.
 */
sealed class PBRemoteBearerError : Exception() {

    /**
     * The Remote Provisioning Server refused to open the link, or did not answer the
     * Remote Provisioning Link Open message at all.
     *
     * @property status Status reported by the server, or `null` on timeout.
     */
    class LinkCannotOpen(val status: RemoteProvisioningMessageStatus?) : PBRemoteBearerError() {
        override fun toString() = "Remote Provisioning link cannot open (status: $status)"
    }

    /**
     * The link was accepted but never reached [RemoteProvisioningLinkState.LINK_ACTIVE] within
     * the allowed time — typically the unprovisioned device stopped advertising or is out of the
     * server's range.
     */
    class LinkOpenTimeout : PBRemoteBearerError() {
        override fun toString() = "Timed out waiting for the Remote Provisioning link to open"
    }

    /**
     * A Remote Provisioning PDU Send was not acknowledged by a
     * [RemoteProvisioningPDUOutboundReport] after the initial attempt and one retry.
     *
     * @property outboundPduNumber The PDU number that was not acknowledged.
     */
    class OutboundPduTimeout(val outboundPduNumber: UByte) : PBRemoteBearerError() {
        override fun toString() =
            "Remote Provisioning PDU #$outboundPduNumber was not acknowledged"
    }

    /**
     * The Remote Provisioning Server reported that the link closed.
     *
     * @property report The Link Report that closed the link.
     */
    class LinkClosed(val report: RemoteProvisioningLinkReport) : PBRemoteBearerError() {
        override fun toString() = "Remote Provisioning link closed: $report"
    }
}

/**
 * An implementation of the PB-Remote provisioning bearer (MshPRT 1.1, sections 4.4.5 and 5.2.2).
 *
 * Instead of the phone talking PB-GATT directly to the unprovisioned device, provisioning PDUs are
 * tunnelled through a Node that hosts the Remote Provisioning Server model. That Node opens a
 * PB-ADV link to the device on our behalf, so devices out of the phone's range — or that the phone
 * cannot connect to because its GATT connection budget is exhausted — become provisionable.
 *
 * Because it satisfies [ProvisioningBearer], `ProvisioningManager` needs **no modification**:
 *
 * ```
 * val bearer = PBRemoteBearer(manager, server = serverAddress, uuid = report.uuid)
 * try {
 *     bearer.open()
 *     ProvisioningManager(report.toUnprovisionedDevice(), network, bearer)
 *         .provision(attentionTimer = 5u)
 *         .collect { … }
 * } finally {
 *     // open() 안에서 취소돼도 베어러는 스스로 정리하지만, close() 는 멱등이므로 항상 부른다.
 *     bearer.close()
 * }
 * ```
 *
 * | [ProvisioningBearer] | Remote Provisioning                                      |
 * |----------------------|----------------------------------------------------------|
 * | [open]               | Link Open → Link Status, then await Link Report ACTIVE    |
 * | [send]               | PDU Send → await PDU Outbound Report                      |
 * | [pdus]               | PDU Report                                                |
 * | [close]              | Link Close → Link Status                                  |
 *
 * ### simdo-fork divergences from iOS `PBRemoteBearer.swift` (4.1.0)
 *
 * 1. **Received PDUs are queued, not broadcast.** `ProvisioningManager` calls
 *    `bearer.pdus.first { … }` *after* `bearer.send()` returns, and [send] deliberately suspends
 *    until the PDU Outbound Report arrives — exactly the window in which the device's answer
 *    (PDU Report) can land. A `SharedFlow` with no active subscriber would drop it and
 *    provisioning would hang until the retry. [pdus] is therefore backed by an unlimited
 *    [Channel]: at-most-once delivery, with buffering. Bounded in practice, because the Inbound
 *    PDU Count is a `UByte` that must strictly increase, so a link carries at most 255 reports.
 * 2. **Provisioning PDUs are passed through as raw bytes** — see [RemoteProvisioningPDUSend].
 * 3. **Duplicate PDU Reports are suppressed** by the Inbound PDU Count, as the NCS Remote
 *    Provisioning Client does. iOS forwards every report.
 *
 * ### Concurrency model (simdo-fork, 2026-08-12 감사 P1-1 / P1-2 / P2-d)
 *
 * Three coroutines touch this object:
 *  - the **caller** coroutine ([open] / [send] / [close]),
 *  - the **observer** coroutine, which collects incoming mesh messages,
 *  - the **supervisor** coroutine, which performs teardown when the server closes the link.
 *
 * Rules that keep that safe, and that must be preserved by future edits:
 *  - Every field shared across those coroutines is `@Volatile`. They are all single-reference or
 *    boolean reads; the one compound sequence (set `pendingOutbound` → await → clear) is already
 *    serialised on the writer side by [sendMutex], and the observer only ever reads it. A confined
 *    single-thread dispatcher was considered and rejected: it would force every public suspend
 *    entry point through `withContext`, serialising [send] behind [open]'s long await and changing
 *    cancellation semantics, for no additional guarantee.
 *  - The compound *state transition* (open→Opened vs. teardown→Closed) is additionally guarded by
 *    the non-suspending [stateLock] monitor, so a Link Report arriving between "link became
 *    active" and "publish Opened" cannot leave the bearer as `isOpen = true` with a closed channel.
 *  - **[finish] never runs on the observer coroutine.** The observer only completes
 *    [closeSignal]; the supervisor awaits it and calls [finish]. That is why [finish] may cancel
 *    the observer job safely — it is always a *different* job.
 *
 * @property server                Unicast address of the **element** hosting the Remote
 *                                 Provisioning Server model.
 * @property uuid                  Device UUID of the unprovisioned device to provision, or `null`
 *                                 when running a Node Provisioning Protocol Interface procedure
 *                                 against the server's own Node.
 * @property nppiProcedure         NPPI procedure to run. Mutually exclusive with [uuid].
 * @property linkOpenTimeout       Value of the Timeout field of the Link Open message,
 *                                 1..60 seconds. `null` uses the server default (10 s in NCS).
 * @property linkActiveTimeout     How long to wait for the Link Report announcing
 *                                 [RemoteProvisioningLinkState.LINK_ACTIVE].
 * @property outboundPduTimeout    How long to wait for a PDU Outbound Report before retrying.
 */
@OptIn(ExperimentalUuidApi::class)
class PBRemoteBearer internal constructor(
    private val transport: MeshMessageTransport,
    val server: Address,
    val uuid: Uuid? = null,
    val nppiProcedure: NodeProvisioningProtocolInterfaceProcedure? = null,
    val linkOpenTimeout: Duration? = null,
    val linkActiveTimeout: Duration = DEFAULT_LINK_ACTIVE_TIMEOUT,
    val outboundPduTimeout: Duration = DEFAULT_OUTBOUND_PDU_TIMEOUT,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : ProvisioningBearer {

    init {
        require((uuid == null) != (nppiProcedure == null)) {
            "Exactly one of uuid and nppiProcedure must be given"
        }
    }

    /**
     * Creates a PB-Remote bearer tunnelling through the Remote Provisioning Server at [server].
     *
     * @param manager            Mesh network manager used to exchange the messages.
     * @param server             Unicast address of the element hosting the Remote Provisioning
     *                           Server model.
     * @param uuid               Device UUID of the unprovisioned device.
     * @param nppiProcedure      NPPI procedure to run instead. Mutually exclusive with [uuid].
     * @param linkOpenTimeout    Link Open Timeout field, 1..60 seconds, or `null` for the server
     *                           default.
     * @param linkActiveTimeout  How long to wait for the link to become active.
     * @param outboundPduTimeout How long to wait for a PDU Outbound Report before retrying.
     * @param ioDispatcher       Dispatcher for the bearer's internal coroutines.
     */
    constructor(
        manager: MeshNetworkManager,
        server: Address,
        uuid: Uuid? = null,
        nppiProcedure: NodeProvisioningProtocolInterfaceProcedure? = null,
        linkOpenTimeout: Duration? = null,
        linkActiveTimeout: Duration = DEFAULT_LINK_ACTIVE_TIMEOUT,
        outboundPduTimeout: Duration = DEFAULT_OUTBOUND_PDU_TIMEOUT,
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    ) : this(
        transport = MeshNetworkManagerTransport(manager = manager),
        server = server,
        uuid = uuid,
        nppiProcedure = nppiProcedure,
        linkOpenTimeout = linkOpenTimeout,
        linkActiveTimeout = linkActiveTimeout,
        outboundPduTimeout = outboundPduTimeout,
        ioDispatcher = ioDispatcher,
    )

    /**
     * Creates a PB-Remote bearer targeting the given Remote Provisioning Server model.
     *
     * @param manager Mesh network manager.
     * @param model   A Remote Provisioning Server model, e.g. from
     *                `node.model(Model.REMOTE_PROVISIONING_SERVER_MODEL_ID.toUInt())`.
     * @param uuid    Device UUID of the unprovisioned device.
     * @throws IllegalArgumentException if the model is not a Remote Provisioning Server, or does
     *                                  not belong to an Element.
     */
    constructor(manager: MeshNetworkManager, model: Model, uuid: Uuid) : this(
        manager = manager,
        server = requireNotNull(
            model.takeIf { it.isRemoteProvisioningServer }?.parentElement?.unicastAddress
        ) {
            "The given model is not a Remote Provisioning Server model bound to an Element"
        }.address,
        uuid = uuid
    )

    /** Logger receiving bearer level logs. */
    var logger: Logger? = null

    private val scope = CoroutineScope(SupervisorJob() + ioDispatcher)
    private val sendMutex = Mutex()

    /**
     * Guards the compound state transition. Deliberately a plain monitor and not a
     * [kotlinx.coroutines.sync.Mutex]: [finish] must stay a **non-suspending** function so that
     * the compiler prevents anyone from adding a suspension point after it cancels a Job.
     */
    private val stateLock = Any()

    private val _state = MutableStateFlow<BearerEvent>(BearerEvent.Closed(BearerError.Closed()))
    override val state: StateFlow<BearerEvent> = _state.asStateFlow()

    override val supportedTypes: Array<PduTypes> = arrayOf(PduTypes.ProvisioningPdu)

    override val isOpen: Boolean
        get() = _state.value is BearerEvent.Opened

    @Volatile
    private var pduChannel: Channel<Pdu> = Channel(capacity = Channel.UNLIMITED)

    override val pdus: Flow<Pdu>
        get() = pduChannel.receiveAsFlow()

    /** `true` between [open] and teardown, regardless of whether the link became active. */
    @Volatile
    private var isOpening: Boolean = false

    @Volatile
    private var observer: Job? = null

    @Volatile
    private var linkActive: CompletableDeferred<Unit>? = null

    /** Completed by the observer when the server reports the link is gone. */
    @Volatile
    private var closeSignal: CompletableDeferred<Throwable>? = null

    @Volatile
    private var pendingOutbound: Pair<UByte, CompletableDeferred<Unit>>? = null

    @Volatile
    private var closeCause: Throwable? = null

    /** Remote Provisioning Inbound PDU Count, used to suppress duplicate PDU Reports. */
    @Volatile
    private var inboundPduCount: UByte = 0u

    /** Remote Provisioning Outbound PDU Count. Written only under [sendMutex]. */
    private var outboundPduCount: UByte = 0u

    /**
     * Opens the provisioning bearer by establishing a Remote Provisioning link between the server
     * and the unprovisioned device.
     *
     * Returns once the link is [RemoteProvisioningLinkState.LINK_ACTIVE].
     *
     * @throws PBRemoteBearerError.LinkCannotOpen if the server refused or did not answer.
     * @throws PBRemoteBearerError.LinkOpenTimeout if the link never became active.
     * @throws PBRemoteBearerError.LinkClosed if the server tore the link down while opening, or
     *         immediately after reporting it active.
     *
     * If the calling coroutine is cancelled while this is running, the half-open link is torn
     * down before the [kotlinx.coroutines.CancellationException] propagates — see the `finally`
     * block below for why that matters.
     */
    @Throws(PBRemoteBearerError::class)
    override suspend fun open() {
        val active: CompletableDeferred<Unit>
        val closed: CompletableDeferred<Throwable>
        synchronized(stateLock) {
            if (isOpening) return
            isOpening = true
            closeCause = null
            outboundPduCount = 0u
            inboundPduCount = 0u
            pduChannel = Channel(capacity = Channel.UNLIMITED)
            active = CompletableDeferred()
            closed = CompletableDeferred()
            linkActive = active
            closeSignal = closed
        }

        // 감사 P2-N2: 취소(또는 transport 의 예기치 못한 예외, 예: CannotRelay)로 이 함수를
        // 벗어나면 finish() 를 한 번도 안 거친다. 그러면 ① `isOpening` 이 true 로 남아 다음
        // open() 이 조용히 no-op 이 되고 ② observer/supervisor 코루틴 2개가 leak 되며
        // ③ **Link Close 를 안 보내 서버 링크가 Link Open Timeout(최대 60 s) 동안 점유**되어
        // 다른 client 의 Link Open 이 LINK_CANNOT_OPEN 으로 거절된다.
        // 정상 종료와 이미 정리된 실패 경로에서는 아래 가드가 no-op 이다.
        var completed = false
        try {
            startObserving()
            // Teardown always runs here, never on the observer coroutine — see the class KDoc.
            // 핸들을 보관하지 않는 이유: 이 코루틴은 `closed` 가 완료되면 스스로 끝나고,
            // finish() 가 어느 경로로 불리든 반드시 `closed` 를 complete 하므로 (아래 finally 의
            // 취소 정리 포함) 남을 방법이 없다. 핸들을 두면 "cancel 해야 하나" 하는 오해만 생긴다.
            scope.launch { finish(error = closed.await()) }

            val status = transport.send(
                message = uuid
                    ?.let { RemoteProvisioningLinkOpen(uuid = it, timeout = linkOpenTimeout) }
                    ?: RemoteProvisioningLinkOpen(nppiProcedure = nppiProcedure!!),
                destination = server
            ) as? RemoteProvisioningLinkStatus

            if (status == null || !status.isSuccess) {
                logger?.e(LogCategory.BEARER) {
                    "Remote Provisioning Link Open failed: ${status?.status ?: "no response"}"
                }
                val error = PBRemoteBearerError.LinkCannotOpen(status = status?.status)
                finish(error = error)
                throw error
            }

            // A successful Link Open normally reports LINK_OPENING; LINK_ACTIVE is announced
            // later by a Link Report. NPPI procedures go straight to LINK_ACTIVE.
            if (status.linkState == RemoteProvisioningLinkState.LINK_ACTIVE) {
                active.complete(Unit)
            }

            // `active` is completed exceptionally by finish(), so a link that dies while opening
            // surfaces the real cause here rather than a bare timeout.
            val opened = withTimeoutOrNull(linkActiveTimeout) { active.await() }
            if (opened == null) {
                logger?.e(LogCategory.BEARER) {
                    "Timed out waiting for the Remote Provisioning link to become active"
                }
                closeQuietly()
                throw PBRemoteBearerError.LinkOpenTimeout()
            }

            synchronized(stateLock) {
                // 감사 P1-1: LINK_ACTIVE 바로 뒤에 IDLE 이 도착하면 teardown 이 먼저 끝나 있을 수
                // 있다. 그 상태에서 Opened 로 덮으면 `isOpen = true` + 이미 close 된 채널 =
                // 좀비 베어러가 되고, ProvisioningManager 는 provision() 을 시작한 뒤 첫 PDU
                // 에서야 실패한다.
                if (!isOpening) throw closeCause ?: BearerError.Closed()
                _state.value = BearerEvent.Opened
            }
            completed = true
            logger?.i(LogCategory.BEARER) { "Remote Provisioning link open (server: $server)" }
        } finally {
            // `isOpening` 이 아직 true 라는 것은 finish() 를 한 번도 안 거쳤다는 뜻 = 취소이거나
            // 예기치 못한 예외다. NonCancellable 로 감싸야 이미 취소된 코루틴에서도 Link Close 가
            // 실제로 나간다.
            if (!completed && isOpening) {
                withContext(NonCancellable) { closeQuietly() }
            }
        }
    }

    /**
     * Sends a Provisioning PDU to the unprovisioned device through the Remote Provisioning Server.
     *
     * Suspends until the server confirms delivery with a Remote Provisioning PDU Outbound Report.
     * On timeout the PDU is retransmitted once with the **same** outbound PDU number, which the
     * server tolerates: it answers a mismatching number with its own current count, so a duplicate
     * is acknowledged rather than desynchronising the link.
     *
     * @param pdu  The Provisioning PDU, starting with its type octet.
     * @param type Must be [PduType.PROVISIONING_PDU].
     * @throws BearerError.PduTypeNotSupported if [type] is not a provisioning PDU.
     * @throws BearerError.Closed if the bearer is not open.
     * @throws PBRemoteBearerError.LinkClosed if the server closed the link while sending.
     * @throws PBRemoteBearerError.OutboundPduTimeout if delivery was never confirmed.
     */
    @Throws(BearerError::class, PBRemoteBearerError::class)
    override suspend fun send(pdu: ByteArray, type: PduType) {
        if (!supports(type)) throw BearerError.PduTypeNotSupported()
        if (!isOpen) throw BearerError.Closed()

        sendMutex.withLock {
            outboundPduCount = (outboundPduCount + 1u).toUByte()
            val number = outboundPduCount
            val message = RemoteProvisioningPDUSend(
                outboundPduNumber = number,
                provisioningPdu = pdu
            )

            var attempt = 0
            var delivered = false
            while (attempt < 1 + OUTBOUND_PDU_RETRIES && !delivered) {
                if (!isOpen) throw closeCause ?: BearerError.Closed()
                val acknowledged = CompletableDeferred<Unit>()
                pendingOutbound = number to acknowledged
                try {
                    transport.send(message = message, destination = server)
                    // `acknowledged` may complete exceptionally if the link dies mid-send; that
                    // cause propagates out of send() instead of being reported as a timeout.
                    delivered =
                        withTimeoutOrNull(outboundPduTimeout) { acknowledged.await() } != null
                } finally {
                    pendingOutbound = null
                }
                if (!delivered) {
                    logger?.w(LogCategory.BEARER) {
                        "Remote Provisioning PDU #$number not acknowledged " +
                                "(attempt ${attempt + 1}/${1 + OUTBOUND_PDU_RETRIES})"
                    }
                }
                attempt++
            }

            if (!delivered) {
                closeQuietly()
                throw PBRemoteBearerError.OutboundPduTimeout(outboundPduNumber = number)
            }
        }
    }

    /**
     * Closes the Remote Provisioning link, reporting success.
     *
     * Use [close] with an explicit reason after a failed provisioning attempt so the server can
     * report the correct Link Close Reason to its own observers.
     */
    override suspend fun close() = close(reason = RemoteProvisioningLinkCloseReason.SUCCESS)

    /**
     * Closes the Remote Provisioning link with an explicit reason.
     *
     * The Link Close message is best effort: whatever the server answers, the bearer is considered
     * closed afterwards.
     *
     * ⚠️ The Link Close is sent inside [NonCancellable], so cancelling the caller while this runs
     * is delayed until the Link Status arrives or the acknowledged-message timeout in
     * `NetworkManager.awaitMeshMessageResponse` elapses. The delay is bounded, never indefinite.
     *
     * @param reason Link close reason. [RemoteProvisioningLinkCloseReason.UNRECOGNIZED] is
     *               normalised to `FAIL`, because the server drops any other value without
     *               responding.
     */
    suspend fun close(reason: RemoteProvisioningLinkCloseReason) {
        if (!isOpening) return
        sendLinkClose(reason = reason)
        finish(error = null)
    }

    private suspend fun startObserving() {
        val subscribed = CompletableDeferred<Unit>()
        observer = scope.launch {
            transport.events(onSubscribed = { subscribed.complete(Unit) })
                .collect { event ->
                    if (event.source != server) return@collect
                    when (val message = event.message) {
                        is RemoteProvisioningPDUReport -> onPduReport(message)
                        is RemoteProvisioningPDUOutboundReport -> onOutboundReport(message)
                        is RemoteProvisioningLinkReport -> onLinkReport(message)
                        else -> Unit
                    }
                }
        }
        subscribed.await()
    }

    private fun onPduReport(report: RemoteProvisioningPDUReport) {
        // The NCS Remote Provisioning Client discards a report whose Inbound PDU Count is not
        // greater than the last one seen. Segmented reports can be delivered twice when a Segment
        // Acknowledgment is lost, and replaying a provisioning PDU would desynchronise the
        // provisioning state machine.
        if (report.inboundPduNumber <= inboundPduCount) {
            logger?.w(LogCategory.BEARER) {
                "Duplicate Remote Provisioning PDU Report #${report.inboundPduNumber} ignored"
            }
            return
        }
        inboundPduCount = report.inboundPduNumber
        pduChannel.trySend(Pdu(data = report.provisioningPdu, type = PduType.PROVISIONING_PDU))
    }

    private fun onOutboundReport(report: RemoteProvisioningPDUOutboundReport) {
        val pending = pendingOutbound ?: return
        if (report.outboundPduNumber == pending.first) {
            pending.second.complete(Unit)
        } else {
            logger?.w(LogCategory.BEARER) {
                "Non-matching Remote Provisioning PDU Outbound Report " +
                        "(${report.outboundPduNumber}, expected ${pending.first})"
            }
        }
    }

    private fun onLinkReport(report: RemoteProvisioningLinkReport) {
        logger?.i(LogCategory.BEARER) { "$report" }
        when (report.linkState) {
            RemoteProvisioningLinkState.LINK_ACTIVE -> linkActive?.complete(Unit)

            RemoteProvisioningLinkState.IDLE,
            RemoteProvisioningLinkState.LINK_CLOSING ->
                // The observer must not tear down the bearer itself; it only raises the signal
                // that the supervisor coroutine awaits. See the class KDoc, "Concurrency model".
                closeSignal?.complete(PBRemoteBearerError.LinkClosed(report = report))

            else -> Unit
        }
    }

    /**
     * Closes the link after an internal failure, reporting `FAIL` as MshPRT 1.1 requires for an
     * abnormal termination. See [close] regarding the bounded cancellation delay.
     */
    private suspend fun closeQuietly() {
        sendLinkClose(reason = RemoteProvisioningLinkCloseReason.FAIL)
        finish(error = null)
    }

    private suspend fun sendLinkClose(reason: RemoteProvisioningLinkCloseReason) {
        withContext(NonCancellable) {
            runCatching {
                transport.send(
                    message = RemoteProvisioningLinkClose(reason = reason),
                    destination = server
                )
            }.onFailure {
                logger?.w(LogCategory.BEARER) { "Remote Provisioning Link Close failed: $it" }
            }
        }
    }

    /**
     * Tears the bearer down. Idempotent.
     *
     * ⚠️ **Must remain non-suspending.** Its last statement cancels the observer Job; a suspension
     * point after that would be a cancellation hazard the moment this is ever reached from a
     * coroutine related to it. The compiler is the enforcement mechanism.
     *
     * @param error Cause to report, or `null` for an orderly close.
     */
    private fun finish(error: Throwable?) {
        val job: Job?
        val signal: CompletableDeferred<Throwable>?
        val cause: Throwable
        synchronized(stateLock) {
            if (!isOpening) return
            cause = error ?: BearerError.Closed()
            closeCause = cause
            isOpening = false
            linkActive?.completeExceptionally(cause)
            linkActive = null
            // Waiters are completed exceptionally rather than cancelled: `CompletableDeferred
            // .cancel()` makes `await()` throw a plain CancellationException, which
            // `withTimeoutOrNull` does not catch and which would cancel the *caller's* coroutine
            // instead of surfacing an error.
            pendingOutbound?.second?.completeExceptionally(cause)
            pendingOutbound = null
            // Closing with the cause makes `ProvisioningManager`'s `bearer.pdus.first { … }`
            // throw that cause instead of a bare NoSuchElementException.
            pduChannel.close(cause = error)
            _state.value = BearerEvent.Closed(error = cause)
            job = observer
            observer = null
            // 감사 P2-N1: `observer` 와 대칭으로 lock 안에서 캡처 + null 해야 한다. lock 밖에서
            // 필드를 다시 읽으면, finish() 와 재-open() 이 겹칠 때 **다음 세션의** closeSignal 을
            // 이전 세션의 cause 로 complete 해버려 새 링크가 열리자마자 teardown 된다.
            signal = closeSignal
            closeSignal = null
        }
        // Wakes a supervisor that is still awaiting, so it finishes instead of lingering. When
        // finish() *is* the supervisor's own body this is a no-op on an already-completed signal.
        signal?.complete(cause)
        // Always a different coroutine than the caller — see the class KDoc.
        job?.cancel()
    }

    companion object {

        /** Default time to wait for the Link Report announcing an active link. */
        val DEFAULT_LINK_ACTIVE_TIMEOUT: Duration = 30.seconds

        /**
         * Default time to wait for a PDU Outbound Report. Matches the 15 s used by the iOS
         * implementation; the NCS client uses a far shorter 2 s but retries internally.
         */
        val DEFAULT_OUTBOUND_PDU_TIMEOUT: Duration = 15.seconds

        /** Number of retransmissions of an unacknowledged PDU Send, as in the iOS bearer. */
        const val OUTBOUND_PDU_RETRIES = 1
    }
}
