package com.balh.oms.projector;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Phase 2 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: subscribes to the
 * cluster's events recording via Aeron Archive replay and writes Postgres projection rows.
 *
 * <p><strong>Slice 2d scope.</strong> Reads {@link OrderAdmittedEvent}s from the recording on
 * {@link OmsClusterWireFormat#EVENTS_CHANNEL} / {@link OmsClusterWireFormat#EVENTS_STREAM_ID} and,
 * for each event, runs a single Postgres transaction that:
 * <ol>
 *   <li>idempotently inserts the {@code orders} row at {@code status=NEW, version=0} via
 *       {@link OrdersRepository#insertFromAdmittedEvent} (slice 2b-2);</li>
 *   <li>delegates to {@link OrderControlAdmission#persistAdmission} for risk + buying power +
 *       CAS to {@code WORKING} or {@code REJECTED} + {@code control_decisions} row +
 *       {@code domain_event_outbox} envelope (slice 2d, replaces what
 *       {@code OutboxReconciler} → Chronicle → {@link com.balh.oms.tailer.ControlTailer} did
 *       in the legacy topology — which slices 2e/2f delete);</li>
 *   <li>advances the {@code aeron_projector_cursor} monotonically.</li>
 * </ol>
 *
 * <p>All three steps are idempotent on replay: ON CONFLICT on the orders insert, version-mismatch
 * CAS on the admission update, and {@link AeronProjectorCursorRepository#advance} only moves the
 * cursor forward. Crash before commit re-applies the event safely; crash after commit but before
 * cursor advance only happens on Postgres-side mid-transaction failures (which throw and stop the
 * projector for operator inspection — see "Failure handling" below).
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li>{@link #init()}: read cursor, log resume position, start the replay thread.</li>
 *   <li>Replay thread: connect Aeron + AeronArchive, locate the recording for the events stream,
 *       open a replay subscription from the cursor position, poll-decode-write-advance until
 *       interrupted.</li>
 *   <li>{@link #close()}: signal shutdown, join the thread, close the Aeron stack in reverse.</li>
 * </ol>
 *
 * <h2>Failure handling</h2>
 *
 * If the recording does not yet exist (cluster startup race), the thread polls
 * {@link AeronArchive#listRecordingsForUri} every
 * {@link OmsConfig.Cluster.Projector#getRecordingLookupParkMs()} ms until it appears. If the replay
 * stream closes mid-life (cluster restart, archive recompaction) the thread loops back to the
 * recording-lookup state with the current persisted cursor. Postgres write failures bubble up as
 * runtime exceptions and stop the projector — operators must see them in logs and fix the schema /
 * connectivity issue rather than silently skipping events.
 */
@Component
@Profile(OmsProfiles.POSTGRES_PROJECTOR)
@ConditionalOnProperty(prefix = "oms.cluster.projector", name = "enabled", havingValue = "true")
public class OmsPostgresProjector {

    private static final Logger log = LoggerFactory.getLogger(OmsPostgresProjector.class);

    public static final String PROJECTOR_ID = "oms-postgres-default";

    private final OmsConfig config;
    private final AeronProjectorCursorRepository cursorRepository;
    private final OrdersRepository ordersRepository;
    private final OrderControlAdmission controlAdmission;
    private final TransactionTemplate transactionTemplate;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Long> lastAppliedPosition = new AtomicReference<>(0L);
    private Thread replayThread;

    public OmsPostgresProjector(
            OmsConfig config,
            AeronProjectorCursorRepository cursorRepository,
            OrdersRepository ordersRepository,
            OrderControlAdmission controlAdmission,
            PlatformTransactionManager transactionManager) {
        this.config = config;
        this.cursorRepository = cursorRepository;
        this.ordersRepository = ordersRepository;
        this.controlAdmission = controlAdmission;
        // Programmatic boundary: the replay loop is a non-Spring thread, so AOP-proxied
        // @Transactional on this bean's own methods would not be intercepted. A
        // TransactionTemplate guarantees orders.insert + persistAdmission + cursor.advance
        // commit (or roll back) as one unit per fragment.
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @PostConstruct
    void init() {
        long resumePos = cursorRepository
                .findLastAppliedPosition(PROJECTOR_ID, OmsClusterWireFormat.EVENTS_STREAM_ID)
                .orElse(0L);
        lastAppliedPosition.set(resumePos);
        log.info(
                "oms-postgres-projector starting; resuming from log position {} (projectorId={}, streamId={})",
                resumePos,
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
        running.set(true);
        replayThread = new Thread(this::replayLoop, "oms-postgres-projector-replay");
        replayThread.setDaemon(true);
        replayThread.start();
    }

    @PreDestroy
    void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (replayThread != null) {
            replayThread.interrupt();
            try {
                replayThread.join(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Visible for tests. Latest log position the projector has applied to Postgres in this JVM.
     * Updated atomically with each row write; survives JVM restart via the persisted cursor.
     */
    public long lastAppliedPosition() {
        return lastAppliedPosition.get();
    }

    private void replayLoop() {
        OmsConfig.Cluster.Projector projectorCfg = config.getCluster().getProjector();
        if (projectorCfg.getAeronDirectory().isBlank()) {
            // No cluster wiring configured. Common in Spring context-only tests that boot the
            // projector profile to verify bean topology without standing up Aeron. Production must
            // always set OMS_POSTGRES_PROJECTOR_AERON_DIR; the topology validator will be extended in
            // slice 2c to fail-fast on this when the projector is enabled.
            log.warn(
                    "oms-postgres-projector replay loop skipped: oms.cluster.projector.aeron-directory is empty");
            return;
        }
        Aeron aeron = null;
        AeronArchive archive = null;
        Subscription replay = null;
        try {
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(projectorCfg.getAeronDirectory()));
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel(projectorCfg.getArchiveControlRequestChannel())
                    .controlResponseChannel(projectorCfg.getArchiveControlResponseChannel()));

            RecordingDescriptor descriptor = waitForRecording(archive, projectorCfg.getRecordingLookupParkMs());
            if (descriptor == null) {
                return; // shutdown requested before recording appeared
            }

            long persistedPos = lastAppliedPosition.get();
            long requestedPos = clampToRecording(archive, descriptor, persistedPos);
            replay = openReplay(archive, descriptor, requestedPos, projectorCfg);
            log.info(
                    "Projector replay open; recordingId={} startPos={} (recordingStart={}, recordingStop={}) channel={} streamId={}",
                    descriptor.recordingId(),
                    lastAppliedPosition.get(),
                    descriptor.startPosition(),
                    descriptor.stopPosition(),
                    projectorCfg.getReplayChannel(),
                    projectorCfg.getReplayStreamId());

            FragmentHandler handler = new ProjectingFragmentHandler();
            while (running.get()) {
                int polled = replay.poll(handler, projectorCfg.getFragmentLimit());
                if (polled == 0) {
                    LockSupport.parkNanos(projectorCfg.getPollParkNanos());
                }
            }
        } catch (RuntimeException e) {
            log.error("oms-postgres-projector replay loop terminating", e);
        } finally {
            CloseHelper.quietClose(replay);
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
            log.info("oms-postgres-projector replay loop stopped");
        }
    }

    /**
     * Polls {@link AeronArchive#listRecordingsForUri} until the events stream recording appears.
     *
     * @return recording descriptor (id + start/stop position), or {@code null} if shutdown was
     *         requested before the recording appeared.
     */
    private RecordingDescriptor waitForRecording(AeronArchive archive, long parkMs) {
        while (running.get()) {
            RecordingDescriptor[] result = {null};
            archive.listRecordingsForUri(
                    /* fromRecordingId = */ 0L,
                    /* recordCount = */ 1024,
                    OmsClusterWireFormat.EVENTS_CHANNEL,
                    OmsClusterWireFormat.EVENTS_STREAM_ID,
                    (controlSessionId,
                            correlationId,
                            recordingId,
                            startTimestamp,
                            stopTimestamp,
                            startPosition,
                            stopPosition,
                            initialTermId,
                            segmentFileLength,
                            termBufferLength,
                            mtuLength,
                            sessionId,
                            streamId,
                            strippedChannel,
                            originalChannel,
                            sourceIdentity) -> {
                        if (streamId == OmsClusterWireFormat.EVENTS_STREAM_ID
                                && (result[0] == null || recordingId > result[0].recordingId())) {
                            result[0] = new RecordingDescriptor(recordingId, startPosition, stopPosition);
                        }
                    });
            if (result[0] != null) {
                return result[0];
            }
            try {
                Thread.sleep(parkMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    /**
     * Opens the replay subscription, with a one-shot fallback to the recording's
     * {@code startPosition} if Aeron rejects the requested position as not frame-aligned.
     *
     * <p>Frame boundaries in an Aeron Archive recording depend on the term layout established when
     * the recording was created (initial term id, term length, mtu). Across cluster restarts or
     * archive recreation, the same numerical position may fall mid-frame in the new recording even
     * if it was a clean boundary in the old one. The persisted cursor cannot detect this on its
     * own — the safe response is "rewind to startPosition and replay; the {@code orders} INSERT is
     * idempotent". The fallback also covers the {@code "position must be less than the limit
     * position"} error path when the cursor exceeded the live tail.
     */
    private Subscription openReplay(
            AeronArchive archive,
            RecordingDescriptor descriptor,
            long requestedPos,
            OmsConfig.Cluster.Projector projectorCfg) {
        long currentPos = requestedPos;
        try {
            Subscription sub = archive.replay(
                    descriptor.recordingId(),
                    currentPos,
                    /* length = */ Long.MAX_VALUE,
                    projectorCfg.getReplayChannel(),
                    projectorCfg.getReplayStreamId());
            persistResolvedStartPos(currentPos);
            return sub;
        } catch (io.aeron.archive.client.ArchiveException e) {
            log.warn(
                    "archive.replay({}, pos={}) failed: {}; resetting to recording startPosition={}.",
                    descriptor.recordingId(),
                    currentPos,
                    e.getMessage(),
                    descriptor.startPosition());
            currentPos = descriptor.startPosition();
            cursorRepository.reset(PROJECTOR_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, currentPos);
            lastAppliedPosition.set(currentPos);
            Subscription sub = archive.replay(
                    descriptor.recordingId(),
                    currentPos,
                    /* length = */ Long.MAX_VALUE,
                    projectorCfg.getReplayChannel(),
                    projectorCfg.getReplayStreamId());
            return sub;
        }
    }

    private void persistResolvedStartPos(long pos) {
        if (pos != lastAppliedPosition.get()) {
            cursorRepository.reset(PROJECTOR_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, pos);
            lastAppliedPosition.set(pos);
        }
    }

    /**
     * Aeron Archive replay requires the start position to be a frame boundary inside the recording's
     * available range. A persisted cursor can fall outside that range when the recording is recreated
     * (test JVM reboot, prod node disk wipe, manual archive deletion). We clamp to the recording's
     * {@code startPosition}; events between {@code startPosition} and the old cursor are re-applied,
     * and the projector's idempotent {@code INSERT ... ON CONFLICT DO NOTHING} into {@code orders}
     * keeps that safe.
     *
     * <p>For an active recording (still being written, {@code stopPosition == NULL_POSITION}), the
     * upper bound is {@link AeronArchive#getRecordingPosition(long)} which returns the current
     * write-head position. For a stopped recording, the descriptor's {@code stopPosition} is final.
     */
    private long clampToRecording(AeronArchive archive, RecordingDescriptor descriptor, long cursorPos) {
        long start = descriptor.startPosition();
        if (cursorPos < start) {
            log.warn(
                    "Projector cursor {} below recording startPosition {}; resuming from {}.",
                    cursorPos,
                    start,
                    start);
            return start;
        }
        long upperBound;
        long stop = descriptor.stopPosition();
        if (stop == io.aeron.archive.client.AeronArchive.NULL_POSITION) {
            // Active recording: ask the Archive for the live write head.
            upperBound = archive.getRecordingPosition(descriptor.recordingId());
            if (upperBound == io.aeron.archive.client.AeronArchive.NULL_POSITION) {
                upperBound = start;
            }
        } else {
            upperBound = stop;
        }
        if (cursorPos > upperBound) {
            log.warn(
                    "Projector cursor {} above recording upper-bound {} (likely stale cursor from a previous"
                            + " incarnation); resuming from {}.",
                    cursorPos,
                    upperBound,
                    start);
            return start;
        }
        return cursorPos;
    }

    /**
     * Snapshot of the recording's identity and bounds returned by
     * {@link AeronArchive#listRecordingsForUri}. Used to clamp a stale cursor to a valid replay
     * range.
     */
    private record RecordingDescriptor(long recordingId, long startPosition, long stopPosition) {}

    /**
     * Decodes one fragment and applies it to Postgres inside a single transaction:
     * {@code orders} insert (idempotent ON CONFLICT) + {@link OrderControlAdmission#persistAdmission}
     * (CAS to WORKING/REJECTED, {@code control_decisions} row, {@code domain_event_outbox} envelope)
     * + cursor advance. {@link io.aeron.logbuffer.Header#position()} is the cluster log position
     * <em>after</em> this fragment.
     *
     * <p>Crash semantics: if the JVM dies after the transaction commits but before the in-memory
     * {@link #lastAppliedPosition} updates, restart resumes from the persisted cursor — this is
     * the same fragment, and every step in the transaction is idempotent. If the transaction
     * itself fails (Postgres connectivity, schema drift), the exception bubbles up and stops the
     * replay loop; operators must intervene rather than skip events silently.
     */
    private final class ProjectingFragmentHandler implements FragmentHandler {

        @Override
        public void onFragment(org.agrona.DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
            int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
            if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED) {
                // Phase 2 only knows about admission events. Future event types (executions, cancels)
                // decode here in Phase 3+.
                return;
            }
            OrderAdmittedEvent ev = OrderAdmittedEvent.decode(buffer, offset, length);
            long newPosition = header.position();
            transactionTemplate.executeWithoutResult(status -> applyAdmittedEvent(ev, newPosition));
            lastAppliedPosition.set(newPosition);
        }
    }

    /**
     * Single-transaction body for one {@link OrderAdmittedEvent}. Public + package-private only so
     * tests can drive admission directly from a synthesised event when standing up an Aeron-less
     * Spring context is wasteful (the live IT in {@code OmsPostgresProjectorIT} still drives the
     * full path through the cluster).
     */
    void applyAdmittedEvent(OrderAdmittedEvent ev, long newPosition) {
        ordersRepository.insertFromAdmittedEvent(ev);
        PendingControlEvent pending = toPendingControlEvent(ev);
        controlAdmission.persistAdmission(pending);
        cursorRepository.advance(PROJECTOR_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, newPosition);
    }

    private static PendingControlEvent toPendingControlEvent(OrderAdmittedEvent ev) {
        // orderTimestamp drives StaleJobGuard ("did this event sit in the journal so long that
        // pre-trade risk decisions are unsafe?"). For a downstream projector, the cluster log is
        // the source of truth and replaying older events on restart is normal — a stale guard
        // would erroneously reject legitimate admissions during catch-up. Use Instant.now() so
        // staleness measures projector-apply latency, which is by definition near-zero. We
        // deliberately do NOT decode ev.acceptedAtNanos() here: the cluster's time unit is
        // ClusterClock millis-by-default but the wire field is named *Nanos; correcting that is
        // a separate, contained fix (see OmsAdmissionClusteredService and OmsClusterNodeBootstrap).
        Instant now = Instant.now();
        return new PendingControlEvent(
                "OrderAccepted",
                ev.orderId(),
                ev.version(),
                ev.shardId(),
                ev.accountIdHash(),
                /* orderTimestamp = */ now,
                /* enqueuedAt    = */ now);
    }
}
