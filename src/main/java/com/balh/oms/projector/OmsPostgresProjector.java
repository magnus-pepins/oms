package com.balh.oms.projector;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.persistence.OrdersRepository;
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

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Phase 2 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: subscribes to the
 * cluster's events recording via Aeron Archive replay and writes Postgres projection rows.
 *
 * <p><strong>Slice 2b-2 scope.</strong> Reads {@link OrderAdmittedEvent}s from the recording on
 * {@link OmsClusterWireFormat#EVENTS_CHANNEL} / {@link OmsClusterWireFormat#EVENTS_STREAM_ID},
 * idempotently inserts {@code orders} rows via {@link OrdersRepository#insertFromAdmittedEvent}, and
 * advances the {@code aeron_projector_cursor} cursor monotonically. Slice 2c removes the parallel
 * write from {@link com.balh.oms.ingress.OrderIngressService}; today both writers race and the row
 * is whichever lands first (idempotent ON CONFLICT).
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

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Long> lastAppliedPosition = new AtomicReference<>(0L);
    private Thread replayThread;

    public OmsPostgresProjector(
            OmsConfig config,
            AeronProjectorCursorRepository cursorRepository,
            OrdersRepository ordersRepository) {
        this.config = config;
        this.cursorRepository = cursorRepository;
        this.ordersRepository = ordersRepository;
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
     * Decodes and applies a single fragment. Records the position the cluster log advanced to AFTER this
     * fragment ({@link io.aeron.logbuffer.Header#position()}). The cursor is advanced inside the same
     * call; cursor + Postgres-row write are not yet wrapped in a single JDBC transaction (slice 2c does
     * that) — for slice 2b-2 the orders insert is already idempotent so re-applying after a crash before
     * cursor advance is safe.
     */
    private final class ProjectingFragmentHandler implements FragmentHandler {

        @Override
        public void onFragment(org.agrona.DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
            int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
            if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED) {
                // Phase 2 only knows about admission events. Future event types (executions, control
                // decisions) decode here in slices 2d+.
                return;
            }
            OrderAdmittedEvent ev = OrderAdmittedEvent.decode(buffer, offset, length);
            ordersRepository.insertFromAdmittedEvent(ev);
            long newPosition = header.position();
            cursorRepository.advance(PROJECTOR_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, newPosition);
            lastAppliedPosition.set(newPosition);
        }
    }
}
