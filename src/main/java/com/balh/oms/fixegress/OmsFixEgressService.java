package com.balh.oms.fixegress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Phase 3 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: the FIX-egress
 * role.
 *
 * <p><strong>Slice 3b-1 scope.</strong> Adds the replay infrastructure: connects {@code Aeron}
 * + {@code AeronArchive} against the cluster member's media driver + archive, polls
 * {@link AeronArchive#listRecordingsForUri} for the events recording, opens a replay
 * subscription from the persisted cursor (with {@link #clampToRecording} +
 * recording-recreation-rewind, mirroring {@link com.balh.oms.projector.OmsPostgresProjector}),
 * decodes each {@link OrderAdmittedEvent}, and advances the
 * {@code oms_fix_egress_cursor} after each fragment. <em>Slice 3b-1 does not send
 * {@code NewOrderSingle}</em>: that lands in slice 3b-2 together with the legacy
 * {@code FixOutboundDispatchWorker} exclusion + the restart-from-cursor IT against a broker
 * mock. Splitting 3b in two keeps each diff reviewable and lets the replay infrastructure land
 * before any FIX wire side-effect goes through it.
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li>{@link #init()}: read cursor, log resume position, start the replay thread.</li>
 *   <li>Replay thread: connect Aeron + AeronArchive, locate the recording for the events
 *       stream, open a replay subscription from the cursor position, poll-decode-advance until
 *       interrupted.</li>
 *   <li>{@link #close()}: signal shutdown, join the thread, close the Aeron stack in reverse.
 *       </li>
 * </ol>
 *
 * <h2>Failure handling</h2>
 *
 * <p>If the recording does not yet exist (cluster startup race), the thread polls every
 * {@link OmsConfig.Cluster.FixEgress#getRecordingLookupParkMs()} ms until it appears. If the
 * persisted cursor falls outside the current recording's range (recording recreated across
 * cluster incarnations), {@link #clampToRecording} rewinds to {@code startPosition} and
 * {@link OmsFixEgressCursorRepository#reset} persists the rewind. If Aeron rejects a replay
 * position as not frame-aligned, {@link #openReplay} retries from {@code startPosition}.
 * Postgres write failures bubble up as runtime exceptions and stop the egress loop —
 * operators must see them in logs and fix the connectivity / schema issue rather than
 * silently skipping events.
 */
@Component
@Profile(OmsProfiles.FIX_EGRESS)
@ConditionalOnProperty(prefix = "oms.cluster.fix-egress", name = "enabled", havingValue = "true")
public class OmsFixEgressService {

    private static final Logger log = LoggerFactory.getLogger(OmsFixEgressService.class);

    public static final String EGRESS_ID = "oms-fix-egress-default";

    private final OmsConfig config;
    private final OmsFixEgressCursorRepository cursorRepository;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Long> lastAppliedPosition = new AtomicReference<>(0L);
    private Thread replayThread;

    public OmsFixEgressService(OmsConfig config, OmsFixEgressCursorRepository cursorRepository) {
        this.config = config;
        this.cursorRepository = cursorRepository;
    }

    @PostConstruct
    void init() {
        long resumePos = cursorRepository
                .findLastAppliedPosition(EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID)
                .orElse(0L);
        lastAppliedPosition.set(resumePos);
        log.info(
                "oms-fix-egress starting (slice 3b-1 — replay infrastructure only, no NOS send yet);"
                        + " resuming from log position {} (egressId={}, streamId={})",
                resumePos,
                EGRESS_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
        running.set(true);
        replayThread = new Thread(this::replayLoop, "oms-fix-egress-replay");
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
     * Visible for tests. Latest log position the egress has applied (cursor-advanced) in this
     * JVM. Updated atomically with each fragment; survives JVM restart via the persisted
     * cursor.
     */
    public long lastAppliedPosition() {
        return lastAppliedPosition.get();
    }

    private void replayLoop() {
        OmsConfig.Cluster.FixEgress cfg = config.getCluster().getFixEgress();
        if (cfg.getAeronDirectory().isBlank()) {
            // No cluster wiring configured. Common in Spring context-only tests that boot the
            // egress profile to verify bean topology without standing up Aeron. Production
            // must always set OMS_FIX_EGRESS_AERON_DIR; the topology validator covers the
            // grpc-off / cluster-client-off invariants but does not require an aeron-directory
            // until slice 3b-2 wires the actual FIX send (a no-op replay loop on an unset
            // aeron-directory is the safer default for context-only ITs today).
            log.warn(
                    "oms-fix-egress replay loop skipped: oms.cluster.fix-egress.aeron-directory is empty");
            return;
        }
        Aeron aeron = null;
        AeronArchive archive = null;
        Subscription replay = null;
        try {
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cfg.getAeronDirectory()));
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel(cfg.getArchiveControlRequestChannel())
                    .controlResponseChannel(cfg.getArchiveControlResponseChannel()));

            RecordingDescriptor descriptor = waitForRecording(archive, cfg.getRecordingLookupParkMs());
            if (descriptor == null) {
                return;
            }

            long persistedPos = lastAppliedPosition.get();
            long requestedPos = clampToRecording(archive, descriptor, persistedPos);
            replay = openReplay(archive, descriptor, requestedPos, cfg);
            log.info(
                    "oms-fix-egress replay open; recordingId={} startPos={} (recordingStart={}, recordingStop={}) channel={} streamId={}",
                    descriptor.recordingId(),
                    lastAppliedPosition.get(),
                    descriptor.startPosition(),
                    descriptor.stopPosition(),
                    cfg.getReplayChannel(),
                    cfg.getReplayStreamId());

            FragmentHandler handler = new EgressFragmentHandler();
            while (running.get()) {
                int polled = replay.poll(handler, cfg.getFragmentLimit());
                if (polled == 0) {
                    LockSupport.parkNanos(cfg.getPollParkNanos());
                }
            }
        } catch (RuntimeException e) {
            log.error("oms-fix-egress replay loop terminating", e);
        } finally {
            CloseHelper.quietClose(replay);
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
            log.info("oms-fix-egress replay loop stopped");
        }
    }

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
     * <p>Mirrors {@link com.balh.oms.projector.OmsPostgresProjector}'s {@code openReplay} —
     * frame boundaries depend on the term layout established when the recording was created
     * (initial term id, term length, mtu). Across cluster restarts or archive recreation the
     * same numerical position may fall mid-frame in the new recording even if it was a clean
     * boundary in the old one. The safe response is "rewind to startPosition and replay"; for
     * slice 3b-1 the side-effect is just a cursor advance (idempotent), so a rewind is purely
     * a re-traversal cost. Slice 3b-2's {@code Session.sendToTarget} side effect changes that
     * trade-off — see plan slice 3b-2 for the dedupe options.
     */
    private Subscription openReplay(
            AeronArchive archive,
            RecordingDescriptor descriptor,
            long requestedPos,
            OmsConfig.Cluster.FixEgress cfg) {
        long currentPos = requestedPos;
        try {
            Subscription sub = archive.replay(
                    descriptor.recordingId(),
                    currentPos,
                    /* length = */ Long.MAX_VALUE,
                    cfg.getReplayChannel(),
                    cfg.getReplayStreamId());
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
            cursorRepository.reset(EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, currentPos);
            lastAppliedPosition.set(currentPos);
            return archive.replay(
                    descriptor.recordingId(),
                    currentPos,
                    /* length = */ Long.MAX_VALUE,
                    cfg.getReplayChannel(),
                    cfg.getReplayStreamId());
        }
    }

    private void persistResolvedStartPos(long pos) {
        if (pos != lastAppliedPosition.get()) {
            cursorRepository.reset(EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, pos);
            lastAppliedPosition.set(pos);
        }
    }

    /**
     * Aeron Archive replay requires the start position to be a frame boundary inside the
     * recording's available range. A persisted cursor can fall outside that range when the
     * recording is recreated (test JVM reboot, prod node disk wipe, manual archive deletion).
     * Mirrors {@link com.balh.oms.projector.OmsPostgresProjector}'s {@code clampToRecording}:
     * we clamp to the recording's {@code startPosition}; for an active recording (still being
     * written, {@code stopPosition == NULL_POSITION}), the upper bound is
     * {@link AeronArchive#getRecordingPosition(long)}.
     */
    private long clampToRecording(AeronArchive archive, RecordingDescriptor descriptor, long cursorPos) {
        long start = descriptor.startPosition();
        if (cursorPos < start) {
            log.warn(
                    "Egress cursor {} below recording startPosition {}; resuming from {}.",
                    cursorPos,
                    start,
                    start);
            return start;
        }
        long upperBound;
        long stop = descriptor.stopPosition();
        if (stop == AeronArchive.NULL_POSITION) {
            upperBound = archive.getRecordingPosition(descriptor.recordingId());
            if (upperBound == AeronArchive.NULL_POSITION) {
                upperBound = start;
            }
        } else {
            upperBound = stop;
        }
        if (cursorPos > upperBound) {
            log.warn(
                    "Egress cursor {} above recording upper-bound {} (likely stale cursor from a previous"
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
     * {@link AeronArchive#listRecordingsForUri}. Used to clamp a stale cursor to a valid
     * replay range.
     */
    private record RecordingDescriptor(long recordingId, long startPosition, long stopPosition) {}

    /**
     * Decodes one fragment and (slice 3b-1) advances the cursor.
     * {@link io.aeron.logbuffer.Header#position()} is the cluster log position <em>after</em>
     * this fragment.
     *
     * <p>Slice 3b-2 will extend this handler with a {@link FixNewOrderSingleBuilder}-like step
     * + {@link com.balh.oms.fix.FixOutboundSessionSend#send} call before the cursor advance.
     * Crash semantics in slice 3b-1 are simple: if the JVM dies after the cursor SQL UPDATE
     * but before {@link #lastAppliedPosition} updates, restart resumes from the persisted
     * cursor and re-decodes the same fragment (no side effect to repeat in 3b-1; {@code
     * advance} is idempotent on the monotonic guard).
     */
    private final class EgressFragmentHandler implements FragmentHandler {

        @Override
        public void onFragment(
                org.agrona.DirectBuffer buffer,
                int offset,
                int length,
                io.aeron.logbuffer.Header header) {
            int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
            if (typeId != OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED) {
                // Phase 3 only knows about admission events from this recording today.
                // Slice 3c adds ApplyExecutionReport / ExecutionAppliedEvent typeIds that the
                // egress service will (slice 3d) write back via the cluster ingress client.
                return;
            }
            OrderAdmittedEvent ev = OrderAdmittedEvent.decode(buffer, offset, length);
            long newPosition = header.position();
            applyAdmittedEvent(ev, newPosition);
            lastAppliedPosition.set(newPosition);
        }
    }

    /**
     * Single-fragment apply for one {@link OrderAdmittedEvent}. Package-private so tests can
     * drive replay from a synthesised event without standing up Aeron when that is wasteful
     * (live ITs in {@code OmsFixEgressIT} still drive the full path through the cluster).
     *
     * <p>Slice 3b-1: cursor advance only. Slice 3b-2 builds the {@code NewOrderSingle},
     * calls {@link com.balh.oms.fix.FixOutboundSessionSend#send}, and only advances the
     * cursor on a successful send.
     */
    void applyAdmittedEvent(OrderAdmittedEvent ev, long newPosition) {
        cursorRepository.advance(EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, newPosition);
    }
}
