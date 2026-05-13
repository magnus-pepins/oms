package com.balh.oms.fixegress;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fix.FixNewOrderSingleBuilder;
import com.balh.oms.fix.FixOutboundSessionSend;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.SessionNotFound;
import quickfix.fix44.NewOrderSingle;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Phase 3 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: the FIX-egress
 * role.
 *
 * <p><strong>Slice 3b-2 scope.</strong> Folds the FIX wire side effect into the slice 3b-1 replay
 * loop: each {@link OrderAdmittedEvent} fragment is decoded, translated to FIX
 * {@link NewOrderSingle} via {@link FixNewOrderSingleBuilder#build(OrderAdmittedEvent)}, sent
 * through {@link FixOutboundSessionSend#send(quickfix.Message)}, and only then advances the
 * {@code oms_fix_egress_cursor}. The hot send path is {@code AeronArchive.replay} → builder →
 * {@code Session.sendToTarget}: no Postgres lookup, no in-memory queue.
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li>{@link #init()}: read cursor, log resume position, start the replay thread.</li>
 *   <li>Replay thread: connect Aeron + AeronArchive, locate the recording for the events
 *       stream, open a replay subscription from the cursor position, poll-decode-send-advance
 *       until interrupted.</li>
 *   <li>{@link #close()}: signal shutdown, join the thread, close the Aeron stack in reverse.
 *       </li>
 * </ol>
 *
 * <h2>Deduplication semantics (option 1: cursor-only, "at-least-once at broker")</h2>
 *
 * <p>The cursor is advanced <strong>after</strong> {@code Session.sendToTarget} returns, in a
 * separate Postgres write. If the JVM dies between the FIX send and the cursor SQL UPDATE, the
 * persisted cursor still points at the byte boundary of the previously-sent fragment; on
 * restart the replay loop redelivers <em>this</em> fragment and we send a duplicate
 * {@code NewOrderSingle} for the same {@code orderId}. The broker then rejects on
 * {@code DupClOrdID} per FIX 4.4 spec — an operationally noisy outcome, but message-loss-free
 * (no NOS is ever silently dropped) and exactly-once on the projector side because
 * {@code OrderAdmittedEvent} is emitted only once per fresh admission.
 *
 * <p>The chosen trade-off matches the user-facing latency budget: option 1 lands one Postgres
 * UPDATE per cursor advance and zero extra reads on the hot path. The discarded alternatives
 * are documented here so the decision is reversible without re-deriving the trade-offs:
 *
 * <ul>
 *   <li><strong>Option 2 — sent-set table (read-before-send dedupe).</strong> Persist
 *       {@code (orderId, version)} keys after each successful send and probe before sending. Adds
 *       one Postgres SELECT per fragment (the path becomes Aeron→Postgres→FIX→Postgres) and
 *       gives exactly-once at the broker even after a crash mid-window. Worth revisiting if
 *       broker-side duplicate alerts become operationally expensive (e.g. a venue that flags
 *       suspect-duplicate dropping the session) or if we move to a venue that does not honour
 *       {@code DupClOrdID}. Re-uses {@code oms_fix_egress_cursor} unchanged; adds a sibling
 *       {@code oms_fix_egress_sent_orders(order_id, version, sent_at)} table.</li>
 *   <li><strong>Option 3 — two-phase commit (XA across Postgres + QuickFIX message store).</strong>
 *       Strongest correctness (no broker-side duplicates ever) but highest latency and most
 *       moving parts: requires QuickFIX/J's JDBC store, an XA-capable Postgres pool, and bumps
 *       the per-fragment cost from one async UPDATE to a coordinated two-phase commit. Reserved
 *       for a hypothetical regulatory regime that prohibits {@code DupClOrdID} resends; not on
 *       the OMS roadmap as of slice 3b-2.</li>
 * </ul>
 *
 * <p>Option choice landed in slice 3b-2 as a documented decision; flipping to options 2 or 3
 * is a future-slice change with a single diff (add the dedupe write before
 * {@link #applyAdmittedEvent}'s cursor advance).
 *
 * <h2>FIX session reconnection</h2>
 *
 * <p>{@link FixOutboundSessionSend#send} throws {@link SessionNotFound} when the QuickFIX
 * initiator is mid-reconnect (TCP gap, broker logout). In that case we park
 * {@link OmsConfig.Cluster.FixEgress#getSessionNotReadyParkNanos()} ns and retry the same
 * fragment without advancing the cursor — Aeron has already delivered the fragment to the
 * fragment handler, so retrying inside {@link #applyAdmittedEvent} keeps the message in flight
 * without forcing a replay-loop teardown. If the running flag flips false during the wait, we
 * exit without advancing; restart redelivers the fragment from the persisted cursor (one
 * fragment behind the failed send, since slice 3b-1's cursor advance is post-send).
 *
 * <h2>Failure handling (replay infrastructure, unchanged from slice 3b-1)</h2>
 *
 * <p>If the recording does not yet exist (cluster startup race), the thread polls every
 * {@link OmsConfig.Cluster.FixEgress#getRecordingLookupParkMs()} ms until it appears. If the
 * persisted cursor falls outside the current recording's range (recording recreated across
 * cluster incarnations), {@link #clampToRecording} rewinds to {@code startPosition} and
 * {@link OmsFixEgressCursorRepository#reset} persists the rewind. If Aeron rejects a replay
 * position as not frame-aligned, {@link #openReplay} retries from {@code startPosition}.
 *
 * <h2>Optional FIX dependencies</h2>
 *
 * <p>{@link FixNewOrderSingleBuilder} and {@link FixOutboundSessionSend} are both gated on
 * {@code oms.routing.backend=fix}. They are autowired with {@code required = false} so this
 * service still loads in context-only ITs that set {@code oms.routing.backend=noop} (e.g.
 * {@code OmsFixEgressApplicationIT}, {@code OmsFixEgressReplayIT}). When either is absent the
 * loop runs in slice-3b-1 mode (cursor advance only) — that path is exercised by the slice 3b-1
 * IT and stays as a safety hatch for tests that want to validate the replay infrastructure
 * without standing up QuickFIX. Production deployments always set {@code routing.backend=fix}
 * so both beans load.
 */
@Component
@Profile(OmsProfiles.FIX_EGRESS)
@ConditionalOnProperty(prefix = "oms.cluster.fix-egress", name = "enabled", havingValue = "true")
public class OmsFixEgressService {

    private static final Logger log = LoggerFactory.getLogger(OmsFixEgressService.class);

    public static final String EGRESS_ID = "oms-fix-egress-default";

    private final OmsConfig config;
    private final OmsFixEgressCursorRepository cursorRepository;
    private final FixNewOrderSingleBuilder newOrderSingleBuilder;
    private final FixOutboundSessionSend fixOutboundSessionSend;
    private final MeterRegistry meterRegistry;
    private final Clock wallClock;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Long> lastAppliedPosition = new AtomicReference<>(0L);
    private Thread replayThread;

    /**
     * Slice 4l H2 (batched cursor advance). Cached at {@link #init} from
     * {@link OmsConfig.Cluster.FixEgress#getCursorFlushEvery()}. Default {@code 1} preserves the
     * per-event advance shape from slice 3b-2; {@code N>1} batches the Postgres UPSERT to
     * {@code oms_fix_egress_cursor} and widens the at-least-once-at-broker redelivery window to
     * up to {@code N-1} NOS per crash (the broker rejects duplicates via {@code DupClOrdID}).
     * Cached locally so the hot path does not chase through {@code OmsConfig} on every fragment.
     */
    private int cursorFlushEvery = 1;

    // Slice 4l H2 batched-flush bookkeeping. Single-writer (replay thread) for the increment;
    // the shutdown final flush also runs on the replay thread (in replayLoop's finally block),
    // so plain ints/longs are safe. lastAppliedPosition stays atomic because lastAppliedPosition()
    // is read from other threads (tests, JMX/actuator if added later).
    private int eventsSinceCursorFlush;
    private long pendingCursorPosition;

    @Autowired
    public OmsFixEgressService(
            OmsConfig config,
            OmsFixEgressCursorRepository cursorRepository,
            MeterRegistry meterRegistry,
            @Autowired(required = false) FixNewOrderSingleBuilder newOrderSingleBuilder,
            @Autowired(required = false) FixOutboundSessionSend fixOutboundSessionSend) {
        this(config, cursorRepository, meterRegistry, Clock.systemUTC(), newOrderSingleBuilder, fixOutboundSessionSend);
    }

    /**
     * Visible for tests: inject a deterministic {@link Clock} so unit tests can pin
     * {@code System.currentTimeMillis()}-equivalent values when asserting on the per-event
     * admit-to-NOS Timer. Production wiring goes through the public constructor with
     * {@code Clock.systemUTC()}.
     */
    OmsFixEgressService(
            OmsConfig config,
            OmsFixEgressCursorRepository cursorRepository,
            MeterRegistry meterRegistry,
            Clock wallClock,
            FixNewOrderSingleBuilder newOrderSingleBuilder,
            FixOutboundSessionSend fixOutboundSessionSend) {
        this.config = config;
        this.cursorRepository = cursorRepository;
        this.meterRegistry = meterRegistry;
        this.wallClock = wallClock;
        this.newOrderSingleBuilder = newOrderSingleBuilder;
        this.fixOutboundSessionSend = fixOutboundSessionSend;
    }

    @PostConstruct
    void init() {
        long resumePos = cursorRepository
                .findLastAppliedPosition(EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID)
                .orElse(0L);
        lastAppliedPosition.set(resumePos);
        pendingCursorPosition = resumePos;
        eventsSinceCursorFlush = 0;
        // Slice 4l H2: cache cursor-flush-every from OmsConfig so the hot replay path doesn't
        // chase the config object on every fragment. Min-clamped to 1 by the setter; we still
        // belt-and-braces clamp here to fail closed on pathological injection.
        cursorFlushEvery = Math.max(1, config.getCluster().getFixEgress().getCursorFlushEvery());
        boolean fixWired = newOrderSingleBuilder != null && fixOutboundSessionSend != null;
        log.info(
                "oms-fix-egress starting (slice 3b-2 — {}); resuming from log position {} (egressId={}, streamId={}); cursorFlushEvery={}",
                fixWired
                        ? "FIX send active: NewOrderSingle via FixOutboundSessionSend"
                        : "cursor-only mode (no FIX beans on classpath; routing.backend != fix)",
                resumePos,
                EGRESS_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                cursorFlushEvery);
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
            // Slice 4l H2: flush any pending batched cursor advance before tearing down. The
            // replay thread is the only writer of eventsSinceCursorFlush / pendingCursorPosition,
            // so reading them here (still on the replay thread) is race-free. With
            // cursorFlushEvery=1 (default) this is always a no-op.
            flushPendingCursorAdvance("replay loop shutdown");
            CloseHelper.quietClose(replay);
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
            log.info("oms-fix-egress replay loop stopped");
        }
    }

    /**
     * Slice 4l H2 helper. Persists the latest in-memory cursor position when at least one
     * fragment has been applied since the last flush. Idempotent: with no pending fragments
     * (or {@code cursorFlushEvery=1} so every fragment already flushes inline) this is a
     * no-op. Failures are logged but not rethrown — on restart, replay redelivers the
     * batch's worth of fragments and the broker rejects duplicate NOS via {@code DupClOrdID}
     * (option 1 dedupe).
     */
    private void flushPendingCursorAdvance(String reason) {
        if (eventsSinceCursorFlush <= 0) {
            return;
        }
        long pos = pendingCursorPosition;
        eventsSinceCursorFlush = 0;
        try {
            cursorRepository.advance(EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, pos);
        } catch (RuntimeException e) {
            log.warn(
                    "oms-fix-egress: pending cursor flush failed at {} (reason={}); restart will replay fragments and broker will dedupe via DupClOrdID",
                    pos,
                    reason,
                    e);
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
     * Decodes one fragment and applies it: slice 3b-2 builds + sends {@link NewOrderSingle},
     * then advances the cursor. {@link io.aeron.logbuffer.Header#position()} is the cluster log
     * position <em>after</em> this fragment.
     *
     * <p>If {@link #applyAdmittedEvent} returns without advancing (e.g. running flipped false
     * during a session-not-ready park loop), {@link #lastAppliedPosition} is also not updated —
     * the next iteration of the replay loop sees the new {@code running} state and exits, and
     * restart redelivers this fragment from the persisted cursor (option 1 dedupe; see
     * class-level Javadoc).
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
            boolean advanced = applyAdmittedEvent(ev, newPosition);
            if (advanced) {
                lastAppliedPosition.set(newPosition);
            }
        }
    }

    /**
     * Single-fragment apply for one {@link OrderAdmittedEvent}. Package-private so tests can
     * drive replay from a synthesised event without standing up Aeron when that is wasteful
     * (live ITs in {@code OmsFixEgressBrokerIT} still drive the full path through the cluster +
     * embedded acceptor).
     *
     * <p><strong>Slice 3b-2 sequence:</strong> build {@link NewOrderSingle} from the event →
     * {@link FixOutboundSessionSend#send send} → {@link OmsFixEgressCursorRepository#advance
     * cursor advance}. The cursor advances <em>after</em> a successful send so a crash between
     * send and cursor write replays the fragment (option 1: at-least-once at broker; broker
     * rejects with {@code DupClOrdID}). See class-level Javadoc for the full dedupe semantics.
     *
     * <p>If FIX beans are not autowired (context-only ITs that set
     * {@code oms.routing.backend=noop} so {@code FixNewOrderSingleBuilder} /
     * {@code FixOutboundSessionSend} are absent), the method falls back to the slice-3b-1
     * cursor-only behaviour. Production always wires the FIX path.
     *
     * <p>{@link SessionNotFound} (initiator mid-reconnect) → park
     * {@link OmsConfig.Cluster.FixEgress#getSessionNotReadyParkNanos()} ns and retry. If
     * {@link #running} flips false during the wait, return {@code false} so the caller does
     * <em>not</em> advance {@link #lastAppliedPosition} — restart will redeliver the fragment
     * from the persisted cursor.
     *
     * @return {@code true} if the cursor was advanced (event fully applied / send succeeded);
     *     {@code false} if the apply was aborted (shutdown during session-not-ready wait).
     */
    boolean applyAdmittedEvent(OrderAdmittedEvent ev, long newPosition) {
        boolean fixSendObserved = false;
        if (newOrderSingleBuilder != null && fixOutboundSessionSend != null) {
            NewOrderSingle nos = newOrderSingleBuilder.build(ev);
            long parkNanos = config.getCluster().getFixEgress().getSessionNotReadyParkNanos();
            while (running.get()) {
                if (!fixOutboundSessionSend.hasActiveSession()) {
                    LockSupport.parkNanos(parkNanos);
                    continue;
                }
                try {
                    fixOutboundSessionSend.send(nos);
                    fixSendObserved = true;
                    break;
                } catch (SessionNotFound e) {
                    log.warn(
                            "oms-fix-egress: SessionNotFound on Session.sendToTarget for orderId={}; retrying in {}ns.",
                            ev.orderId(),
                            parkNanos);
                    LockSupport.parkNanos(parkNanos);
                }
            }
            if (!running.get()) {
                // Service shutting down before the FIX send succeeded — leave the cursor
                // un-advanced so the next start replays this fragment.
                return false;
            }
        }
        if (fixSendObserved) {
            // Phase 4j per-event histogram: record cluster-admit -> NOS-on-wire only when the FIX
            // send actually happened. Cursor-only context-IT mode (FIX beans absent) keeps the
            // Timer silent, matching its "did the NOS leave?" semantic.
            long latencyMs = wallClock.millis() - ev.acceptedAtMillis();
            OmsPipelineMetrics.recordClusterAdmitToFixNos(
                    meterRegistry, EGRESS_ID, ev.side(), ev.timeInForceCode(), latencyMs);
        }
        // Slice 4l H2: gate the Postgres UPSERT on the configured batch size. Default 1
        // preserves slice 3b-2 per-event semantics; >1 widens the at-least-once-at-broker
        // redelivery window to up to (cursorFlushEvery-1) NOS per crash.
        pendingCursorPosition = newPosition;
        eventsSinceCursorFlush++;
        if (eventsSinceCursorFlush >= cursorFlushEvery) {
            cursorRepository.advance(EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID, newPosition);
            eventsSinceCursorFlush = 0;
        }
        return true;
    }
}
