package com.balh.oms.venueegress;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterEventsRecordingSupport;
import com.balh.oms.cluster.OmsClusterEventsRecordingSupport.BootstrapPick;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.cluster.OrderCancelRequestedEvent;
import com.balh.oms.cluster.OrderReplaceRequestedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.balh.oms.venue.VenueGrpcExecutionReportMapper;
import com.balh.oms.routing.VenueRoutingSymbols;
import com.balh.oms.venue.VenueRouteOrderClient;
import com.balh.oms.venue.VenueRouteTransportException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 * {@code oms_venue_egress_cursor}. The hot send path is {@code AeronArchive.replay} → builder →
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
 *       {@code DupClOrdID}. Re-uses {@code oms_venue_egress_cursor} unchanged; adds a sibling
 *       {@code oms_venue_egress_sent_orders(order_id, version, sent_at)} table.</li>
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
 * {@link OmsConfig.Cluster.VenueEgress#getSessionNotReadyParkNanos()} ns and retry the same
 * fragment without advancing the cursor — Aeron has already delivered the fragment to the
 * fragment handler, so retrying inside {@link #applyAdmittedEvent} keeps the message in flight
 * without forcing a replay-loop teardown. If the running flag flips false during the wait, we
 * exit without advancing; restart redelivers the fragment from the persisted cursor (one
 * fragment behind the failed send, since slice 3b-1's cursor advance is post-send).
 *
 * <h2>Failure handling (replay infrastructure, 2026-05-23 hardening — V56)</h2>
 *
 * <p>If the cluster has never written a recording (first ever cluster boot, fresh archive
 * directory), the thread polls every
 * {@link OmsConfig.Cluster.VenueEgress#getRecordingLookupParkMs()} ms until a recording appears,
 * then bootstraps the cursor to {@code (oldestRecordingId, startPosition)}. Once at least one
 * recording exists the cursor always carries an explicit Aeron Archive recording id (V56
 * migration) and the replay loop walks recordings in id order, rolling forward at each
 * recording's {@code stopPosition} when a successor exists. The bug class fixed by V56 is the
 * pre-hardening silent-clamp behaviour: when the saved cursor fell outside the active
 * recording's bounds (e.g. saved position pointed into a previous, still-on-disk recording the
 * egress no longer had a pointer to), the old {@code clampToRecording} path silently reset
 * to {@code descriptor.startPosition()} of the latest recording and persisted that — destroying
 * the breadcrumb to the earlier recording's events. The egress' specific impact is harder to
 * spot than the projector's (the bench broker idempotently re-accepts NOS retries, hiding the
 * "skipped events" symptom), but the bug is identical to the projector pre-V55. See
 * {@code system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md}
 * §9 / §9.6 for the full incident narrative and the projector-side fix that this commit mirrors.
 *
 * <p>The egress now fails loud (throws {@link IllegalStateException} and the replay loop exits)
 * in three situations that the pre-V56 code silently papered over:
 * <ul>
 *   <li>A legacy {@code oms_venue_egress_cursor} row exists where {@code last_applied_recording_id}
 *       is {@code NULL} (pre-V56 schema or a row written by pre-2026-05-23 code). The operator
 *       must pin a recording id explicitly via SQL before the egress can resume.</li>
 *   <li>The saved recording id no longer appears in the Aeron Archive listing (disk wipe,
 *       manual archive deletion, wiring pointed at the wrong archive). Operator decides
 *       whether to repoint the archive or accept data loss via {@code resetWithRecording}.</li>
 *   <li>The saved position is past the live tail of the saved recording (recording truncated
 *       under the egress, or other unusual condition). Operator inspection only.</li>
 * </ul>
 *
 * <h2>Optional FIX dependencies</h2>
 *
 * <p>{@link FixNewOrderSingleBuilder} and {@link FixOutboundSessionSend} are both gated on
 * {@code oms.routing.backend=fix}. They are autowired with {@code required = false} so this
 * service still loads in context-only ITs that set {@code oms.routing.backend=noop} (e.g.
 * {@code OmsVenueEgressApplicationIT}, {@code OmsVenueEgressReplayIT}). When either is absent the
 * loop runs in slice-3b-1 mode (cursor advance only) — that path is exercised by the slice 3b-1
 * IT and stays as a safety hatch for tests that want to validate the replay infrastructure
 * without standing up QuickFIX. Production deployments always set {@code routing.backend=fix}
 * so both beans load.
 */
@Component
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(prefix = "oms.cluster.venue-egress", name = "enabled", havingValue = "true")
public class OmsVenueEgressService {

    private static final Logger log = LoggerFactory.getLogger(OmsVenueEgressService.class);

    public static final String EGRESS_ID = "oms-venue-egress-default";

    private final OmsConfig config;
    private final OmsVenueEgressCursorRepository cursorRepository;
    private final VenueRouteOrderClient venueRouteOrderClient;
    private final OmsClusterIngressClient clusterIngressClient;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final Clock wallClock;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Long> lastAppliedPosition = new AtomicReference<>(0L);
    /**
     * 2026-05-23 hardening (handover §9.6). Aeron Archive recording id this egress is currently
     * replaying. Read by {@link #advanceCursor} / {@link #flushPendingCursorAdvance} so each
     * persisted {@code (recording_id, position)} pair always travels together.
     *
     * <p>{@code -1} means "not yet set" — the replay loop populates it before opening the first
     * replay subscription. A value of {@code -1} reaching the cursor write is a programming bug
     * and {@link #requireCurrentRecordingId} fails loud rather than writing {@code -1} to
     * Postgres.
     */
    private final AtomicLong currentRecordingId = new AtomicLong(-1L);
    /**
     * 2026-05-23 hardening. When non-empty, {@link #init} stashes the resume cursor here and the
     * replay loop honors it as the start position; on a fresh first-ever start it stays empty and
     * the replay loop bootstraps from the oldest available recording at position 0.
     */
    private final AtomicReference<OmsVenueEgressCursorRepository.RecordedCursor> startupCursor =
            new AtomicReference<>(null);
    private Thread replayThread;

    /**
     * Slice 4l H2 (batched cursor advance). Cached at {@link #init} from
     * {@link OmsConfig.Cluster.VenueEgress#getCursorFlushEvery()}. Default {@code 1} preserves the
     * per-event advance shape from slice 3b-2; {@code N>1} batches the Postgres UPSERT to
     * {@code oms_venue_egress_cursor} and widens the at-least-once-at-broker redelivery window to
     * up to {@code N-1} NOS per crash (the broker rejects duplicates via {@code DupClOrdID}).
     * Cached locally so the hot path does not chase through {@code OmsConfig} on every fragment.
     */
    private int cursorFlushEvery = 1;

    /**
     * Cached from {@link OmsConfig.Cluster.VenueEgress} at {@link #init()} so the replay hot path
     * does not chase the config object on every {@code replay.poll}.
     */
    /** Aligns with {@link OmsConfig.Cluster.VenueEgress} default until {@link #init()} caches config. */
    private int replayFragmentLimit = 512;
    private long replayPollParkNanos = 1_000_000L;

    /**
     * Design A pipeline (slice: venue egress pipelining). Non-null only when
     * {@code oms.cluster.venue-egress.venue-route-max-in-flight > 1} <em>and</em> the venue clients
     * are wired. When null, the replay loop runs the exact pre-pipelining serial path. Owned by the
     * replay thread; see {@link EgressRoutePipeline}.
     */
    private EgressRoutePipeline pipeline;

    // Slice 4l H2 batched-flush bookkeeping. Single-writer (replay thread) for the increment;
    // the shutdown final flush also runs on the replay thread (in replayLoop's finally block),
    // so plain ints/longs are safe. lastAppliedPosition stays atomic because lastAppliedPosition()
    // is read from other threads (tests, JMX/actuator if added later).
    private int eventsSinceCursorFlush;
    private long pendingCursorPosition;

    /**
     * Route-once cache: replay redelivery must not call {@link VenueRouteOrderClient} again for
     * the same {@code orderId} — a second gRPC {@code RouteOrder} after the book consumed contra
     * liquidity yields a zero-qty ack and the cluster drops the apply, leaving FIX-in clients
     * stuck at {@code ExecType=NEW}.
     */
    private final ConcurrentHashMap<UUID, Optional<com.balh.venue.grpc.v1.ExecutionReport>>
            venueRouteResultByOrderId = new ConcurrentHashMap<>();

    @Autowired
    public OmsVenueEgressService(
            OmsConfig config,
            OmsVenueEgressCursorRepository cursorRepository,
            MeterRegistry meterRegistry,
            ObjectMapper objectMapper,
            @Autowired(required = false) VenueRouteOrderClient venueRouteOrderClient,
            @Autowired(required = false) OmsClusterIngressClient clusterIngressClient) {
        this(
                config,
                cursorRepository,
                meterRegistry,
                objectMapper,
                Clock.systemUTC(),
                venueRouteOrderClient,
                clusterIngressClient);
    }

    OmsVenueEgressService(
            OmsConfig config,
            OmsVenueEgressCursorRepository cursorRepository,
            MeterRegistry meterRegistry,
            ObjectMapper objectMapper,
            Clock wallClock,
            VenueRouteOrderClient venueRouteOrderClient,
            OmsClusterIngressClient clusterIngressClient) {
        this.config = config;
        this.cursorRepository = cursorRepository;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
        this.wallClock = wallClock;
        this.venueRouteOrderClient = venueRouteOrderClient;
        this.clusterIngressClient = clusterIngressClient;
    }

    @PostConstruct
    void init() {
        // 2026-05-23 hardening (handover §9.6). Load the saved cursor in its full
        // (recording_id, position, high_water) shape. Four cases:
        //
        //   1. Empty Optional      — first-ever egress start (no row in oms_venue_egress_cursor).
        //                            Replay loop will bootstrap from the oldest recording at pos 0.
        //   2. Legacy NULL row     — row exists but predates V56 (no recording id). The pre-V56
        //                            replay loop silently clamped position to 0 of the active
        //                            recording when the saved position overshot, losing the
        //                            pointer to events in earlier recordings. We refuse to start
        //                            until an operator pins the recording id explicitly via SQL.
        //   3. V59 rewind detected — high_water is non-NULL and lexicographically exceeds
        //                            last_applied. Operator (or a bug) UPDATEd the cursor
        //                            backwards. Replaying from the rewound point would re-ship
        //                            duplicate NOS to the broker; refuse to start until the
        //                            operator explicitly zeros the high-water mark too.
        //   4. Recording-aware row — resume from the saved (recording_id, position) directly.
        Optional<OmsVenueEgressCursorRepository.RecordedCursorWithHighWater> savedRow = cursorRepository
                .findLastAppliedCursorWithHighWater(EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID);
        Optional<OmsVenueEgressCursorRepository.RecordedCursor> savedCursor =
                savedRow.map(OmsVenueEgressCursorRepository.RecordedCursorWithHighWater::lastApplied);
        if (savedCursor.isPresent() && !savedCursor.get().hasRecordingId()) {
            long legacyPos = savedCursor.get().position();
            String repairSql = String.format(
                    "UPDATE oms_venue_egress_cursor SET last_applied_recording_id = <pick the recording id matching the saved position %d>"
                            + " WHERE egress_id = '%s' AND stream_id = %d;",
                    legacyPos, EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID);
            throw new IllegalStateException(
                    "oms-venue-egress refuses to start: oms_venue_egress_cursor row exists but"
                            + " last_applied_recording_id IS NULL (pre-V56 schema or pre-2026-05-23-hardening write)."
                            + " Without the recording id the egress cannot tell which Aeron Archive recording the"
                            + " saved position " + legacyPos + " refers to, and the old code path silently reset to"
                            + " position 0 of the current recording, which on a non-DupClOrdID-tolerant venue would"
                            + " mean admit events from prior cluster incarnations are never sent as NOS to the"
                            + " broker. Operator must pick the correct recording id (ls"
                            + " /opt/oms/aeron-archive/data/*-events*.rec, or psql to check which recording was"
                            + " current at the moment the saved position was written) and run: "
                            + repairSql
                            + " — see system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md §9.6.");
        }
        // V59 rewind guard. Honoured only when both pairs are recording-aware; legacy rows are
        // caught above. Refuses to start when last_applied < high_water lexicographically: that
        // means the cursor has been rewound (operator SQL UPDATE, a bug, or a recovery script
        // that didn't acknowledge the rewind by zeroing the high-water mark). Replaying from
        // the rewound point would re-ship admit events as duplicate NOS to the broker.
        if (savedRow.isPresent() && savedRow.get().isRewound()) {
            OmsVenueEgressCursorRepository.RecordedCursor last = savedRow.get().lastApplied();
            OmsVenueEgressCursorRepository.RecordedCursor high = savedRow.get().highWater();
            String repairSql = String.format(
                    "UPDATE oms_venue_egress_cursor SET high_water_recording_id = %d, high_water_position = %d"
                            + " WHERE egress_id = '%s' AND stream_id = %d;",
                    last.recordingId(), last.position(), EGRESS_ID, OmsClusterWireFormat.EVENTS_STREAM_ID);
            throw new IllegalStateException(
                    "oms-venue-egress refuses to start: cursor has been rewound."
                            + " last_applied=(recordingId=" + last.recordingId()
                            + ", position=" + last.position() + "), but historical"
                            + " high_water=(recordingId=" + high.recordingId()
                            + ", position=" + high.position() + ") is lexicographically greater."
                            + " Replaying from the rewound point would re-ship admit events as duplicate NOS"
                            + " to the broker (option-1 dedupe assumes the broker honours DupClOrdID, which is"
                            + " venue-specific and operationally noisy even when it works). If the rewind is"
                            + " intentional (operator recovery), acknowledge it by zeroing the high-water mark"
                            + " before next start: " + repairSql
                            + " — see system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md §10.");
        }
        savedCursor.ifPresent(startupCursor::set);
        long resumePos;
        if (savedCursor.isPresent()) {
            OmsVenueEgressCursorRepository.RecordedCursor c = savedCursor.get();
            resumePos = c.position();
            lastAppliedPosition.set(resumePos);
            currentRecordingId.set(c.recordingId());
        } else {
            resumePos = 0L;
            lastAppliedPosition.set(0L);
        }
        pendingCursorPosition = resumePos;
        eventsSinceCursorFlush = 0;
        // Slice 4l H2: cache cursor-flush-every from OmsConfig so the hot replay path doesn't
        // chase the config object on every fragment. Min-clamped to 1 by the setter; we still
        // belt-and-braces clamp here to fail closed on pathological injection.
        cursorFlushEvery = Math.max(1, config.getCluster().getVenueEgress().getCursorFlushEvery());
        OmsConfig.Cluster.VenueEgress venueEgressCfg = config.getCluster().getVenueEgress();
        replayFragmentLimit = effectiveReplayFragmentLimit(venueEgressCfg.getFragmentLimit());
        replayPollParkNanos = venueEgressCfg.getPollParkNanos();
        boolean venueWired = venueRouteOrderClient != null && clusterIngressClient != null;
        int maxInFlight = Math.max(1, config.getCluster().getVenueEgress().getVenueRouteMaxInFlight());
        if (savedCursor.isPresent()) {
            log.info(
                    "oms-venue-egress starting ({}); resuming from recording {} at log position {} (egressId={}, streamId={}); cursorFlushEvery={}",
                    venueWired
                            ? "gRPC RouteOrder active via VenueRouteOrderClient"
                            : "cursor-only mode (venue client absent; routing.backend != internal-venue)",
                    savedCursor.get().recordingId(),
                    resumePos,
                    EGRESS_ID,
                    OmsClusterWireFormat.EVENTS_STREAM_ID,
                    cursorFlushEvery);
        } else {
            log.info(
                    "oms-venue-egress starting fresh (no saved cursor; will bootstrap from oldest available events recording at position 0) ({}); (egressId={}, streamId={}); cursorFlushEvery={}",
                    venueWired
                            ? "gRPC RouteOrder active via VenueRouteOrderClient"
                            : "cursor-only mode (venue client absent; routing.backend != internal-venue)",
                    EGRESS_ID,
                    OmsClusterWireFormat.EVENTS_STREAM_ID,
                    cursorFlushEvery);
        }
        running.set(true);
        if (venueWired && maxInFlight > 1) {
            pipeline = new EgressRoutePipeline(maxInFlight);
            log.info(
                    "oms-venue-egress pipelined RouteOrderStream active: venueRouteMaxInFlight={} (cursor advances over the contiguous-completed prefix; crash redelivery window <= {} RouteOrders, deduped by the venue)",
                    maxInFlight,
                    maxInFlight);
        }
        replayThread = new Thread(this::replayLoop, "oms-venue-egress-replay");
        replayThread.setDaemon(true);
        applyReplayThreadPriority(replayThread, venueEgressCfg.getReplayThreadPriority());
        replayThread.start();
    }

    /**
     * Sets {@link Thread#setPriority(int)} for the dedicated replay drain thread. Package-private
     * so tests can assert clamping without starting Aeron.
     */
    static void applyReplayThreadPriority(Thread replayThread, int priority) {
        try {
            replayThread.setPriority(priority);
        } catch (SecurityException e) {
            log.warn(
                    "oms-venue-egress: could not set replay thread priority to {} (SecurityManager?)",
                    priority,
                    e);
        }
    }

    /**
     * Visible for tests that drive {@link #applyAdmittedEvent} / {@link #applyCancelRequestedEvent}
     * / {@link #applyReplaceRequestedEvent} directly (bypassing {@link #init} and the replay loop
     * that would normally seed the recording id from the Aeron Archive). Production code never
     * calls this — the replay loop owns the field.
     */
    void setCurrentRecordingIdForTesting(long recordingId) {
        currentRecordingId.set(recordingId);
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
        if (pipeline != null) {
            pipeline.shutdown();
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

    /**
     * 2026-05-23 hardening (handover §9.6). Rewritten to walk all events recordings sorted by id
     * instead of unconditionally picking the highest-id recording and silently clamping the
     * saved cursor to its start position. Mirrors the projector's V55 rewrite shape-for-shape;
     * see {@code OmsPostgresProjector#replayLoop} for the per-step rationale.
     *
     * <p>The egress has one extra concern the projector does not: batched cursor flush
     * ({@code cursorFlushEvery > 1}). Every recording-boundary roll-forward AND every
     * recording-start clamp flushes the pending batch <em>under the old recording id</em>
     * before {@link OmsVenueEgressCursorRepository#resetWithRecording} moves the cursor to the
     * new recording. Without the pre-reset flush, a pending position belonging to the OLD
     * recording would be persisted with the NEW recording id, violating the cursor's
     * (recording_id, position) invariant.
     */
    private void replayLoop() {
        OmsConfig.Cluster.VenueEgress cfg = config.getCluster().getVenueEgress();
        if (cfg.getAeronDirectory().isBlank()) {
            // No cluster wiring configured. Common in Spring context-only tests that boot the
            // egress profile to verify bean topology without standing up Aeron. Production
            // must always set OMS_FIX_EGRESS_AERON_DIR; the topology validator covers the
            // grpc-off / cluster-client-off invariants but does not require an aeron-directory
            // until slice 3b-2 wires the actual FIX send (a no-op replay loop on an unset
            // aeron-directory is the safer default for context-only ITs today).
            log.warn(
                    "oms-venue-egress replay loop skipped: oms.cluster.venue-egress.aeron-directory is empty");
            return;
        }
        // Outer reconnect loop. A transient OMS cluster MediaDriver bounce (e.g. cluster-node
        // restart) makes Aeron Archive calls throw io.aeron.exceptions.TimeoutException /
        // DriverTimeoutException, and the Aeron client conductor raises AgentTerminationException.
        // Before 2026-06-03 these propagated to a terminal catch and PERMANENTLY stopped the
        // replay thread, silently severing the OMS->venue bridge until an operator restarted the
        // process (observed on pop: loop died 17:25 on a cluster bounce, never recovered). Such
        // infra timeouts are recoverable: close the dead Aeron/Archive, park, and reconnect from
        // the persisted cursor. Genuine cursor/recording-state corruption (IllegalStateException
        // from requireRecordingPresent / requirePositionWithinRecording) stays fatal — reconnecting
        // would re-throw immediately and silently resetting risks data loss, so an operator decides.
        while (running.get()) {
            Aeron aeron = null;
            AeronArchive archive = null;
            boolean reconnectAfterTransientError = false;
            try {
                aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cfg.getAeronDirectory()));
                archive = AeronArchive.connect(new AeronArchive.Context()
                        .aeron(aeron)
                        .ownsAeronClient(false)
                        .controlRequestChannel(cfg.getArchiveControlRequestChannel())
                        .controlResponseChannel(cfg.getArchiveControlResponseChannel()));

                // Bootstrap: if no saved cursor at startup, wait for the first recording to appear,
                // then persist (oldestId, 0) so subsequent restarts have a recording id to anchor on.
                if (startupCursor.get() == null) {
                    if (!bootstrapFromOldestRecording(archive, cfg.getRecordingLookupParkMs())) {
                        return; // shutdown requested during bootstrap
                    }
                }

                runReplayLoopWithRecordingWalk(archive, cfg);
                // Normal return only happens when running == false (shutdown). The while-guard exits.
            } catch (IllegalStateException e) {
                // Genuine, unrecoverable cursor/recording-state corruption (saved recording id
                // missing from Archive, saved position past recording end). Loud-stop: the
                // exception message carries the operator remediation. Reconnecting cannot help.
                log.error("oms-venue-egress replay loop terminating (unrecoverable cursor/recording state)", e);
                return;
            } catch (RuntimeException e) {
                if (!running.get()) {
                    return; // interrupted during shutdown — not an error worth reconnecting for
                }
                if (isRecoverableClusterInfraError(e)) {
                    reconnectAfterTransientError = true;
                    log.warn(
                            "oms-venue-egress lost the OMS cluster archive (transient infra error);"
                                    + " reconnecting in {}ms and resuming from the persisted cursor",
                            cfg.getRecordingLookupParkMs(), e);
                } else {
                    log.error("oms-venue-egress replay loop terminating (unexpected error)", e);
                    return;
                }
            } finally {
                // Slice 4l H2: flush any pending batched cursor advance before tearing down. The
                // replay thread is the only writer of eventsSinceCursorFlush / pendingCursorPosition,
                // so reading them here (still on the replay thread) is race-free. With
                // cursorFlushEvery=1 (default) this is always a no-op.
                flushPendingCursorAdvance("replay loop teardown");
                CloseHelper.quietClose(archive);
                CloseHelper.quietClose(aeron);
            }

            if (!reconnectAfterTransientError) {
                break; // clean shutdown
            }
            try {
                Thread.sleep(cfg.getRecordingLookupParkMs());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("oms-venue-egress replay loop stopped");
    }

    /**
     * Classifies a {@link RuntimeException} caught by the replay loop as a <em>recoverable</em>
     * OMS-cluster infrastructure error (the Aeron MediaDriver / Archive became unreachable, e.g.
     * because {@code oms-cluster-node} restarted) versus a fatal state error.
     *
     * <p>Recoverable: any {@link io.aeron.exceptions.TimeoutException} (covers
     * {@link io.aeron.exceptions.DriverTimeoutException}),
     * {@link io.aeron.exceptions.RegistrationException}, or
     * {@link org.agrona.concurrent.AgentTerminationException} anywhere in the cause chain — these
     * are transport/driver-level and clear once the cluster is reachable again. Cursor/recording
     * state corruption is signalled separately via {@link IllegalStateException} (see
     * {@link #requireRecordingPresent} / {@link #requirePositionWithinRecording}) and is handled
     * as fatal by the caller, so it is intentionally NOT matched here.
     *
     * <p>Package-private + static so it can be unit-tested without standing up a real Aeron
     * Archive.
     */
    static boolean isRecoverableClusterInfraError(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof io.aeron.exceptions.TimeoutException
                    || t instanceof io.aeron.exceptions.RegistrationException
                    || t instanceof org.agrona.concurrent.AgentTerminationException) {
                return true;
            }
        }
        return false;
    }

    /**
     * First-ever-start path: picks the lowest-id <em>non-empty</em> events recording on the
     * channel/stream (skipping empty tombstones such as recording {@code 0} after a cluster
     * wipe), persists {@code (id, startPosition)} as the initial cursor, and seeds
     * {@link #currentRecordingId} + {@link #lastAppliedPosition} + the batch-flush bookkeeping.
     *
     * @return {@code true} if a recording was found and persisted; {@code false} if shutdown was
     *     requested before any recording appeared.
     */
    private boolean bootstrapFromOldestRecording(AeronArchive archive, long parkMs) {
        while (running.get()) {
            List<RecordingDescriptor> recordings = listEventsRecordingsSorted(archive);
            Optional<BootstrapPick> bootstrapPick = OmsClusterEventsRecordingSupport.pickBootstrapRecording(
                    archive,
                    recordings.stream()
                            .map(d -> new BootstrapPick(d.recordingId(), d.startPosition(), d.stopPosition()))
                            .toList());
            if (bootstrapPick.isPresent()) {
                BootstrapPick target = bootstrapPick.get();
                if (target.skippedEmptyTombstones() > 0) {
                    log.info(
                            "oms-venue-egress bootstrap: skipped {} empty events recording tombstone(s);"
                                    + " persisting initial cursor (recordingId={}, position={})"
                                    + " — first start on this egress, no prior cursor row.",
                            target.skippedEmptyTombstones(),
                            target.recordingId(),
                            target.startPosition());
                } else {
                    log.info(
                            "oms-venue-egress bootstrap: persisting initial cursor (recordingId={}, position={})"
                                    + " — first start on this egress, no prior cursor row.",
                            target.recordingId(),
                            target.startPosition());
                }
                cursorRepository.resetWithRecording(
                        EGRESS_ID,
                        OmsClusterWireFormat.EVENTS_STREAM_ID,
                        target.recordingId(),
                        target.startPosition());
                currentRecordingId.set(target.recordingId());
                lastAppliedPosition.set(target.startPosition());
                pendingCursorPosition = target.startPosition();
                eventsSinceCursorFlush = 0;
                return true;
            }
            try {
                Thread.sleep(parkMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /**
     * The reopen-and-poll outer loop, exited only on shutdown or unrecoverable error. Each
     * iteration looks up the current recording descriptor, validates the saved position against
     * it, opens a replay, drains it, and decides whether to roll forward to a newer recording.
     *
     * <p>Roll-forward semantics: a recording is "complete" when its {@code stopPosition} is
     * finalized (set by Aeron Archive when the recording's writer publication closes — cluster
     * exit). When the egress reaches the {@code stopPosition} of the current recording AND a
     * recording with a higher id exists on the same stream, the egress flushes its pending
     * batched cursor (under the OLD recording id), promotes its cursor to
     * {@code (nextId, startPosition)} via {@code resetWithRecording}, and re-opens replay
     * against the new recording.
     */
    private void runReplayLoopWithRecordingWalk(AeronArchive archive, OmsConfig.Cluster.VenueEgress cfg) {
        FragmentHandler handler = new EgressFragmentHandler();
        while (running.get()) {
            long recordingIdNow = currentRecordingId.get();
            if (recordingIdNow < 0L) {
                throw new IllegalStateException(
                        "oms-venue-egress replay loop entered with currentRecordingId="
                                + recordingIdNow
                                + "; init() / bootstrap should have set it. This is a programming bug.");
            }
            RecordingDescriptor descriptor = findRecordingById(archive, recordingIdNow);
            requireRecordingPresent(recordingIdNow, descriptor);
            long savedPosition = lastAppliedPosition.get();
            long upperBound = recordingUpperBound(archive, descriptor);
            requirePositionWithinRecording(recordingIdNow, savedPosition, upperBound, descriptor);
            if (savedPosition < descriptor.startPosition()) {
                // Saved position is below the recording's start. Acceptable to advance to
                // startPosition: the broker's DupClOrdID dedupe makes re-sending NOS for already
                // admitted orders safe (option-1 at-least-once-at-broker semantics, see class
                // Javadoc). Flush any pending batch under the OLD recording id first so the
                // (recording_id, position) invariant is preserved across the reset.
                log.warn(
                        "Egress cursor below recording startPosition: saved=(recordingId={}, position={}),"
                                + " recordingStart={}; advancing to recording start (NOS re-sends will be"
                                + " deduplicated by the broker via DupClOrdID).",
                        recordingIdNow,
                        savedPosition,
                        descriptor.startPosition());
                flushPendingCursorAdvance("recording-start clamp");
                cursorRepository.resetWithRecording(
                        EGRESS_ID,
                        OmsClusterWireFormat.EVENTS_STREAM_ID,
                        recordingIdNow,
                        descriptor.startPosition());
                lastAppliedPosition.set(descriptor.startPosition());
                pendingCursorPosition = descriptor.startPosition();
                eventsSinceCursorFlush = 0;
                savedPosition = descriptor.startPosition();
            }

            // Empty tombstone recordings (post-wipe id=0 with stop=start=0) cannot be replayed.
            while (running.get()
                    && OmsClusterEventsRecordingSupport.isEmptyRecording(
                            archive,
                            descriptor.recordingId(),
                            descriptor.startPosition(),
                            descriptor.stopPosition())) {
                RecordingDescriptor successor = findNextRecording(archive, recordingIdNow);
                if (successor != null) {
                    log.info(
                            "Egress skipping empty events recordingId={}; rolling forward to recordingId={}"
                                    + " startPosition={}.",
                            recordingIdNow,
                            successor.recordingId(),
                            successor.startPosition());
                    flushPendingCursorAdvance("empty recording tombstone skip");
                    cursorRepository.resetWithRecording(
                            EGRESS_ID,
                            OmsClusterWireFormat.EVENTS_STREAM_ID,
                            successor.recordingId(),
                            successor.startPosition());
                    currentRecordingId.set(successor.recordingId());
                    lastAppliedPosition.set(successor.startPosition());
                    pendingCursorPosition = successor.startPosition();
                    eventsSinceCursorFlush = 0;
                    recordingIdNow = successor.recordingId();
                    descriptor = successor;
                    savedPosition = successor.startPosition();
                    continue;
                }
                parkReplayIdle(replayPollParkNanos);
                descriptor = findRecordingById(archive, recordingIdNow);
                requireRecordingPresent(recordingIdNow, descriptor);
            }
            if (!running.get()) {
                return;
            }
            savedPosition = lastAppliedPosition.get();
            upperBound = recordingUpperBound(archive, descriptor);

            Subscription replay = null;
            try {
                try {
                    replay = archive.replay(
                            descriptor.recordingId(),
                            savedPosition,
                            /* length = */ Long.MAX_VALUE,
                            cfg.getReplayChannel(),
                            cfg.getReplayStreamId());
                } catch (RuntimeException e) {
                    if (OmsClusterEventsRecordingSupport.isEmptyRecordingReplayArchiveException(e)) {
                        RecordingDescriptor successor = findNextRecording(archive, recordingIdNow);
                        if (successor != null) {
                            log.warn(
                                    "Egress Archive rejected replay on empty recordingId={}; rolling forward to"
                                            + " recordingId={} startPosition={}.",
                                    recordingIdNow,
                                    successor.recordingId(),
                                    successor.startPosition());
                            flushPendingCursorAdvance("empty recording ArchiveException skip");
                            cursorRepository.resetWithRecording(
                                    EGRESS_ID,
                                    OmsClusterWireFormat.EVENTS_STREAM_ID,
                                    successor.recordingId(),
                                    successor.startPosition());
                            currentRecordingId.set(successor.recordingId());
                            lastAppliedPosition.set(successor.startPosition());
                            pendingCursorPosition = successor.startPosition();
                            eventsSinceCursorFlush = 0;
                            continue;
                        }
                    }
                    throw e;
                }
                log.info(
                        "oms-venue-egress replay open; recordingId={} startPos={} (recordingStart={},"
                                + " recordingStop={}, upperBound={}) channel={} streamId={}",
                        descriptor.recordingId(),
                        savedPosition,
                        descriptor.startPosition(),
                        descriptor.stopPosition(),
                        upperBound,
                        cfg.getReplayChannel(),
                        cfg.getReplayStreamId());

                while (running.get()) {
                    int polledBurst = pollReplayBurst(replay, handler);
                    if (polledBurst > 0) {
                        continue;
                    }
                    if (pipeline != null) {
                        // Lightweight cursor advance only — do not quiesce on every idle poll. At
                        // live tail under sustained admit load (pop @ 5k RPS) a momentary zero-
                        // fragment return is normal; blocking until all in-flight routes complete
                        // stalls the Aeron drain and inflates egress_wall_lag_ms while
                        // egress_route_service_ms stays ~0. Full quiesce runs only before a
                        // recording roll-forward (below).
                        pipeline.drainContiguous();
                    }
                    int idleTailBurst = pollReplayIdleTail(replay, handler);
                    if (idleTailBurst > 0) {
                        continue;
                    }
                    // No fragments. Three cases — mirrors OmsPostgresProjector.runReplayLoopWithRecordingWalk:
                    //   (a) live-tail wait on the cluster's currently-active recording — park.
                    //   (b) end of a completed recording (stopPosition set) with a successor
                    //       available — roll forward.
                    //   (c) stale-open recording from a crashed cluster session: stopPosition
                    //       is NULL_POSITION (never properly closed) but the cluster has
                    //       restarted and is writing to a higher recordingId. Without this
                    //       branch the egress would park on the dead recording forever and
                    //       never emit FIX for new admits. Detected as "a successor exists on
                    //       the same channel/stream" — cluster writes one recording at a time,
                    //       so a higher recordingId means the current one is no longer being
                    //       written to. Symmetric to the projector fix landed 2026-05-23.
                    RecordingDescriptor refreshed = findRecordingById(archive, recordingIdNow);
                    if (refreshed == null) {
                        // Recording vanished mid-replay (extremely unusual). Fall through to
                        // outer loop iteration which will fail loud.
                        break;
                    }
                    long recordingStop = refreshed.stopPosition();
                    long currentApplied = lastAppliedPosition.get();
                    RecordingDescriptor successor = findNextRecording(archive, recordingIdNow);
                    WalkForwardDecision decision = decideWalkForward(recordingStop, currentApplied, successor);
                    if (decision.walk) {
                        if (pipeline != null && !pipeline.quiesce()) {
                            return; // shutdown requested during drain
                        }
                        log.info(
                                "Egress recording boundary: recordingId={} {} at stopPosition={};"
                                        + " rolling forward to recordingId={} startPosition={}.",
                                recordingIdNow,
                                decision.reason,
                                recordingStop,
                                successor.recordingId(),
                                successor.startPosition());
                        // CRITICAL: flush any pending batched cursor advance under the OLD
                        // recording id BEFORE persisting the successor. If we skipped this,
                        // a pendingCursorPosition belonging to the old recording would be
                        // written with the new recording id by the next advanceCursor call,
                        // violating the (recording_id, position) invariant.
                        flushPendingCursorAdvance("recording boundary roll-forward");
                        cursorRepository.resetWithRecording(
                                EGRESS_ID,
                                OmsClusterWireFormat.EVENTS_STREAM_ID,
                                successor.recordingId(),
                                successor.startPosition());
                        currentRecordingId.set(successor.recordingId());
                        lastAppliedPosition.set(successor.startPosition());
                        pendingCursorPosition = successor.startPosition();
                        eventsSinceCursorFlush = 0;
                        break; // exit inner poll loop -> outer loop reopens against successor
                    }
                    parkReplayIdleAfterPoll(pipeline);
                }
            } finally {
                CloseHelper.quietClose(replay);
            }
        }
    }

    /**
     * Spin-yield re-polls after a zero-return {@link #pollReplayBurst} before archive metadata
     * refresh or configured idle park. At 5k–10k admits/s the cluster writer often lands fragments
     * in the gap between the burst terminal zero and {@link #findRecordingById}; skipping this
     * slice forces an Archive control round-trip plus park on every momentary race.
     */
    /** Live-tail spins before park/recording-walk; raised for pop @ 10k admit/s knee vs 5k. */
    private static final int REPLAY_IDLE_TAIL_POLLS = 8;

    /**
     * Tight-spin drain: call {@link Subscription#poll} repeatedly while Aeron delivers fragments,
     * without idle park or recording-walk work between passes. Returns the fragment count for
     * this burst (zero ⇒ caller should run idle / quiesce / walk-forward logic).
     */
    int pollReplayBurst(Subscription replay, FragmentHandler handler) {
        int total = 0;
        int polled;
        do {
            polled = replay.poll(handler, replayFragmentLimit);
            total += polled;
        } while (polled > 0 && running.get());
        if (total > 0 && pipeline != null) {
            pipeline.drainContiguous();
            Thread.onSpinWait();
        }
        return total;
    }

    /**
     * Live-tail idle burst: yield and re-run {@link #pollReplayBurst} a few times before the
     * replay loop pays for recording-walk metadata or a configured park. Package-private for
     * unit tests that assert the spin count without Aeron Archive.
     */
    int pollReplayIdleTail(Subscription replay, FragmentHandler handler) {
        for (int attempt = 0; attempt < REPLAY_IDLE_TAIL_POLLS && running.get(); attempt++) {
            Thread.onSpinWait();
            int polled = pollReplayBurst(replay, handler);
            if (polled > 0) {
                return polled;
            }
            if (pipeline != null && pipeline.replayPollHasBacklog()) {
                continue;
            }
        }
        return 0;
    }

    /**
     * Idle wait between replay polls when Aeron returned zero fragments. When the pipelined route
     * queue or completion tracker still has work, spin-yield instead of parking so the replay
     * thread re-polls the live tail without a configured idle slice behind in-flight admits.
     */
    void parkReplayIdleAfterPoll(EgressRoutePipeline pipeline) {
        if (pipeline != null && pipeline.replayPollHasBacklog()) {
            Thread.onSpinWait();
            return;
        }
        parkReplayIdle(replayPollParkNanos);
    }

    /**
     * Idle park between replay polls when no fragments were available. Kept as a named helper so
     * unit tests can assert the configured nanos without standing up Aeron Archive.
     */
    static void parkReplayIdle(long pollParkNanos) {
        LockSupport.parkNanos(pollParkNanos);
    }

    /**
     * Amortizes {@code replay.poll} syscall overhead at 5k–10k admits/s. Operators may set a lower
     * configured limit; production floors at 2048 frags/poll for sustained 10k/s drain.
     */
    static int effectiveReplayFragmentLimit(int configuredLimit) {
        return Math.max(configuredLimit, 2048);
    }

    /**
     * Lists every recording on the events channel+stream and returns it sorted ascending by
     * {@code recordingId}. Empty list means no recording exists yet (cluster has never run on
     * this Archive directory).
     */
    private List<RecordingDescriptor> listEventsRecordingsSorted(AeronArchive archive) {
        List<RecordingDescriptor> out = new ArrayList<>();
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
                    if (streamId == OmsClusterWireFormat.EVENTS_STREAM_ID) {
                        out.add(new RecordingDescriptor(recordingId, startPosition, stopPosition));
                    }
                });
        out.sort(Comparator.comparingLong(RecordingDescriptor::recordingId));
        return out;
    }

    /**
     * @return the descriptor with the given recording id, or {@code null} if absent from the
     *     Archive's listing for the events channel/stream.
     */
    private RecordingDescriptor findRecordingById(AeronArchive archive, long recordingId) {
        for (RecordingDescriptor d : listEventsRecordingsSorted(archive)) {
            if (d.recordingId() == recordingId) {
                return d;
            }
        }
        return null;
    }

    /**
     * @return the lowest-id recording strictly greater than {@code currentId}, or {@code null}
     *     if no successor exists yet (current recording is the live tail).
     */
    private RecordingDescriptor findNextRecording(AeronArchive archive, long currentId) {
        for (RecordingDescriptor d : listEventsRecordingsSorted(archive)) {
            if (d.recordingId() > currentId) {
                return d;
            }
        }
        return null;
    }

    /**
     * Effective upper bound for a recording at this instant. For a stopped recording it is the
     * finalized {@code stopPosition}; for an active recording it is the Archive's live
     * write-head position. Returns {@code startPosition} when neither is available.
     */
    private long recordingUpperBound(AeronArchive archive, RecordingDescriptor descriptor) {
        return OmsClusterEventsRecordingSupport.recordingUpperBound(
                archive, descriptor.recordingId(), descriptor.startPosition(), descriptor.stopPosition());
    }

    /**
     * 2026-05-23 hardening. Reads {@link #currentRecordingId} for use by any cursor write in
     * the apply path. The replay loop guarantees this is set to a non-negative Aeron Archive
     * recording id before any fragment can be polled, so {@code -1} reaching here is a
     * programming bug — not an operational condition — and we fail loud rather than write
     * {@code -1} to Postgres.
     */
    private long requireCurrentRecordingId() {
        long id = currentRecordingId.get();
        if (id < 0L) {
            throw new IllegalStateException(
                    "oms-venue-egress apply path invoked before currentRecordingId was set"
                            + " (got " + id + "). This indicates the replay loop opened a Subscription"
                            + " without seeding the recording id — programming bug, not an operational condition.");
        }
        return id;
    }

    /**
     * Slice 4l H2 helper. Persists the latest in-memory cursor position when at least one
     * fragment has been applied since the last flush. Idempotent: with no pending fragments
     * (or {@code cursorFlushEvery=1} so every fragment already flushes inline) this is a
     * no-op. Failures are logged but not rethrown — on restart, replay redelivers the
     * batch's worth of fragments and the broker rejects duplicate NOS via {@code DupClOrdID}
     * (option 1 dedupe).
     *
     * <p>2026-05-23 hardening (V56): writes the pending position under the
     * {@link #requireCurrentRecordingId() current recording id}. Callers that cross a
     * recording boundary MUST invoke this BEFORE advancing {@link #currentRecordingId} —
     * otherwise the pending position (from the OLD recording) would be persisted under the
     * NEW recording id, violating the (recording_id, position) invariant. The recording-walk
     * loop above honours that ordering at every roll-forward / recording-start-clamp site.
     */
    private void flushPendingCursorAdvance(String reason) {
        if (eventsSinceCursorFlush <= 0) {
            return;
        }
        long pos = pendingCursorPosition;
        eventsSinceCursorFlush = 0;
        try {
            cursorRepository.advanceWithRecording(
                    EGRESS_ID,
                    OmsClusterWireFormat.EVENTS_STREAM_ID,
                    requireCurrentRecordingId(),
                    pos);
        } catch (RuntimeException e) {
            log.warn(
                    "oms-venue-egress: pending cursor flush failed at {} (reason={}); restart will replay fragments and broker will dedupe via DupClOrdID",
                    pos,
                    reason,
                    e);
        }
    }

    /**
     * Snapshot of the recording's identity and bounds returned by
     * {@link AeronArchive#listRecordingsForUri}.
     */
    record RecordingDescriptor(long recordingId, long startPosition, long stopPosition) {}

    /**
     * Outcome of the per-poll walk-forward decision: should the replay loop break out of the
     * current recording and reopen against {@code successor}? Same shape as the projector's
     * decision (see {@code OmsPostgresProjector.WalkForwardDecision}) on purpose — both
     * services run the same recording-walk machinery against the same Aeron Archive.
     */
    record WalkForwardDecision(boolean walk, String reason) {
        static WalkForwardDecision park() {
            return new WalkForwardDecision(false, "park");
        }
    }

    /**
     * Pure decision function — package-private for {@code OmsVenueEgressServiceWalkForwardTest}.
     * Walk forward when either (a) the current recording is closed and we've fully drained
     * it, OR (b) the current recording is stuck-open ({@code stopPosition == NULL_POSITION}
     * from a crashed cluster session) AND a successor exists. The stale-open branch was
     * added 2026-05-23 alongside the projector fix — without it the egress would park
     * forever on a dead recording after any ungraceful cluster restart, dropping new FIX
     * emissions.
     */
    static WalkForwardDecision decideWalkForward(
            long recordingStop, long currentApplied, RecordingDescriptor successor) {
        if (successor == null) {
            return WalkForwardDecision.park();
        }
        long nullPos = AeronArchive.NULL_POSITION;
        if (recordingStop != nullPos && currentApplied >= recordingStop) {
            return new WalkForwardDecision(true, "complete");
        }
        if (recordingStop == nullPos) {
            return new WalkForwardDecision(true, "stale-open (successor exists)");
        }
        return WalkForwardDecision.park();
    }

    /**
     * Loud-fail when the saved recording id is no longer listed in the Aeron Archive. The
     * pre-hardening behaviour silently reset to position 0 of the active recording, which on
     * a non-DupClOrdID-tolerant venue would mean every admit from the missing recording is
     * never sent as NOS to the broker. Refuse instead; operator decides whether to bootstrap
     * fresh via {@code resetWithRecording(newOldestId, 0)} (accept loss) or restore the
     * recording files. Package-private so the unit test in
     * {@code OmsVenueEgressServiceReplayCursorGuardTest} can exercise both branches without
     * standing up a real {@link AeronArchive}.
     */
    static void requireRecordingPresent(long savedRecordingId, RecordingDescriptor descriptor) {
        if (descriptor != null) {
            return;
        }
        throw new IllegalStateException(
                "oms-venue-egress cannot continue: saved recordingId="
                        + savedRecordingId
                        + " is not listed in the Aeron Archive for events stream "
                        + OmsClusterWireFormat.EVENTS_STREAM_ID
                        + " (channel=" + OmsClusterWireFormat.EVENTS_CHANNEL + ")."
                        + " The recording files may have been deleted or the egress is wired to the wrong"
                        + " Aeron Archive. Operator must decide between (a) repointing to a different Archive,"
                        + " or (b) accepting data loss by running resetWithRecording to a known-good"
                        + " (recordingId, position). Refusing to silently reset to position 0 of an unrelated"
                        + " recording — see handover §9.6.");
    }

    /**
     * Loud-fail when the saved cursor position is past the live upper bound (stopPosition for
     * a closed recording, write-head for an active one). Same shape as the projector twin;
     * package-private for unit tests.
     */
    static void requirePositionWithinRecording(
            long savedRecordingId,
            long savedPosition,
            long upperBound,
            RecordingDescriptor descriptor) {
        if (savedPosition <= upperBound) {
            return;
        }
        throw new IllegalStateException(
                "oms-venue-egress saved cursor (recordingId="
                        + savedRecordingId
                        + ", position=" + savedPosition + ") is past the end of the recording (upperBound="
                        + upperBound + ", startPosition=" + descriptor.startPosition()
                        + ", stopPosition=" + descriptor.stopPosition()
                        + "). This indicates the recording was truncated or replaced under the egress."
                        + " Refusing to silently reset to position 0 — operator must inspect via"
                        + " AeronArchive control + resetWithRecording to a verified (recordingId, position).");
    }

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
            long newPosition = header.position();
            if (pipeline != null) {
                pipeline.onFragment(typeId, buffer, offset, length, newPosition);
                return;
            }
            boolean advanced;
            try {
                switch (typeId) {
                    case OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED -> {
                        OrderAdmittedEvent ev = OrderAdmittedEvent.decode(buffer, offset, length);
                        advanced = applyAdmittedEvent(ev, newPosition);
                    }
                    case OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_REQUESTED -> {
                        OrderCancelRequestedEvent ev =
                                OrderCancelRequestedEvent.decode(buffer, offset, length);
                        advanced = applyCancelRequestedEvent(ev, newPosition);
                    }
                    case OmsClusterWireFormat.TYPE_ID_ORDER_REPLACE_REQUESTED -> {
                        OrderReplaceRequestedEvent ev =
                                OrderReplaceRequestedEvent.decode(buffer, offset, length);
                        advanced = applyReplaceRequestedEvent(ev, newPosition);
                    }
                    default -> {
                        // Other typeIds (ExecutionApplied / OrderRejected / OrderCancelApplied / ...)
                        // are handled by other services; the FIX egress only consumes the venue-
                        // outbound side. Advance the cursor so we don't loop on them.
                        advanced = applyCursorOnly(newPosition);
                    }
                }
            } catch (RuntimeException e) {
                log.error(
                        "oms-venue-egress fragment apply failed (cursor not advanced): typeId={} position={}",
                        typeId,
                        newPosition,
                        e);
                advanced = false;
            }
            if (advanced) {
                lastAppliedPosition.set(newPosition);
            }
        }
    }

    /**
     * Cursor-only advance for event type-ids the egress does not care about. Mirrors the cursor
     * advance shape from {@link #applyAdmittedEvent} so the at-least-once-at-broker semantics
     * stay identical: on JVM crash before the cursor write, restart replays the unrelated
     * fragment and we no-op again.
     *
     * <p>Delegates to {@link #advanceCursor} so the V56 recording-aware cursor write path is
     * the single source of truth for batched flush + recording-id threading.
     */
    boolean applyCursorOnly(long newPosition) {
        return advanceCursor(newPosition);
    }

    /**
     * Single-fragment apply for one {@link OrderAdmittedEvent}. Package-private so tests can
     * drive replay from a synthesised event without standing up Aeron when that is wasteful
     * (live ITs in {@code OmsVenueEgressBrokerIT} still drive the full path through the cluster +
     * embedded acceptor).
     *
     * <p><strong>Slice 3b-2 sequence:</strong> build {@link NewOrderSingle} from the event →
     * {@link FixOutboundSessionSend#send send} → {@link OmsVenueEgressCursorRepository#advance
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
     * {@link OmsConfig.Cluster.VenueEgress#getSessionNotReadyParkNanos()} ns and retry. If
     * {@link #running} flips false during the wait, return {@code false} so the caller does
     * <em>not</em> advance {@link #lastAppliedPosition} — restart will redeliver the fragment
     * from the persisted cursor.
     *
     * @return {@code true} if the cursor was advanced (event fully applied / send succeeded);
     *     {@code false} if the apply was aborted (shutdown during session-not-ready wait).
     */
    boolean applyAdmittedEvent(OrderAdmittedEvent ev, long newPosition) {
        if (!VenueRoutingSymbols.routesToInternalVenue(config, ev.instrumentSymbol())) {
            return advanceCursor(newPosition);
        }
        if (venueRouteOrderClient != null && clusterIngressClient != null) {
            Optional<com.balh.venue.grpc.v1.ExecutionReport> erOpt;
            try {
                erOpt = routeVenueOrderOnce(ev);
            } catch (VenueRouteTransportException e) {
                venueRouteResultByOrderId.remove(ev.orderId());
                log.warn(
                        "oms-venue-egress: venue RouteOrder transport failed; submitting VENUE_REJECT: orderId={} symbol={}",
                        ev.orderId(),
                        ev.instrumentSymbol(),
                        e);
                if (!submitVenueRouteFailedReject(
                        ev.orderId(),
                        "venue_route_transport_failed",
                        ev.acceptedAtMillis(),
                        ev.side(),
                        ev.timeInForceCode())) {
                    return false;
                }
                return advanceCursor(newPosition);
            }
            if (erOpt.isEmpty()) {
                venueRouteResultByOrderId.remove(ev.orderId());
                log.warn(
                        "oms-venue-egress: venue RouteOrder rejected (no ER); submitting VENUE_REJECT: orderId={} symbol={}",
                        ev.orderId(),
                        ev.instrumentSymbol());
                if (!submitVenueRouteFailedReject(
                        ev.orderId(),
                        "venue_route_rejected",
                        ev.acceptedAtMillis(),
                        ev.side(),
                        ev.timeInForceCode())) {
                    return false;
                }
                return advanceCursor(newPosition);
            }
            if (!submitVenueExecutionReport(
                    ev.orderId(), erOpt, ev.acceptedAtMillis(), ev.side(), ev.timeInForceCode())) {
                return false;
            }
            venueRouteResultByOrderId.remove(ev.orderId());
        }
        return advanceCursor(newPosition);
    }

    private Optional<com.balh.venue.grpc.v1.ExecutionReport> routeVenueOrderOnce(OrderAdmittedEvent ev) {
        return venueRouteResultByOrderId.computeIfAbsent(
                ev.orderId(), ignored -> venueRouteOrderClient.routeAdmittedOrder(ev));
    }

    boolean applyCancelRequestedEvent(OrderCancelRequestedEvent ev, long newPosition) {
        if (!VenueRoutingSymbols.routesToInternalVenue(config, ev.instrumentSymbol())) {
            return advanceCursor(newPosition);
        }
        if (venueRouteOrderClient != null && clusterIngressClient != null) {
            Optional<com.balh.venue.grpc.v1.ExecutionReport> erOpt;
            try {
                erOpt = venueRouteOrderClient.routeCancelRequested(ev);
            } catch (VenueRouteTransportException e) {
                // Best-effort cancel during backlog replay: order may never have reached the venue
                // (egress lag). Advancing avoids wedging the entire journal behind one cancel.
                log.warn(
                        "oms-venue-egress: venue RouteCancel transport failed; advancing cursor (best-effort): orderId={} symbol={}",
                        ev.orderId(),
                        ev.instrumentSymbol(),
                        e);
                return advanceCursor(newPosition);
            }
            if (!submitVenueExecutionReport(ev.orderId(), erOpt, ev.requestedAtMillis(), ev.sideCode(), (byte) 0)) {
                return erOpt.isEmpty() ? advanceCursor(newPosition) : false;
            }
        }
        return advanceCursor(newPosition);
    }

    boolean applyReplaceRequestedEvent(OrderReplaceRequestedEvent ev, long newPosition) {
        if (!VenueRoutingSymbols.routesToInternalVenue(config, ev.instrumentSymbol())) {
            return advanceCursor(newPosition);
        }
        if (venueRouteOrderClient != null && clusterIngressClient != null) {
            Optional<com.balh.venue.grpc.v1.ExecutionReport> erOpt;
            try {
                erOpt = venueRouteOrderClient.routeReplaceRequested(ev);
            } catch (VenueRouteTransportException e) {
                log.warn(
                        "oms-venue-egress: venue RouteReplace transport failed; advancing cursor (best-effort): orderId={} symbol={}",
                        ev.orderId(),
                        ev.instrumentSymbol(),
                        e);
                return advanceCursor(newPosition);
            }
            if (!submitVenueExecutionReport(ev.orderId(), erOpt, ev.requestedAtMillis(), ev.sideCode(), ev.timeInForceCode())) {
                return erOpt.isEmpty() ? advanceCursor(newPosition) : false;
            }
        }
        return advanceCursor(newPosition);
    }

    private boolean submitVenueRouteFailedReject(
            UUID orderId,
            String reason,
            long acceptedAtMillis,
            byte side,
            byte timeInForceCode) {
        long venueTsNanos = TimeUnit.MILLISECONDS.toNanos(acceptedAtMillis);
        String venueExecRef = "venue-route-failed-" + orderId;
        String rawEnvelopeJson = "{\"reason\":\"" + reason + "\"}";
        Duration submitTimeout = Duration.ofMillis(config.getCluster().getClient().getSubmitTimeoutMs());
        int maxAttempts = config.getCluster().getVenueEgress().getVenueRejectSubmitMaxAttempts();
        long retryBackoffMs = config.getCluster().getVenueEgress().getVenueRejectSubmitRetryBackoffMs();

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            long correlationId = clusterIngressClient.nextCorrelationId();
            ApplyExecutionReportCommand cmd =
                    new ApplyExecutionReportCommand(
                            correlationId,
                            orderId,
                            0L,
                            0L,
                            venueTsNanos,
                            0,
                            ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT,
                            (byte) RejectCode.VENUE_REJECT.ordinal(),
                            config.getVenue().getVenueId(),
                            venueExecRef,
                            "",
                            rawEnvelopeJson);
            try {
                clusterIngressClient.submitApplyExecutionReport(cmd, submitTimeout);
                long latencyMs = wallClock.millis() - acceptedAtMillis;
                OmsPipelineMetrics.recordClusterAdmitToFixNos(
                        meterRegistry, EGRESS_ID, side, timeInForceCode, latencyMs);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                if (attempt < maxAttempts) {
                    log.warn(
                            "oms-venue-egress: cluster VENUE_REJECT submit attempt {}/{} failed for orderId={}; retrying",
                            attempt,
                            maxAttempts,
                            orderId,
                            e);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(retryBackoffMs));
                } else {
                    log.error(
                            "oms-venue-egress: cluster VENUE_REJECT submit exhausted {} attempts for orderId={};"
                                    + " cursor will not advance — fragment retries on replay",
                            maxAttempts,
                            orderId,
                            e);
                }
            }
        }
        return false;
    }

    private boolean submitVenueExecutionReport(
            java.util.UUID orderId,
            java.util.Optional<com.balh.venue.grpc.v1.ExecutionReport> erOpt,
            long acceptedAtMillis,
            byte side,
            byte timeInForceCode) {
        if (erOpt.isEmpty()) {
            return true;
        }
        ApplyExecutionReportCommand cmd =
                VenueGrpcExecutionReportMapper.toApplyCommand(
                        erOpt.get(), config.getVenue().getVenueId(), objectMapper);
        Duration submitTimeout = Duration.ofMillis(config.getCluster().getClient().getSubmitTimeoutMs());
        try {
            clusterIngressClient.submitApplyExecutionReport(cmd, submitTimeout);
            long latencyMs = wallClock.millis() - acceptedAtMillis;
            OmsPipelineMetrics.recordClusterAdmitToFixNos(
                    meterRegistry, EGRESS_ID, side, timeInForceCode, latencyMs);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            log.warn("oms-venue-egress: cluster submit failed for orderId={}", orderId, e);
            return false;
        }
    }

    private boolean advanceCursor(long newPosition) {
        pendingCursorPosition = newPosition;
        eventsSinceCursorFlush++;
        if (eventsSinceCursorFlush >= cursorFlushEvery) {
            cursorRepository.advanceWithRecording(
                    EGRESS_ID,
                    OmsClusterWireFormat.EVENTS_STREAM_ID,
                    requireCurrentRecordingId(),
                    newPosition);
            eventsSinceCursorFlush = 0;
        }
        return true;
    }

    /**
     * Pipelined-mode cursor advance: persists the contiguous-completed prefix immediately (no batched
     * deferral — the in-flight window already bounds how often this fires, ≤ once per poll batch). The
     * in-flight window <em>is</em> the durability boundary, so the persisted position is exactly the
     * highest fragment whose route + ER-submit fully completed. Runs only on the replay thread, so it
     * shares the same single-writer guarantee as {@link #advanceCursor} for {@code
     * pendingCursorPosition} / {@code eventsSinceCursorFlush} / {@code currentRecordingId}.
     */
    private void advanceCursorContiguous(long position) {
        lastAppliedPosition.set(position);
        pendingCursorPosition = position;
        eventsSinceCursorFlush = 0;
        cursorRepository.advanceWithRecording(
                EGRESS_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                position);
    }

    /**
     * Design A pipeline: dispatches venue {@code RouteOrder}s over the ordered {@code RouteOrderStream}
     * in cluster-log order (preserving venue price-time priority), completes acks asynchronously, and
     * lets the replay thread advance the persisted cursor only over the contiguous-completed prefix
     * (via {@link EgressCompletionTracker}). See
     * {@code system-documentation/plans/oms-venue-egress-pipelining.md}.
     *
     * <h2>Ordering &amp; correctness</h2>
     *
     * <ul>
     *   <li><strong>Venue priority.</strong> Venue-routed admits are written to the stream on the
     *       replay thread in poll order ⇒ the venue cluster offers (hence assigns FIFO sequence) in
     *       OMS admission order.</li>
     *   <li><strong>Per-order ordering / non-admit fragments.</strong> Cancel/replace and any
     *       non-venue / cursor-only fragment are <em>quiesce points</em>: the pipeline drains all
     *       in-flight admits (advancing the cursor) before the fragment is applied synchronously via
     *       the existing serial apply methods. So a cancel for order X can never reach the venue
     *       before X's admit, and the cursor stays a contiguous prefix.</li>
     *   <li><strong>Crash recovery.</strong> The cursor only advances over fully-completed fragments;
     *       in-flight fragments beyond the prefix are replayed on restart (≤ {@code maxInFlight}
     *       duplicate RouteOrders, deduped by the venue) — the same at-least-once-at-venue contract
     *       the serial path documents for {@code cursorFlushEvery}.</li>
     * </ul>
     *
     * <h2>Route-offer throughput ceiling (measured @ 400 RPS admit on pop, 2026-06-05)</h2>
     *
     * <p>{@code RouteOrderStream} writes must preserve OMS admission order for venue price-time
     * priority. {@link VenueRouteOrderClient#routeAdmittedOrderAsync} is explicitly single-writer
     * ({@code streamLock} around {@code onNext}) — <strong>max safe stream-write parallelism = 1</strong>.
     * Parallel {@code onNext} would race on {@code streamLock} without increasing ordered throughput
     * and could reorder offers relative to cluster-log order if multiple callers interleave.
     *
     * <p>Observed @ 400 RPS: egress wall lag grows while venue {@code place_matched} stays ~0.01 ms
     * and ER cluster offer stays ~0 ms — the bottleneck is egress offer dispatch, not venue apply or
     * ER submit. With {@code maxInFlight} permits released on venue ack (not ER completion), replay
     * can enqueue faster than the sole {@code route-offer} thread can drain {@code onNext} calls.
     * Throughput ≈ {@code 1 / per-offer-latency}; at ~2.5 ms/offer that yields ~400 offers/s.
     *
     * <p>Within the single-writer constraint, this class:
     * <ul>
     *   <li>Registers each fragment on the replay thread ({@link #dispatchAdmitAsync}) so
     *       {@link EgressCompletionTracker#register} does not consume offer-thread time.</li>
     *   <li>Hands offers to a dedicated {@code oms-venue-egress-route-offer} consumer thread via a
     *       {@link LinkedTransferQueue} (avoids per-admit {@code ExecutorService} runnable allocation).</li>
     *   <li>Does <em>not</em> wait for venue ack before dequeuing the next offer (permits bound
     *       in-flight stream depth only).</li>
     * </ul>
     * Batch {@code onNext} or multi-stream parallelism would require {@code VenueRouteOrderClient}
     * changes and breaks global FIFO unless the venue shards sequence per instrument.
     */
    private final class EgressRoutePipeline {

        /** Park between non-progress passes while quiescing the in-flight window. */
        private static final long QUIESCE_PARK_NANOS = 50_000L;

        /** Idle park for the dedicated route-offer consumer when the transfer queue is empty. */
        private static final long ROUTE_OFFER_IDLE_PARK_NANOS = 1_000L;

        /**
         * Spin-yield passes in {@link #awaitDispatchCapacity()} before paying a configured park
         * slice — completions {@link #unparkReplayThread()} frequently under 10k admit/s.
         */
        private static final int DISPATCH_CAPACITY_SPIN_LIMIT = 128;

        /**
         * Outstanding fragments (venue ack pending or ER submit pending) before we stop dispatching.
         * 6× venue permits keeps replay registering while ER submit trails venue acks (observed pop
         * @ 10k RPS clean book: 4× cap cut {@code egress_wall_lag_ms} 306→154 ms; 6× targets &lt;100 ms
         * meanRouteMs with raised ER-queue throttle floor).
         */
        private static final int PENDING_FRAGMENT_CAP_MULTIPLIER = 6;

        private final int maxInFlight;
        private final int maxPendingFragments;
        private final int backlogThrottlePendingRouteThreshold;
        private final int backlogThrottleErOfferQueueDepthThreshold;
        private final int backlogThrottleMaxInFlight;
        private final int backlogThrottleErSoftCapPermitMultiplier;
        private final long backlogThrottleParkNanos;
        private final Semaphore permits;
        private final EgressCompletionTracker tracker = new EgressCompletionTracker();
        private final java.util.concurrent.Executor erSubmitExecutor;
        private final ExecutorService ownedErSubmitExecutor;
        /** Persists the contiguous cursor off the ER-offer pool so JDBC does not starve offers. */
        private final java.util.concurrent.Executor cursorDrainExecutor;
        private final ExecutorService ownedCursorDrainExecutor;
        /**
         * Serial ordered {@code RouteOrderStream} writer. The replay thread enqueues admits here after
         * acquiring a venue-route permit so permit waits and gRPC {@code onNext} do not stall Aeron
         * poll (observed knee ~120→150 RPS: egress wall lag tracked ingress while venue
         * place_matched ~0.01 ms and ER offer ~0).
         */
        private final java.util.concurrent.Executor routeOfferExecutor;
        private final LinkedTransferQueue<PendingRouteOffer> routeOfferQueue;
        private final Thread routeOfferThread;
        private record PendingRouteOffer(OrderAdmittedEvent ev, long position) {}
        /** Serialises cursor persistence between the replay thread and cursor-drain completions. */
        private final Object contiguousDrainLock = new Object();
        /**
         * Coalesces {@link #scheduleDrainContiguous()} so the single-thread {@link #cursorDrainExecutor}
         * never queues one runnable per ER completion (observed on pop @ 200 RPS: thousands of
         * redundant {@code advanceWithRecording} calls inflated {@code egress_wall_lag_ms}).
         */
        private final AtomicBoolean cursorDrainScheduled = new AtomicBoolean(false);
        /**
         * Coalesces {@link #completeRoute} + {@link EgressCompletionTracker#complete} so the
         * virtual-thread ER pool queues one flush task per burst instead of one per venue ack
         * (observed @ 400 routes/s: per-completion tasks amplified ingress {@code clientLock}
         * churn even when {@code egress_ER_offer_ms} ~ 0).
         */
        private final ConcurrentLinkedQueue<ErCompletion> erCompletionQueue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean erCompletionFlushScheduled = new AtomicBoolean(false);

        private record ErCompletion(
                OrderAdmittedEvent ev,
                Optional<com.balh.venue.grpc.v1.ExecutionReport> erOpt,
                Throwable err,
                long position) {}

        EgressRoutePipeline(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            this.maxPendingFragments = maxInFlight * PENDING_FRAGMENT_CAP_MULTIPLIER;
            OmsConfig.Cluster.VenueEgress venueCfg = config.getCluster().getVenueEgress();
            this.backlogThrottlePendingRouteThreshold =
                    venueCfg.getBacklogThrottlePendingRouteThreshold();
            this.backlogThrottleErOfferQueueDepthThreshold =
                    venueCfg.getBacklogThrottleErOfferQueueDepthThreshold();
            this.backlogThrottleMaxInFlight =
                    Math.min(maxInFlight, venueCfg.getBacklogThrottleMaxInFlight());
            this.backlogThrottleErSoftCapPermitMultiplier =
                    venueCfg.getBacklogThrottleErSoftCapPermitMultiplier();
            this.backlogThrottleParkNanos = venueCfg.getBacklogThrottleParkNanos();
            this.permits = new Semaphore(maxInFlight);
            // Virtual-thread per ER completion: cluster offer back-pressure parks the carrier
            // without exhausting a fixed platform pool (observed @ 150+ admits/s with
            // maxInFlight=512 — platform pool threads blocked in offerWithBackpressure stalled
            // tracker.complete and wedged awaitDispatchCapacity).
            ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
            this.erSubmitExecutor = pool;
            this.ownedErSubmitExecutor = pool;
            ExecutorService cursorPool =
                    Executors.newSingleThreadExecutor(
                            r -> {
                                Thread t = new Thread(r, "oms-venue-egress-cursor-drain");
                                t.setDaemon(true);
                                return t;
                            });
            this.cursorDrainExecutor = cursorPool;
            this.ownedCursorDrainExecutor = cursorPool;
            this.routeOfferQueue = new LinkedTransferQueue<>();
            this.routeOfferExecutor = null;
            Thread offerThread = new Thread(this::routeOfferLoop, "oms-venue-egress-route-offer");
            offerThread.setDaemon(true);
            applyReplayThreadPriority(offerThread, venueCfg.getReplayThreadPriority());
            offerThread.start();
            this.routeOfferThread = offerThread;
            meterRegistry.gauge("oms.venue.egress.pipeline.pending.routes", tracker, EgressCompletionTracker::inFlight);
        }

        /** Test seam: inject executors for deterministic ER completion and cursor-drain isolation. */
        EgressRoutePipeline(
                int maxInFlight,
                java.util.concurrent.Executor erSubmitExecutor,
                java.util.concurrent.Executor cursorDrainExecutor) {
            this(maxInFlight, erSubmitExecutor, cursorDrainExecutor, erSubmitExecutor);
        }

        /** Test seam: optional dedicated route-offer executor (caller-runs keeps replay-thread dispatch). */
        EgressRoutePipeline(
                int maxInFlight,
                java.util.concurrent.Executor erSubmitExecutor,
                java.util.concurrent.Executor cursorDrainExecutor,
                java.util.concurrent.Executor routeOfferExecutor) {
            this.maxInFlight = maxInFlight;
            this.maxPendingFragments = maxInFlight * PENDING_FRAGMENT_CAP_MULTIPLIER;
            OmsConfig.Cluster.VenueEgress venueCfg = config.getCluster().getVenueEgress();
            this.backlogThrottlePendingRouteThreshold =
                    venueCfg.getBacklogThrottlePendingRouteThreshold();
            this.backlogThrottleErOfferQueueDepthThreshold =
                    venueCfg.getBacklogThrottleErOfferQueueDepthThreshold();
            this.backlogThrottleMaxInFlight =
                    Math.min(maxInFlight, venueCfg.getBacklogThrottleMaxInFlight());
            this.backlogThrottleErSoftCapPermitMultiplier =
                    venueCfg.getBacklogThrottleErSoftCapPermitMultiplier();
            this.backlogThrottleParkNanos = venueCfg.getBacklogThrottleParkNanos();
            this.permits = new Semaphore(maxInFlight);
            this.erSubmitExecutor = erSubmitExecutor;
            this.ownedErSubmitExecutor = null;
            this.cursorDrainExecutor = cursorDrainExecutor;
            this.ownedCursorDrainExecutor = null;
            this.routeOfferExecutor = routeOfferExecutor;
            this.routeOfferQueue = null;
            this.routeOfferThread = null;
            meterRegistry.gauge("oms.venue.egress.pipeline.pending.routes", tracker, EgressCompletionTracker::inFlight);
        }

        /**
         * Production consumer: strictly serial {@code dispatchRoute} / {@code onNext} drain.
         * Must not exit when {@code running} is still false at pipeline construction — the thread
         * starts before {@link OmsVenueEgressService#running} is set true in {@code init()}.
         */
        private void routeOfferLoop() {
            while (true) {
                PendingRouteOffer item = routeOfferQueue.poll();
                if (item == null) {
                    if (!running.get() && routeOfferQueue.isEmpty()) {
                        return;
                    }
                    LockSupport.parkNanos(ROUTE_OFFER_IDLE_PARK_NANOS);
                    continue;
                }
                do {
                    try {
                        dispatchRoute(item.ev(), item.position());
                    } catch (RuntimeException e) {
                        log.error(
                                "oms-venue-egress route-offer failed orderId={} position={}",
                                item.ev().orderId(),
                                item.position(),
                                e);
                        tracker.complete(item.position());
                        permits.release();
                        unparkReplayThread();
                    }
                } while ((item = routeOfferQueue.poll()) != null);
            }
        }

        private void enqueueRouteOffer(OrderAdmittedEvent ev, long newPosition) {
            if (routeOfferQueue != null) {
                routeOfferQueue.offer(new PendingRouteOffer(ev, newPosition));
                LockSupport.unpark(routeOfferThread);
                return;
            }
            routeOfferExecutor.execute(() -> dispatchRoute(ev, newPosition));
        }

        /** Bounds total pipeline depth when venue permits are released before ER submit completes. */
        private void awaitDispatchCapacity() {
            int idleSpins = 0;
            while (true) {
                int inFlight = tracker.inFlight();
                int effectiveCap = effectiveDispatchCapacity(inFlight, currentErOfferQueueDepth());
                if (inFlight < effectiveCap) {
                    return;
                }
                drainContiguous();
                if (!running.get()) {
                    return;
                }
                if (idleSpins < DISPATCH_CAPACITY_SPIN_LIMIT) {
                    idleSpins++;
                    Thread.onSpinWait();
                    continue;
                }
                idleSpins = 0;
                LockSupport.parkNanos(backlogThrottleParkNanos);
            }
        }

        private int currentErOfferQueueDepth() {
            return clusterIngressClient == null ? 0 : clusterIngressClient.erOfferQueueDepth();
        }

        /**
         * Max registered fragments the replay thread may dispatch before {@link #awaitDispatchCapacity()}
         * parks. Normally {@link #maxPendingFragments} (6× venue permits) so replay keeps registering
         * admits while venue acks recycle permits faster than ER submit completes — capping at
         * {@link #maxInFlight} alone wedged replay at ~512 deep with free permits (observed pop @ 5k
         * RPS: {@code egress_wall_lag_ms} ~27 ms; @ 10k with 2× cap: ~306 ms,
         * {@code egress_route_service_ms} ~0.01 ms).
         * Backlog throttle applies only when the venue permit pool is exhausted or the ER offer queue
         * is deep.
         */
        private int effectiveDispatchCapacity(int inFlight, int erOfferQueueDepth) {
            boolean erOfferBacklogged =
                    erOfferQueueDepth >= backlogThrottleErOfferQueueDepthThreshold;
            boolean venuePermitsExhausted = permits.availablePermits() == 0;
            boolean routeBacklogged =
                    inFlight >= backlogThrottlePendingRouteThreshold && venuePermitsExhausted;
            if (routeBacklogged || (erOfferBacklogged && venuePermitsExhausted)) {
                return Math.max(1, Math.min(maxPendingFragments, backlogThrottleMaxInFlight));
            }
            if (erOfferBacklogged) {
                // ER deep but venue still has permits: graduated soft cap — full 6× inflated
                // meanRouteMs on 10k soak after an 8k step (observed 614 ms); hard 512 cap inflated
                // egress_wall_lag on clean 10k profile (observed 601 ms pre-fix).
                int erBacklogCap =
                        Math.min(
                                maxPendingFragments,
                                maxInFlight * backlogThrottleErSoftCapPermitMultiplier);
                return Math.max(backlogThrottleMaxInFlight, erBacklogCap);
            }
            return maxPendingFragments;
        }

        /** Called on the replay thread for each polled fragment, in cluster-log order. */
        void onFragment(int typeId, org.agrona.DirectBuffer buffer, int offset, int length, long newPosition) {
            if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED) {
                OrderAdmittedEvent ev = OrderAdmittedEvent.decode(buffer, offset, length);
                if (venueRouteOrderClient != null
                        && clusterIngressClient != null
                        && VenueRoutingSymbols.routesToInternalVenue(config, ev.instrumentSymbol())) {
                    dispatchAdmitAsync(ev, newPosition);
                    return;
                }
                // Non-venue admit: cursor-only, but still ordered behind in-flight routes.
                applySyncAfterQuiesce(() -> applyAdmittedEvent(ev, newPosition), newPosition);
                return;
            }
            if (isCursorOnlyFragment(typeId)) {
                // ExecutionApplied and other no-op events must not quiesce the replay thread — each
                // ER would block dispatch of later admits while unrelated routes drain (observed on pop:
                // meanRouteMs >> 1s on a loaded book at 200+ RPS). Checkpoints flush via
                // drainContiguous once prior admits complete.
                tracker.registerCursorOnly(newPosition);
                drainContiguous();
                return;
            }
            applySyncAfterQuiesce(
                    () -> applyNonAdmitFragmentSync(typeId, buffer, offset, length, newPosition), newPosition);
        }

        private boolean isCursorOnlyFragment(int typeId) {
            return switch (typeId) {
                case OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_REQUESTED,
                        OmsClusterWireFormat.TYPE_ID_ORDER_REPLACE_REQUESTED -> false;
                default -> true;
            };
        }

        private void dispatchAdmitAsync(OrderAdmittedEvent ev, long newPosition) {
            awaitDispatchCapacity();
            // Register on the replay thread (poll order) before the offer consumer writes onNext —
            // keeps tracker bookkeeping off the single-writer hot path. Venue-route permits are
            // acquired on the route-offer consumer so Aeron poll does not park on gRPC/venue acks.
            tracker.register(newPosition);
            enqueueRouteOffer(ev, newPosition);
        }

        /**
         * Runs on the sole route-offer consumer — the only {@code RouteOrderStream} writer. Offers to
         * the venue and hands acks to the ER pool without blocking the replay poll loop on gRPC stream
         * backpressure.
         */
        private void dispatchRoute(OrderAdmittedEvent ev, long newPosition) {
            try {
                permits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                unparkReplayThread();
                return; // shutdown — fragment replays from the cursor on restart
            }
            CompletableFuture<Optional<com.balh.venue.grpc.v1.ExecutionReport>> future;
            try {
                future = venueRouteOrderClient.routeAdmittedOrderAsync(ev);
            } catch (RuntimeException e) {
                permits.release();
                unparkReplayThread();
                erCompletionQueue.add(new ErCompletion(ev, Optional.empty(), e, newPosition));
                scheduleErCompletionFlush();
                return;
            }
            // Release the venue-route permit as soon as the venue acks so the route-offer consumer
            // can keep writing RouteOrders while ER cluster offers run on a separate pool; the cursor
            // still advances only after ER submit completes (tracker.complete below).
            future.whenComplete(
                    (erOpt, err) -> {
                        permits.release();
                        unparkReplayThread();
                        erCompletionQueue.add(new ErCompletion(ev, erOpt, err, newPosition));
                        scheduleErCompletionFlush();
                    });
        }

        /**
         * Queues at most one coalesced ER-completion pass on {@link #erSubmitExecutor}. Further venue
         * acks while a flush is already scheduled piggyback on that single task.
         */
        private void scheduleErCompletionFlush() {
            if (!erCompletionFlushScheduled.compareAndSet(false, true)) {
                return;
            }
            erSubmitExecutor.execute(this::runScheduledErCompletionFlush);
        }

        /**
         * Runs on {@link #erSubmitExecutor}. Drains the completion queue in poll order and enqueues
         * each ER via {@link OmsClusterIngressClient#submitApplyExecutionReportAsync} without blocking
         * this thread on cluster offer back-pressure — the coalesced flush can fill
         * {@code erOfferQueue} in one pass so the ingress ER daemon amortises {@code clientLock} trips.
         * {@link #onErSubmitSucceeded} advances the tracker when each async offer completes.
         */
        private void runScheduledErCompletionFlush() {
            try {
                ErCompletion item;
                while ((item = erCompletionQueue.poll()) != null) {
                    if (!dispatchErCompletionAsync(item)) {
                        // Mirror serial replay semantics: stop this flush pass so a synchronously
                        // failed offer is not immediately re-polled in the same loop.
                        erCompletionQueue.add(item);
                        break;
                    }
                }
            } finally {
                erCompletionFlushScheduled.set(false);
                if (!erCompletionQueue.isEmpty()) {
                    scheduleErCompletionFlush();
                }
            }
        }

        /**
         * Non-blocking ER hand-off for the pipelined path. VENUE_REJECT retries still run on
         * {@link #erSubmitExecutor} because they need a bounded multi-attempt loop.
         *
         * @return {@code false} when the cluster offer failed synchronously (caller re-queues and
         *         stops the flush pass); {@code true} when the offer was enqueued or completed inline
         */
        private boolean dispatchErCompletionAsync(ErCompletion item) {
            if (item.err() != null || item.erOpt().isEmpty()) {
                erSubmitExecutor.execute(
                        () -> {
                            boolean applied = completeRoute(item.ev(), item.erOpt(), item.err());
                            if (applied) {
                                onErSubmitSucceeded(item);
                            } else {
                                erCompletionQueue.add(item);
                            }
                        });
                return true;
            }
            ApplyExecutionReportCommand cmd =
                    VenueGrpcExecutionReportMapper.toApplyCommand(
                            item.erOpt().get(), config.getVenue().getVenueId(), objectMapper);
            Duration submitTimeout =
                    Duration.ofMillis(config.getCluster().getClient().getSubmitTimeoutMs());
            CompletableFuture<Void> submitFuture =
                    clusterIngressClient.submitApplyExecutionReportAsync(cmd, submitTimeout);
            if (!submitFuture.isDone()) {
                submitFuture.whenComplete(
                        (v, err) -> {
                            if (err != null) {
                                log.warn(
                                        "oms-venue-egress (pipelined): cluster ER submit failed for orderId={}",
                                        item.ev().orderId(),
                                        err);
                                erCompletionQueue.add(item);
                                scheduleErCompletionFlush();
                                return;
                            }
                            recordErSubmitMetrics(item);
                            onErSubmitSucceeded(item);
                        });
                return true;
            }
            if (submitFuture.isCompletedExceptionally()) {
                log.warn(
                        "oms-venue-egress (pipelined): cluster ER submit failed for orderId={}",
                        item.ev().orderId(),
                        submitFuture.exceptionNow());
                return false;
            }
            recordErSubmitMetrics(item);
            onErSubmitSucceeded(item);
            return true;
        }

        private void recordErSubmitMetrics(ErCompletion item) {
            long latencyMs = wallClock.millis() - item.ev().acceptedAtMillis();
            OmsPipelineMetrics.recordClusterAdmitToFixNos(
                    meterRegistry,
                    EGRESS_ID,
                    item.ev().side(),
                    item.ev().timeInForceCode(),
                    latencyMs);
        }

        private void onErSubmitSucceeded(ErCompletion item) {
            tracker.complete(item.position());
            unparkReplayThread();
            scheduleDrainContiguous();
        }

        /**
         * Queues at most one contiguous-prefix cursor flush on {@link #cursorDrainExecutor}. Further
         * ER completions while a drain is already scheduled coalesce into that single pass — each pass
         * persists only the latest contiguous position ({@link #drainContiguous()}).
         */
        private void scheduleDrainContiguous() {
            if (!cursorDrainScheduled.compareAndSet(false, true)) {
                return;
            }
            cursorDrainExecutor.execute(this::runScheduledCursorDrain);
        }

        /**
         * Runs on {@link #cursorDrainExecutor}. Drains repeatedly until the contiguous prefix is
         * fully persisted and no coalesced schedules remain — one queued task absorbs many ER
         * completions instead of one JDBC per completion.
         */
        private void runScheduledCursorDrain() {
            while (true) {
                long before = lastAppliedPosition.get();
                drainContiguous();
                boolean progressed = lastAppliedPosition.get() > before;
                boolean rescheduled = cursorDrainScheduled.getAndSet(false);
                if (!rescheduled && !progressed) {
                    return;
                }
            }
        }

        /**
         * Runs on the completion executor — applies the venue ack to the OMS cluster.
         *
         * @return {@code true} when the cluster ER was submitted (fix_nos timer recorded);
         *         {@code false} when submit failed and the fragment must stay in-flight for retry
         */
        private boolean completeRoute(
                OrderAdmittedEvent ev,
                Optional<com.balh.venue.grpc.v1.ExecutionReport> erOpt,
                Throwable err) {
            if (err != null) {
                log.warn(
                        "oms-venue-egress (pipelined): venue RouteOrder failed; submitting VENUE_REJECT: orderId={} symbol={}",
                        ev.orderId(),
                        ev.instrumentSymbol(),
                        err);
                return submitVenueRouteFailedReject(
                        ev.orderId(),
                        "venue_route_transport_failed",
                        ev.acceptedAtMillis(),
                        ev.side(),
                        ev.timeInForceCode());
            }
            if (erOpt.isEmpty()) {
                log.warn(
                        "oms-venue-egress (pipelined): venue RouteOrder rejected (no ER); submitting VENUE_REJECT: orderId={} symbol={}",
                        ev.orderId(),
                        ev.instrumentSymbol());
                return submitVenueRouteFailedReject(
                        ev.orderId(),
                        "venue_route_rejected",
                        ev.acceptedAtMillis(),
                        ev.side(),
                        ev.timeInForceCode());
            }
            return submitVenueExecutionReport(
                    ev.orderId(), erOpt, ev.acceptedAtMillis(), ev.side(), ev.timeInForceCode());
        }

        private boolean applyNonAdmitFragmentSync(
                int typeId, org.agrona.DirectBuffer buffer, int offset, int length, long newPosition) {
            return switch (typeId) {
                case OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_REQUESTED -> applyCancelRequestedEvent(
                        OrderCancelRequestedEvent.decode(buffer, offset, length), newPosition);
                case OmsClusterWireFormat.TYPE_ID_ORDER_REPLACE_REQUESTED -> applyReplaceRequestedEvent(
                        OrderReplaceRequestedEvent.decode(buffer, offset, length), newPosition);
                default -> applyCursorOnly(newPosition);
            };
        }

        /**
         * Drain all in-flight routes, then apply {@code syncApply} (a serial apply that advances the
         * cursor inline). Used for every non-async fragment so it lands strictly after the routes that
         * precede it. If shutdown is requested mid-drain the fragment is not applied and replays from
         * the cursor on restart.
         */
        private void applySyncAfterQuiesce(java.util.function.BooleanSupplier syncApply, long newPosition) {
            if (!quiesce()) {
                return;
            }
            if (syncApply.getAsBoolean()) {
                lastAppliedPosition.set(newPosition);
            }
        }

        void registerCursorOnlyCheckpoint(long newPosition) {
            tracker.registerCursorOnly(newPosition);
            drainContiguous();
        }

        /**
         * True when the replay thread should avoid a configured idle park and immediately re-poll
         * the Aeron subscription — in-flight routes or queued offers still need drain capacity.
         */
        boolean replayPollHasBacklog() {
            if (!tracker.isDrained()) {
                return true;
            }
            if (!erCompletionQueue.isEmpty()) {
                return true;
            }
            return routeOfferQueue != null && !routeOfferQueue.isEmpty();
        }

        /** Advance the cursor over whatever contiguous prefix has completed since last call. */
        void drainContiguous() {
            synchronized (contiguousDrainLock) {
                OptionalLong contiguous = tracker.pollContiguous();
                if (contiguous.isPresent()) {
                    advanceCursorContiguous(contiguous.getAsLong());
                }
            }
        }

        /**
         * Block (advancing the cursor as completions arrive) until every dispatched route has completed
         * and the cursor reflects the drained position. Returns {@code false} if shutdown was requested
         * before the drain finished.
         */
        boolean quiesce() {
            while (true) {
                drainContiguous();
                if (tracker.isDrained()) {
                    return true;
                }
                if (!running.get()) {
                    return false;
                }
                LockSupport.parkNanos(QUIESCE_PARK_NANOS);
            }
        }

        void shutdown() {
            if (routeOfferThread != null) {
                routeOfferThread.interrupt();
                try {
                    routeOfferThread.join(2_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (ownedErSubmitExecutor != null) {
                ownedErSubmitExecutor.shutdownNow();
            }
            if (ownedCursorDrainExecutor != null) {
                ownedCursorDrainExecutor.shutdownNow();
            }
        }
    }

    // ---- Test seams for the pipeline (production drives it via init() + EgressFragmentHandler) ----

    void enablePipelineForTesting(int maxInFlight, java.util.concurrent.Executor completionExecutor) {
        enablePipelineForTesting(maxInFlight, completionExecutor, completionExecutor);
    }

    void enablePipelineForTesting(
            int maxInFlight,
            java.util.concurrent.Executor erSubmitExecutor,
            java.util.concurrent.Executor cursorDrainExecutor) {
        enablePipelineForTesting(maxInFlight, erSubmitExecutor, cursorDrainExecutor, erSubmitExecutor);
    }

    void enablePipelineForTesting(
            int maxInFlight,
            java.util.concurrent.Executor erSubmitExecutor,
            java.util.concurrent.Executor cursorDrainExecutor,
            java.util.concurrent.Executor routeOfferExecutor) {
        this.pipeline =
                new EgressRoutePipeline(maxInFlight, erSubmitExecutor, cursorDrainExecutor, routeOfferExecutor);
    }

    void markRunningForTesting() {
        running.set(true);
    }

    void setReplayPollConfigForTesting(int fragmentLimit, long pollParkNanos) {
        this.replayFragmentLimit = fragmentLimit;
        this.replayPollParkNanos = pollParkNanos;
    }

    EgressRoutePipeline pipelineForTesting() {
        return pipeline;
    }

    void pipelineDispatchAdmitForTesting(OrderAdmittedEvent ev, long newPosition) {
        pipeline.dispatchAdmitAsync(ev, newPosition);
    }

    void pipelineRegisterCursorOnlyForTesting(long newPosition) {
        pipeline.registerCursorOnlyCheckpoint(newPosition);
    }

    void pipelineDrainContiguousForTesting() {
        pipeline.drainContiguous();
    }

    boolean pipelineQuiesceForTesting() {
        return pipeline.quiesce();
    }

    boolean pipelineIsDrainedForTesting() {
        return pipeline.tracker.isDrained();
    }

    int pipelineInFlightForTesting() {
        return pipeline.tracker.inFlight();
    }

    /**
     * Wakes the replay thread when ER completions drain {@link EgressCompletionTracker#inFlight()}
     * so {@link EgressRoutePipeline#awaitDispatchCapacity()} does not sit a full park slice behind
     * finished offers.
     */
    private void unparkReplayThread() {
        Thread t = replayThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }
}
