package com.balh.oms.projector;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.ExecutionAppliedEvent;
import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.cluster.OmsClusterEventsRecordingSupport;
import com.balh.oms.cluster.OmsClusterEventsRecordingSupport.BootstrapPick;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.cluster.OrderCancelAppliedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.marketdata.MarketdataNbboQuote;
import com.balh.oms.marketdata.MarketdataPlatformHttpClient;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.MarketContextRepository;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.balh.oms.persistence.SellFillPositionSplit;
import com.balh.oms.risk.BuyFundsRequirement;
import com.balh.oms.returnpath.ExecutionTradeCommand;
import com.balh.oms.returnpath.MarketContextVenueEvidence;
import com.balh.oms.settlement.SettlementDateCalculator;
import com.balh.oms.tailer.OrderControlAdmission;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 *       {@code domain_event_outbox} envelope (slice 2d; slice 3g removed the legacy
 *       {@code OutboxReconciler} → Chronicle → tail-applier path entirely);</li>
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

    /**
     * Scaling factor for {@link ExecutionAppliedEvent#newCumQtyScaled()} and
     * {@link ExecutionAppliedEvent#lastQtyScaled()} (1e9 fixed-point — matches
     * {@link ApplyExecutionReportCommand} and {@link OrderAdmittedEvent#quantityScaled()}).
     */
    private static final BigDecimal QUANTITY_SCALE = BigDecimal.valueOf(1_000_000_000L);

    /**
     * Scaling factor for {@link ExecutionAppliedEvent#lastPxScaled()} (1e6 fixed-point — matches
     * {@link ApplyExecutionReportCommand}).
     */
    private static final BigDecimal PRICE_SCALE = BigDecimal.valueOf(1_000_000L);

    // Slice 3e-2: keep the legacy applier's metric names so existing Grafana dashboards
    // (oms_executions_applied_total, oms_order_filled_events_published_total,
    // oms_free_riding_attribution_merges_total, oms.trade.apply, oms.marketdata.nbbo.fetch)
    // continue working when the projector becomes the sole writer. Names mirror
    // ExecutionReportApplier verbatim — operations should not be able to tell which path emitted
    // the metric, only that the count moved when an ER was applied.
    private static final String METRIC_EXECUTIONS_APPLIED = "oms_executions_applied_total";
    private static final String TAG_OUTCOME = "outcome";
    private static final String OUTCOME_INSERTED = "inserted";
    private static final String OUTCOME_DUPLICATE = "duplicate";
    private static final String METRIC_ORDER_FILLED_EVENTS = "oms_order_filled_events_published_total";
    private static final String METRIC_FREE_RIDING_ATTRIBUTION = "oms_free_riding_attribution_merges_total";
    private static final String TIMER_TRADE_APPLY = "oms.trade.apply";
    private static final String TIMER_NBBO_FETCH = "oms.marketdata.nbbo.fetch";

    /**
     * Phase 4 Tier 2.5 phase E-2 — defensive shard guard. Increments when an
     * {@link OrderAdmittedEvent} arrives whose {@link OrderAdmittedEvent#shardId()} does not
     * match {@link OmsConfig.Shard#getId() this projector's shard id}. At {@code shardCount=1}
     * always {@code 0} (cluster log only carries shard-0 events and the projector defaults to
     * shard 0). Pre-registered so {@code /actuator/prometheus} shows the series with count 0
     * on a freshly booted JVM and Prometheus alerts on {@code rate(...) > 0} match cleanly.
     */
    static final String METRIC_SHARD_MISMATCH_DROPPED = "oms_projector_shard_mismatch_dropped_total";

    private final OmsConfig config;
    private final AeronProjectorCursorRepository cursorRepository;
    private final OrdersRepository ordersRepository;
    private final OrderControlAdmission controlAdmission;
    private final ExecutionsRepository executionsRepository;
    private final DomainEventOutboxRepository domainEventOutboxRepository;
    private final LedgerInflightOutboxRepository ledgerInflightOutboxRepository;
    private final DomainEventEnvelopeCodec envelopeCodec;
    private final MarketContextRepository marketContextRepository;
    private final PositionsRepository positionsRepository;
    private final ObjectProvider<MarketdataPlatformHttpClient> marketdataHttp;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final Clock wallClock;
    /**
     * Stock-settlement gap plan §5.3 Slice 1 — derives {@code trade_date} and
     * {@code expected_settlement_date} for each TRADE row at projector time so the matcher
     * (and downstream UI) can reason about settlement calendar correctness without rederiving
     * both dates on every comparison. Placeholder default cycle today; the eventual rule is
     * profile-driven.
     */
    private final SettlementDateCalculator settlementDateCalculator;
    /**
     * Phase 4 Tier 2.5 phase E-2 — pre-registered counter for the defensive shard guard in
     * {@link #applyAdmittedEvent}. See {@link #METRIC_SHARD_MISMATCH_DROPPED} for the contract.
     */
    private final Counter shardMismatchCounter;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Long> lastAppliedPosition = new AtomicReference<>(0L);
    /**
     * 2026-05-23 hardening (handover §9). Aeron Archive recording id this projector is currently
     * replaying. The {@link ProjectingFragmentHandler} reads this when it advances the cursor so
     * the persisted {@code (recording_id, position)} pair always travels together.
     *
     * <p>{@code -1} means "not yet set" — the replay loop populates it before opening the first
     * replay subscription. A value of {@code -1} reaching the handler is a programming bug and
     * the handler will refuse to advance.
     */
    private final AtomicLong currentRecordingId = new AtomicLong(-1L);
    /**
     * 2026-05-23 hardening. When non-empty, {@link #init} stashes the resume cursor here and the
     * replay loop honors it as the start position; on a fresh first-ever start it stays empty and
     * the replay loop bootstraps from the oldest available recording at position 0.
     */
    private final AtomicReference<AeronProjectorCursorRepository.RecordedCursor> startupCursor =
            new AtomicReference<>(null);
    private Thread replayThread;

    @Autowired
    public OmsPostgresProjector(
            OmsConfig config,
            AeronProjectorCursorRepository cursorRepository,
            OrdersRepository ordersRepository,
            OrderControlAdmission controlAdmission,
            ExecutionsRepository executionsRepository,
            DomainEventOutboxRepository domainEventOutboxRepository,
            LedgerInflightOutboxRepository ledgerInflightOutboxRepository,
            DomainEventEnvelopeCodec envelopeCodec,
            MarketContextRepository marketContextRepository,
            PositionsRepository positionsRepository,
            ObjectProvider<MarketdataPlatformHttpClient> marketdataHttp,
            MeterRegistry meterRegistry,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager,
            SettlementDateCalculator settlementDateCalculator) {
        this(
                config,
                cursorRepository,
                ordersRepository,
                controlAdmission,
                executionsRepository,
                domainEventOutboxRepository,
                ledgerInflightOutboxRepository,
                envelopeCodec,
                marketContextRepository,
                positionsRepository,
                marketdataHttp,
                meterRegistry,
                objectMapper,
                transactionManager,
                Clock.systemUTC(),
                settlementDateCalculator);
    }

    /**
     * Visible for tests: inject a deterministic {@link Clock} so unit tests can pin
     * {@code System.currentTimeMillis()}-equivalent values when asserting on the per-event
     * admit-to-projector Timer (Phase 4j). Production wiring uses {@code Clock.systemUTC()}.
     */
    OmsPostgresProjector(
            OmsConfig config,
            AeronProjectorCursorRepository cursorRepository,
            OrdersRepository ordersRepository,
            OrderControlAdmission controlAdmission,
            ExecutionsRepository executionsRepository,
            DomainEventOutboxRepository domainEventOutboxRepository,
            LedgerInflightOutboxRepository ledgerInflightOutboxRepository,
            DomainEventEnvelopeCodec envelopeCodec,
            MarketContextRepository marketContextRepository,
            PositionsRepository positionsRepository,
            ObjectProvider<MarketdataPlatformHttpClient> marketdataHttp,
            MeterRegistry meterRegistry,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager,
            Clock wallClock,
            SettlementDateCalculator settlementDateCalculator) {
        this.config = config;
        this.cursorRepository = cursorRepository;
        this.ordersRepository = ordersRepository;
        this.controlAdmission = controlAdmission;
        this.executionsRepository = executionsRepository;
        this.domainEventOutboxRepository = domainEventOutboxRepository;
        this.ledgerInflightOutboxRepository = ledgerInflightOutboxRepository;
        this.envelopeCodec = envelopeCodec;
        this.marketContextRepository = marketContextRepository;
        this.positionsRepository = positionsRepository;
        this.marketdataHttp = marketdataHttp;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
        this.wallClock = wallClock;
        this.settlementDateCalculator = settlementDateCalculator;
        this.shardMismatchCounter = Counter.builder(METRIC_SHARD_MISMATCH_DROPPED)
                .description(
                        "Cluster events whose shard id did not match this projector's "
                                + "OmsConfig.Shard.id. Indicates a config bug (projector wired to the "
                                + "wrong cluster's events recording) — should be 0 in steady state.")
                .register(meterRegistry);
        // Programmatic boundary: the replay loop is a non-Spring thread, so AOP-proxied
        // @Transactional on this bean's own methods would not be intercepted. A
        // TransactionTemplate guarantees orders.insert + persistAdmission + cursor.advance
        // commit (or roll back) as one unit per fragment.
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @PostConstruct
    void init() {
        // 2026-05-23 hardening (handover §9). Load the saved cursor in its full
        // (recording_id, position) shape. Three cases:
        //
        //   1. Empty Optional      — first-ever projector start (no row in aeron_projector_cursor).
        //                             Replay loop will bootstrap from the oldest recording at pos 0.
        //   2. Legacy NULL row     — row exists but predates V55 (no recording id). The
        //                             pre-V55 replay loop silently clamped position to 0 of the
        //                             current recording when the saved position overshot, losing
        //                             the pointer to events in earlier recordings. We refuse to
        //                             start until an operator pins the recording id explicitly
        //                             via SQL (instruction in the exception message).
        //   3. Recording-aware row — resume from the saved (recording_id, position) directly.
        Optional<AeronProjectorCursorRepository.RecordedCursor> savedCursor = cursorRepository
                .findLastAppliedCursor(PROJECTOR_ID, OmsClusterWireFormat.EVENTS_STREAM_ID);
        if (savedCursor.isPresent() && !savedCursor.get().hasRecordingId()) {
            long legacyPos = savedCursor.get().position();
            String repairSql = String.format(
                    "UPDATE aeron_projector_cursor SET last_applied_recording_id = <pick the recording id matching the saved position %d>"
                            + " WHERE projector_id = '%s' AND stream_id = %d;",
                    legacyPos, PROJECTOR_ID, OmsClusterWireFormat.EVENTS_STREAM_ID);
            throw new IllegalStateException(
                    "oms-postgres-projector refuses to start: aeron_projector_cursor row exists but"
                            + " last_applied_recording_id IS NULL (pre-V55 schema or pre-2026-05-23-hardening write)."
                            + " Without the recording id the projector cannot tell which Aeron Archive recording the"
                            + " saved position " + legacyPos + " refers to, and the old code path silently reset to"
                            + " position 0 of the current recording, losing visibility into events from prior cluster"
                            + " incarnations. Operator must pick the correct recording id (ls"
                            + " /opt/oms/aeron-archive/data/*-events*.rec, or psql to check which recording was"
                            + " current at the moment the saved position was written) and run: "
                            + repairSql
                            + " — see system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md §9.");
        }
        savedCursor.ifPresent(startupCursor::set);
        if (savedCursor.isPresent()) {
            AeronProjectorCursorRepository.RecordedCursor c = savedCursor.get();
            lastAppliedPosition.set(c.position());
            currentRecordingId.set(c.recordingId());
            log.info(
                    "oms-postgres-projector starting; resuming from recording {} at log position {} (projectorId={}, streamId={})",
                    c.recordingId(),
                    c.position(),
                    PROJECTOR_ID,
                    OmsClusterWireFormat.EVENTS_STREAM_ID);
        } else {
            log.info(
                    "oms-postgres-projector starting fresh (no saved cursor); will bootstrap from the first"
                            + " non-empty events recording at its start position (projectorId={}, streamId={})",
                    PROJECTOR_ID,
                    OmsClusterWireFormat.EVENTS_STREAM_ID);
        }
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

    /**
     * Visible for tests that drive {@link #applyAdmittedEvent} / {@link #applyExecutionAppliedEvent} /
     * {@link #applyOrderCancelAppliedEvent} directly (bypassing {@link #init} and the replay loop
     * that would normally seed the recording id from the Aeron Archive). Production code never
     * calls this — the replay loop owns the field.
     */
    void setCurrentRecordingIdForTesting(long recordingId) {
        currentRecordingId.set(recordingId);
    }

    /**
     * 2026-05-23 hardening (handover §9). Rewritten to walk all events recordings sorted by id
     * instead of unconditionally picking the highest-id recording and silently clamping the
     * saved cursor to its start position.
     *
     * <p>The pre-hardening loop assumed there was only ever one events recording. In practice
     * Aeron Archive opens a new recording every time the cluster process restarts (each cluster
     * lifetime owns one recording). The old "find highest-id recording, clamp cursor to its
     * bounds" path silently lost visibility into events from earlier recordings on every restart
     * — on pop, that destroyed the projector's pointer to 9 working orders whose admit events
     * lived in an older recording.
     *
     * <p>The new loop:
     *
     * <ol>
     *   <li>Bootstraps {@link #currentRecordingId} from {@link #startupCursor} (loaded by
     *       {@link #init}) or, on a first-ever start, picks the lowest-id <em>non-empty</em>
     *       events recording and persists {@code (id, startPosition)} via
     *       {@link AeronProjectorCursorRepository#resetWithRecording} (empty tombstones such as
     *       recording {@code 0} after a cluster wipe are skipped).</li>
     *   <li>Re-lists recordings, finds the descriptor for {@code currentRecordingId}, and
     *       opens a replay subscription at the saved position. <b>Fails loud</b> if the saved
     *       recording id is no longer in the Archive — this is an operator-fix-it condition,
     *       never a silent reset to position 0.</li>
     *   <li>Polls until the replay drains. When poll returns 0 AND the current recording has a
     *       finalized {@code stopPosition} AND a newer recording exists, persists
     *       {@code (nextId, 0)} via {@code resetWithRecording}, advances state, closes the
     *       current replay, and loops to open the next one. Otherwise parks briefly to wait
     *       for live-tail events on the current recording.</li>
     * </ol>
     *
     * <p>"Recording id 0 is allowed even with a much larger saved position" is intentional and
     * handled by the lex-monotonic guard inside {@code advanceWithRecording} (V55 repository
     * doc): a new recording always wins over an old recording regardless of within-recording
     * position. This is the cursor's promotion mechanism across cluster restarts.
     */
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
        try {
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(projectorCfg.getAeronDirectory()));
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel(projectorCfg.getArchiveControlRequestChannel())
                    .controlResponseChannel(projectorCfg.getArchiveControlResponseChannel()));

            // Bootstrap: if no saved cursor at startup, wait for the first recording to appear,
            // then persist (oldestId, 0) so subsequent restarts have a recording id to anchor on.
            if (startupCursor.get() == null) {
                if (!bootstrapFromOldestRecording(archive, projectorCfg.getRecordingLookupParkMs())) {
                    return; // shutdown requested during bootstrap
                }
            }

            runReplayLoopWithRecordingWalk(archive, projectorCfg);
        } catch (RuntimeException e) {
            // Loud failures from the recording-walk loop (saved recording id missing from Archive,
            // saved position past recording end, etc.) land here and stop the projector. Operators
            // see the stack trace and the diagnostic context in the exception message; restarting
            // without fixing the underlying state would re-throw immediately.
            log.error("oms-postgres-projector replay loop terminating", e);
        } finally {
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
            log.info("oms-postgres-projector replay loop stopped");
        }
    }

    /**
     * First-ever-start path: picks the lowest-id <em>non-empty</em> events recording on the
     * channel/stream (skipping empty tombstones such as recording {@code 0} after a cluster
     * wipe), persists {@code (id, startPosition)} as the initial cursor, and seeds
     * {@link #currentRecordingId} and {@link #lastAppliedPosition}.
     *
     * @return {@code true} if a recording was found and persisted; {@code false} if shutdown was
     *         requested before any recording appeared.
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
                            "oms-postgres-projector bootstrap: skipped {} empty events recording tombstone(s);"
                                    + " persisting initial cursor (recordingId={}, position={})"
                                    + " — first start on this projector, no prior cursor row.",
                            target.skippedEmptyTombstones(),
                            target.recordingId(),
                            target.startPosition());
                } else {
                    log.info(
                            "oms-postgres-projector bootstrap: persisting initial cursor (recordingId={}, position={})"
                                    + " — first start on this projector, no prior cursor row.",
                            target.recordingId(),
                            target.startPosition());
                }
                cursorRepository.resetWithRecording(
                        PROJECTOR_ID,
                        OmsClusterWireFormat.EVENTS_STREAM_ID,
                        target.recordingId(),
                        target.startPosition());
                currentRecordingId.set(target.recordingId());
                lastAppliedPosition.set(target.startPosition());
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
     * exit). When the projector reaches the {@code stopPosition} of the current recording AND
     * a recording with a higher id exists on the same stream, the projector promotes its cursor
     * to {@code (nextId, 0)} and re-opens replay against the new recording.
     */
    private void runReplayLoopWithRecordingWalk(AeronArchive archive, OmsConfig.Cluster.Projector projectorCfg) {
        FragmentHandler handler = new ProjectingFragmentHandler();
        while (running.get()) {
            long recordingIdNow = currentRecordingId.get();
            if (recordingIdNow < 0L) {
                throw new IllegalStateException(
                        "oms-postgres-projector replay loop entered with currentRecordingId="
                                + recordingIdNow
                                + "; init() / bootstrap should have set it. This is a programming bug.");
            }
            RecordingDescriptor descriptor = findRecordingById(archive, recordingIdNow);
            requireRecordingPresent(recordingIdNow, descriptor);
            long savedPosition = lastAppliedPosition.get();
            long upperBound = recordingUpperBound(archive, descriptor);
            requirePositionWithinRecording(recordingIdNow, savedPosition, upperBound, descriptor);
            if (savedPosition < descriptor.startPosition()) {
                // Saved position is below the recording's start. Reset upward to the recording's
                // start is safe because the projector's row writes are idempotent (orders ON CONFLICT,
                // executions unique on (account_id, venue_exec_ref)). Persist explicitly via
                // resetWithRecording so the cursor row reflects the new state.
                log.warn(
                        "Projector cursor below recording startPosition: saved=(recordingId={}, position={}),"
                                + " recordingStart={}; advancing to recording start (idempotent on row inserts).",
                        recordingIdNow,
                        savedPosition,
                        descriptor.startPosition());
                cursorRepository.resetWithRecording(
                        PROJECTOR_ID,
                        OmsClusterWireFormat.EVENTS_STREAM_ID,
                        recordingIdNow,
                        descriptor.startPosition());
                lastAppliedPosition.set(descriptor.startPosition());
                savedPosition = descriptor.startPosition();
            }

            // Empty tombstone recordings (post-wipe id=0 with stop=start=0) cannot be replayed.
            // Walk forward across any consecutive empty ids; park on a live empty tail until the
            // cluster writes its first event.
            while (running.get()
                    && OmsClusterEventsRecordingSupport.isEmptyRecording(
                            archive,
                            descriptor.recordingId(),
                            descriptor.startPosition(),
                            descriptor.stopPosition())) {
                RecordingDescriptor successor = findNextRecording(archive, recordingIdNow);
                if (successor != null) {
                    log.info(
                            "Projector skipping empty events recordingId={}; rolling forward to"
                                    + " recordingId={} startPosition={}.",
                            recordingIdNow,
                            successor.recordingId(),
                            successor.startPosition());
                    cursorRepository.resetWithRecording(
                            PROJECTOR_ID,
                            OmsClusterWireFormat.EVENTS_STREAM_ID,
                            successor.recordingId(),
                            successor.startPosition());
                    currentRecordingId.set(successor.recordingId());
                    lastAppliedPosition.set(successor.startPosition());
                    recordingIdNow = successor.recordingId();
                    descriptor = successor;
                    savedPosition = successor.startPosition();
                    continue;
                }
                LockSupport.parkNanos(projectorCfg.getPollParkNanos());
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
                            projectorCfg.getReplayChannel(),
                            projectorCfg.getReplayStreamId());
                } catch (RuntimeException e) {
                    if (OmsClusterEventsRecordingSupport.isEmptyRecordingReplayArchiveException(e)) {
                        RecordingDescriptor successor = findNextRecording(archive, recordingIdNow);
                        if (successor != null) {
                            log.warn(
                                    "Projector Archive rejected replay on empty recordingId={}; rolling forward"
                                            + " to recordingId={} startPosition={}.",
                                    recordingIdNow,
                                    successor.recordingId(),
                                    successor.startPosition());
                            cursorRepository.resetWithRecording(
                                    PROJECTOR_ID,
                                    OmsClusterWireFormat.EVENTS_STREAM_ID,
                                    successor.recordingId(),
                                    successor.startPosition());
                            currentRecordingId.set(successor.recordingId());
                            lastAppliedPosition.set(successor.startPosition());
                            continue;
                        }
                    }
                    throw e;
                }
                log.info(
                        "Projector replay open; recordingId={} startPos={} (recordingStart={}, recordingStop={},"
                                + " upperBound={}) channel={} streamId={}",
                        descriptor.recordingId(),
                        savedPosition,
                        descriptor.startPosition(),
                        descriptor.stopPosition(),
                        upperBound,
                        projectorCfg.getReplayChannel(),
                        projectorCfg.getReplayStreamId());

                while (running.get()) {
                    int polled = replay.poll(handler, projectorCfg.getFragmentLimit());
                    if (polled > 0) {
                        continue;
                    }
                    // No fragments. Three cases:
                    //   (a) live-tail wait on the cluster's currently-active recording — park,
                    //       keep polling so newly written events flow through.
                    //   (b) end of a completed recording (stopPosition set) with a successor
                    //       available — roll forward to the successor.
                    //   (c) stale-open recording from a crashed cluster session: the recording
                    //       was never properly closed (stopPosition = NULL_POSITION) but the
                    //       cluster has since restarted and is writing to a higher recordingId.
                    //       Without this branch the projector would park on the dead recording
                    //       forever and never pick up the new events. Detected as "a successor
                    //       exists on the same channel/stream" — the cluster only writes to one
                    //       recording at a time, so the existence of a higher recordingId on
                    //       the events stream means the current recording is no longer being
                    //       written to. The post-V55 pop incident on 2026-05-23 hit exactly
                    //       this: cursor pinned to recording 16 (stop=-1 from a crash), cluster
                    //       restarted into recordings 17, 18, ..., projector parked forever and
                    //       the `cluster=9 projector=0` open-orders drift never closed.
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
                        log.info(
                                "Projector recording boundary: recordingId={} {} at stopPosition={};"
                                        + " rolling forward to recordingId={} startPosition={}.",
                                recordingIdNow,
                                decision.reason,
                                recordingStop,
                                successor.recordingId(),
                                successor.startPosition());
                        cursorRepository.resetWithRecording(
                                PROJECTOR_ID,
                                OmsClusterWireFormat.EVENTS_STREAM_ID,
                                successor.recordingId(),
                                successor.startPosition());
                        currentRecordingId.set(successor.recordingId());
                        lastAppliedPosition.set(successor.startPosition());
                        break; // exit inner poll loop -> outer loop reopens against successor
                    }
                    LockSupport.parkNanos(projectorCfg.getPollParkNanos());
                }
            } finally {
                CloseHelper.quietClose(replay);
            }
        }
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
     *         Archive's listing for the events channel/stream.
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
     *         if no successor exists yet (current recording is the live tail).
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
     * finalized {@code stopPosition}; for an active recording it is the Archive's live write-head
     * position (which advances as the cluster emits events). Returns {@code startPosition} when
     * neither is available (active recording with no events yet).
     */
    private long recordingUpperBound(AeronArchive archive, RecordingDescriptor descriptor) {
        return OmsClusterEventsRecordingSupport.recordingUpperBound(
                archive, descriptor.recordingId(), descriptor.startPosition(), descriptor.stopPosition());
    }

    /**
     * Snapshot of the recording's identity and bounds returned by
     * {@link AeronArchive#listRecordingsForUri}.
     */
    record RecordingDescriptor(long recordingId, long startPosition, long stopPosition) {}

    /**
     * Outcome of the per-poll walk-forward decision: should the replay loop break out of the
     * current recording and reopen against {@code successor}? Used by
     * {@link #runReplayLoopWithRecordingWalk} and tested directly by
     * {@code OmsPostgresProjectorWalkForwardTest}.
     */
    record WalkForwardDecision(boolean walk, String reason) {
        static WalkForwardDecision park() {
            return new WalkForwardDecision(false, "park");
        }
    }

    /**
     * Pure decision function — package-private for unit test. Walk forward when either
     * <ul>
     *   <li>the current recording is closed ({@code recordingStop != NULL_POSITION}) and we've
     *       fully drained it ({@code currentApplied >= recordingStop}), AND a successor
     *       exists, OR</li>
     *   <li>the current recording is stuck-open ({@code recordingStop == NULL_POSITION},
     *       typically a recording from a crashed cluster session that was never properly
     *       closed) AND a higher-id recording exists on the same channel+stream — the
     *       cluster only writes to one recording at a time, so a successor's existence
     *       proves the current recording is no longer being written to.</li>
     * </ul>
     * Without the stale-open branch the projector parked forever on the dead recording 16
     * on pop after the 2026-05-23 cluster crash + restart cycle, leaving the
     * {@code cluster=9 projector=0} open-orders drift unresolved.
     */
    static WalkForwardDecision decideWalkForward(
            long recordingStop, long currentApplied, RecordingDescriptor successor) {
        if (successor == null) {
            return WalkForwardDecision.park();
        }
        long nullPos = io.aeron.archive.client.AeronArchive.NULL_POSITION;
        boolean closedAndDrained = recordingStop != nullPos && currentApplied >= recordingStop;
        boolean staleOpen = recordingStop == nullPos;
        if (closedAndDrained) {
            return new WalkForwardDecision(true, "complete");
        }
        if (staleOpen) {
            return new WalkForwardDecision(true, "stale-open (successor exists)");
        }
        return WalkForwardDecision.park();
    }

    /**
     * Loud-fail when the saved recording id is no longer listed in the Aeron Archive. The
     * pre-hardening behaviour was to silently reset to position 0 of the active recording on
     * this branch, which would lose every event up to the saved position (the projector bug
     * fixed by V55 on 2026-05-23). Refuse instead; operator decides whether to bootstrap
     * fresh via {@code resetWithRecording(newOldestId, 0)} (accept loss) or restore the
     * recording files. Package-private so the unit test in {@code OmsPostgresProjectorReplayCursorGuardTest}
     * can exercise both branches without standing up a real {@link AeronArchive}.
     */
    static void requireRecordingPresent(long savedRecordingId, RecordingDescriptor descriptor) {
        if (descriptor != null) {
            return;
        }
        throw new IllegalStateException(
                "oms-postgres-projector cannot continue: saved recordingId="
                        + savedRecordingId
                        + " is not listed in the Aeron Archive for events stream "
                        + OmsClusterWireFormat.EVENTS_STREAM_ID
                        + " (channel=" + OmsClusterWireFormat.EVENTS_CHANNEL + ")."
                        + " The recording files may have been deleted or the projector is wired to the wrong"
                        + " Aeron Archive. Operator must decide between (a) repointing to a different Archive,"
                        + " or (b) accepting data loss by running resetWithRecording to a known-good"
                        + " (recordingId, position). Refusing to silently reset to position 0 of an unrelated"
                        + " recording — see handover §9.");
    }

    /**
     * Loud-fail when the saved cursor position is past the live upper bound (stopPosition for
     * a closed recording, write-head for an active one). The pre-hardening behaviour silently
     * reset to {@code descriptor.startPosition()} and persisted that — exactly the bug that
     * destroyed pop's cursor on 2026-05-23. Refuse instead; operator must verify and
     * {@code resetWithRecording} to a known-good {@code (recordingId, position)}.
     * Package-private for unit tests.
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
                "oms-postgres-projector saved cursor (recordingId="
                        + savedRecordingId
                        + ", position=" + savedPosition + ") is past the end of the recording (upperBound="
                        + upperBound + ", startPosition=" + descriptor.startPosition()
                        + ", stopPosition=" + descriptor.stopPosition()
                        + "). This indicates the recording was truncated or replaced under the projector."
                        + " Refusing to silently reset to position 0 — operator must inspect via"
                        + " AeronArchive control + resetWithRecording to a verified (recordingId, position).");
    }

    /**
     * Decodes one fragment and applies it to Postgres inside a single transaction:
     *
     * <ul>
     *   <li>{@link OmsClusterWireFormat#TYPE_ID_ORDER_ADMITTED} → {@code orders} insert (idempotent
     *       ON CONFLICT) + {@link OrderControlAdmission#persistAdmission} (CAS to WORKING /
     *       REJECTED, {@code control_decisions} row, {@code domain_event_outbox} envelope) +
     *       cursor advance (slice 2d).</li>
     *   <li>{@link OmsClusterWireFormat#TYPE_ID_EXECUTION_APPLIED} → {@code executions} insert
     *       (idempotent on {@code (account_id, venue_exec_ref)}) + {@code orders} CAS to
     *       PARTIALLY_FILLED / FILLED / CANCELLED / REJECTED + {@code domain_event_outbox}
     *       envelope (OrderPartiallyFilled / OrderFilled / OrderCancelled / OrderRejected) +
     *       cursor advance (slice 3e).</li>
     * </ul>
     *
     * <p>{@link io.aeron.logbuffer.Header#position()} is the cluster log position <em>after</em>
     * this fragment.
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
            long newPosition = header.position();
            switch (typeId) {
                case OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED -> {
                    OrderAdmittedEvent ev = OrderAdmittedEvent.decode(buffer, offset, length);
                    transactionTemplate.executeWithoutResult(status -> applyAdmittedEvent(ev, newPosition));
                    lastAppliedPosition.set(newPosition);
                }
                case OmsClusterWireFormat.TYPE_ID_EXECUTION_APPLIED -> {
                    ExecutionAppliedEvent ev = ExecutionAppliedEvent.decode(buffer, offset, length);
                    transactionTemplate.executeWithoutResult(status -> applyExecutionAppliedEvent(ev, newPosition));
                    lastAppliedPosition.set(newPosition);
                }
                case OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_APPLIED -> {
                    OrderCancelAppliedEvent ev = OrderCancelAppliedEvent.decode(buffer, offset, length);
                    transactionTemplate.executeWithoutResult(status -> applyOrderCancelAppliedEvent(ev, newPosition));
                    lastAppliedPosition.set(newPosition);
                }
                default -> {
                    // Unknown event types must still advance the cursor so the projector does not stall
                    // on a recording from a future schema. The cluster's snapshot bump procedure already
                    // gates schema-incompatible deploys at the cluster service; the projector is a
                    // downstream consumer that should drain unknown frames silently.
                    long recordingIdNow = requireCurrentRecordingId();
                    transactionTemplate.executeWithoutResult(status ->
                            cursorRepository.advanceWithRecording(
                                    PROJECTOR_ID,
                                    OmsClusterWireFormat.EVENTS_STREAM_ID,
                                    recordingIdNow,
                                    newPosition));
                    lastAppliedPosition.set(newPosition);
                }
            }
        }
    }

    /**
     * Single-transaction body for one {@link OrderAdmittedEvent}. Public + package-private only so
     * tests can drive admission directly from a synthesised event when standing up an Aeron-less
     * Spring context is wasteful (the live IT in {@code OmsPostgresProjectorIT} still drives the
     * full path through the cluster).
     *
     * <p>Phase 4 Tier 2.5 phase D-3: emits the {@code OrderAccepted} domain envelope here on
     * fresh admission (gated on {@link OrdersRepository#insertFromAdmittedEvent}'s boolean
     * return) instead of having the ingress JVM emit it inside its own outbox tx. The
     * {@code orders} row was the existing idempotency key for this projector pass; using its
     * "fresh insert vs replay no-op" signal gives the envelope insert the same idempotency
     * without a separate unique-index migration on {@code domain_event_outbox} (whose
     * {@code order_id} is intentionally not unique — multiple events per order are normal).
     */
    /**
     * Phase 4 Tier 2.5 phase E-2 / phase E-3b — defensive shard guard. At shardCount=1 the cluster
     * log only carries shardId=0 events and {@link OmsConfig.Shard#getId()} defaults to 0, so the
     * guard is a no-op. At shardCount &gt; 1 each projector subscribes to its own cluster's events
     * recording, so it should naturally only see its shard's events; if a config bug wires the
     * projector to the wrong cluster (e.g. {@code OMS_SHARD_ID=1} but the Aeron events recording
     * is shard 0's), this guard catches the mismatch BEFORE applying the event to Postgres.
     *
     * <p>Drop + counter so the operator's existing alert on the failed-projection counter fires.
     * Cursor advances on the dropped event so the projector does not loop on the same misroute;
     * replaying after fixing the wiring will re-emit the event from the recording.
     *
     * <p>E-3b extended this guard to {@link OrderCancelAppliedEvent}: the cluster service now
     * propagates {@link OmsAdmissionClusteredService.AdmittedOrder#shardId()} into the cancel
     * emit (previously hardcoded to 0) so the same misroute check works on the cancel branch
     * without a special case for "shardId always 0 here".
     */
    private boolean isShardMisroute(java.util.UUID orderId, int eventShardId, long newPosition) {
        int expectedShardId = config.getShard().getId();
        if (eventShardId == expectedShardId) {
            return false;
        }
        shardMismatchCounter.increment();
        log.warn(
                "projector shard mismatch: event orderId={} shardId={} but this projector serves shardId={}; dropping event without applying",
                orderId, eventShardId, expectedShardId);
        cursorRepository.advanceWithRecording(
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                newPosition);
        return true;
    }

    /**
     * 2026-05-23 hardening. Reads {@link #currentRecordingId} for use by any cursor write in the
     * apply path. The replay loop guarantees this is set to a non-negative Aeron Archive recording
     * id before any fragment can be polled, so {@code -1} reaching here is a programming bug —
     * not an operational condition — and we fail loud rather than write {@code -1} to Postgres.
     */
    private long requireCurrentRecordingId() {
        long id = currentRecordingId.get();
        if (id < 0L) {
            throw new IllegalStateException(
                    "oms-postgres-projector apply path invoked before currentRecordingId was set"
                            + " (got " + id + "). This indicates the replay loop opened a Subscription"
                            + " without seeding the recording id — programming bug, not an operational condition.");
        }
        return id;
    }

    void applyAdmittedEvent(OrderAdmittedEvent ev, long newPosition) {
        if (isShardMisroute(ev.orderId(), ev.shardId(), newPosition)) {
            return;
        }
        boolean freshAdmission = ordersRepository.insertFromAdmittedEvent(ev);
        if (freshAdmission) {
            // First time we project this admit: emit OrderAccepted into the fanout outbox so
            // downstream consumers see the same {received → working → ...} sequence they used
            // to see when ingress wrote it. On replay (recording recreation, cluster cursor
            // rewind) freshAdmission == false and the envelope is not re-written; the
            // domain_event_outbox row from the original projection stands.
            try {
                domainEventOutboxRepository.insert(ev.orderId(), envelopeCodec.orderAcceptedFromAdmitted(ev));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(
                        "OrderAccepted envelope serialisation failed for orderId=" + ev.orderId(), e);
            }
        }
        PendingControlEvent pending = toPendingControlEvent(ev);
        controlAdmission.persistAdmission(pending);
        // Phase 4 Tier 2.5 phase D-9: project the BUY-async ledger_inflight_outbox row from
        // the cluster's authoritative OrderAdmittedEvent. D-1 introduced this as a crash-window
        // backfill (ingress wrote the row in the happy path, projector filled the gap if the
        // ingress JVM crashed between cluster admit and its outbox-INSERT tx commit). D-9
        // promotes the projector to the only writer, removing the last residual Postgres
        // INSERT from the ingress hot path (Pop! D-8 jstack at c1600/20.8 k rps showed
        // 186/200 ingress exec threads parked in this exact INSERT waiting for a Hikari
        // connection — see OrderIngressService class-level Javadoc). insertIfAbsent stays
        // idempotent on replay because the V4 uq_ledger_inflight_outbox_order_id unique
        // index + ON CONFLICT DO NOTHING make this a safe no-op when the row already exists.
        recordLedgerInflightOutboxIfNeeded(ev);
        cursorRepository.advanceWithRecording(
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                newPosition);
        // Phase 4j per-event histogram: cluster-admit -> projector-applied. Recorded after the last
        // SQL operation in this fragment's transaction; the actual COMMIT happens when the wrapping
        // TransactionTemplate lambda returns (sub-ms after this point in the fast-path). If any of
        // the SQL above throws, the exception exits the lambda before this line and the Timer stays
        // silent for the failed event.
        long latencyMs = wallClock.millis() - ev.acceptedAtMillis();
        OmsPipelineMetrics.recordClusterAdmitToProjector(
                meterRegistry, PROJECTOR_ID, ev.side(), ev.timeInForceCode(), latencyMs);
    }

    /**
     * Phase 4 Tier 2.5 phase D-9 — primary writer of {@code ledger_inflight_outbox} for the
     * BUY-with-async-hold case, driven from {@link OrderAdmittedEvent} on the projector.
     *
     * <p>Originally introduced as D-1's crash-window backfill (when ingress was the primary
     * writer and the projector covered the gap if ingress crashed between cluster admit and
     * its own outbox-INSERT tx commit). D-9 promoted the projector to the <strong>only</strong>
     * writer, removing the last residual Postgres INSERT from the ingress hot path (Pop! D-8
     * jstack showed 186/200 ingress exec threads parked in this exact INSERT waiting for a
     * Hikari connection at c1600 / 20.8 k rps).
     *
     * <p>Gating mirrors {@link com.balh.oms.ingress.OrderIngressService#maybePlaceBuyLedgerInflightHold}
     * for the async path: requires {@code oms.ledger.inflight-reservation-enabled=true},
     * {@code oms.ledger.inflight-async-enabled=true} (the slice 4p outbox path), {@code side=BUY},
     * a non-null {@code ledgerBalanceId}, and a non-zero limit price (the hold notional is
     * {@code quantity * limitPrice}, so market orders are skipped here as well).
     *
     * <p>Payload shape: small JSON object with {@code ledgerBalanceId} / {@code quantity} /
     * {@code limitPrice}, matching the slice 4p contract that {@code LedgerInflightOutboxReconciler}
     * already consumes. Decode of {@code quantityScaled} / {@code limitPriceScaledOrZero} uses
     * the same {@code BigDecimal.valueOf(scaled).divide(SCALE, 10, UNNECESSARY)} pattern as
     * {@link OrdersRepository#insertFromAdmittedEvent(OrderAdmittedEvent)}, so the round-trip
     * preserves exact value (no scientific-notation / trailing-zero drift).
     *
     * <p>Idempotent on replay (cursor rewind, recording rebuild) via
     * {@code uq_ledger_inflight_outbox_order_id} + {@code ON CONFLICT DO NOTHING}: a row that
     * was already projected stays as-is; reconciler/compensator state ({@code attempts},
     * {@code published_at}, {@code compensated_at}) is preserved untouched.
     *
     * <p>Sync HTTP and {@code LedgerInflightCoalescer} (slice 4q) paths are <strong>not</strong>
     * driven from here: those paths still call Ledger / the coalescer synchronously inside
     * {@link com.balh.oms.ingress.OrderIngressService#maybePlaceBuyLedgerInflightHold} so the
     * customer sees the hold result on the response. D-9 only changes the BUY-async branch
     * (the production default).
     */
    private void recordLedgerInflightOutboxIfNeeded(OrderAdmittedEvent ev) {
        if (!config.getLedger().isInflightReservationEnabled()) {
            return;
        }
        if (!config.getLedger().isInflightAsyncEnabled()) {
            return;
        }
        if (ev.side() != AcceptOrderCommand.SIDE_BUY) {
            return;
        }
        if (ev.ledgerBalanceIdOrNull() == null || ev.ledgerBalanceIdOrNull().isBlank()) {
            return;
        }
        if (ev.limitPriceScaledOrZero() == 0L) {
            return;
        }

        BigDecimal quantity = BigDecimal.valueOf(ev.quantityScaled())
                .divide(BigDecimal.valueOf(AcceptOrderCommand.QUANTITY_SCALE), 10, RoundingMode.UNNECESSARY);
        BigDecimal limitPrice = BigDecimal.valueOf(ev.limitPriceScaledOrZero())
                .divide(BigDecimal.valueOf(AcceptOrderCommand.PRICE_SCALE), 10, RoundingMode.UNNECESSARY);
        Order holdSizing =
                new Order(
                        ev.orderId(),
                        UUID.fromString(ev.accountId()),
                        ev.clientIdempotencyKey(),
                        ev.shardId(),
                        ev.version(),
                        OrderStatus.NEW,
                        null,
                        Side.BUY,
                        ev.instrumentSymbol(),
                        quantity,
                        limitPrice,
                        AcceptOrderCommand.timeInForceName(ev.timeInForceCode()),
                        Instant.ofEpochSecond(0, ev.clientTimestampNanos()),
                        Instant.ofEpochMilli(ev.acceptedAtMillis()),
                        null,
                        ev.accountIdHash(),
                        ev.ledgerBalanceIdOrNull(),
                        BigDecimal.ZERO,
                        AcceptOrderCommand.ordTypeName(ev.ordTypeCode()));
        Optional<BigDecimal> holdAmount = BuyFundsRequirement.requiredBuyFunds(holdSizing, config);
        if (holdAmount.isEmpty()) {
            return;
        }
        Optional<BigDecimal> feeAmount = BuyFundsRequirement.estimatedFee(holdSizing, config);

        var node = objectMapper.createObjectNode();
        node.put("ledgerBalanceId", ev.ledgerBalanceIdOrNull());
        node.put("quantity", quantity.toPlainString());
        node.put("limitPrice", limitPrice.toPlainString());
        node.put("holdAmount", holdAmount.get().toPlainString());
        feeAmount.ifPresent(f -> node.put("feeAmount", f.toPlainString()));
        String payload;
        try {
            payload = objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            // Fail the projector tx loudly: a missing inflight outbox row is a financial
            // correctness issue, and we cannot serialize a small fixed-shape JSON object means
            // something is very wrong. Better to halt the projector than silently advance.
            throw new IllegalStateException(
                    "ledger inflight outbox payload serialisation failed for orderId=" + ev.orderId(), e);
        }
        ledgerInflightOutboxRepository.insertIfAbsent(ev.orderId(), payload);
    }

    /**
     * Single-transaction body for one {@link ExecutionAppliedEvent} (slice 3e). The cluster has
     * already walked the order state machine deterministically (slice 3c) and emitted this event
     * with the post-apply state ({@code newCumQtyScaled}, {@code newVersion}, {@code newStatusCode}
     * plus the execution input the projector needs to write the {@code executions} row without a
     * second Postgres lookup of the FIX message). This method translates that authoritative result
     * into the three Postgres rows that the legacy {@link com.balh.oms.returnpath.ExecutionReportApplier}
     * used to write directly:
     *
     * <ol>
     *   <li>{@code executions} row via {@link ExecutionsRepository#tryInsertTrade} /
     *       {@code tryInsertCancel} / {@code tryInsertVenueReject} — idempotent on
     *       {@code (account_id, venue_exec_ref)}; an empty result means we have already applied
     *       this event in a prior incarnation (cursor crash window) and the rest of the transaction
     *       is skipped — {@code orders} and {@code domain_event_outbox} were committed together
     *       in that prior transaction.</li>
     *   <li>{@code orders} CAS via {@link OrdersRepository#updateFillOrCancelWithCas} (TRADE /
     *       CANCEL) or {@link OrdersRepository#updateWithCas} (VENUE_REJECT). The cluster owns
     *       {@code newVersion} so the CAS expectedVersion is {@code newVersion - 1}.</li>
     *   <li>{@code domain_event_outbox} row carrying {@code OrderPartiallyFilled} /
     *       {@code OrderFilled} / {@code OrderCancelled} / {@code OrderRejected} envelopes; same
     *       envelope shapes the legacy applier produced so downstream fanout is unchanged.</li>
     * </ol>
     *
     * <p><strong>Slice 3e-2 scope.</strong> The handler also writes the side effects the legacy
     * {@link com.balh.oms.returnpath.ExecutionReportApplier} produced inside the same transaction:
     *
     * <ul>
     *   <li>{@link MarketContextRepository#mergeVenueFillEvidence} on TRADE — best-execution
     *       evidence patch built from the venue ER fields plus an optional NBBO reference (stub
     *       config or live HTTP via {@link MarketdataPlatformHttpClient} when
     *       {@code oms.marketdata.enabled=true} and the NBBO flags are on).</li>
     *   <li>{@link MarketContextRepository#ensureStubSnapshot} on VENUE_REJECT — guarantees a
     *       {@code market_context} row exists for orders that reject before any trade.</li>
     *   <li>{@link PositionsRepository#recordTradeFill} on TRADE — BUY fills credit the buy /
     *       custody account; SELL fills decrement and produce a
     *       {@link SellFillPositionSplit} which is persisted on the {@code executions} row for
     *       the operator mark-failed unwind path.</li>
     *   <li>Free-riding attribution merge on TRADE BUY (when
     *       {@code oms.settlement.free-riding-attribution-enabled=true}) — finds prior unsettled
     *       BUY {@code executions} for the same account+symbol and appends them to
     *       {@code executions.unsettled_funded_by_exec_ids} on the just-inserted row.</li>
     * </ul>
     *
     * <p>Outbound-queue expiry ({@code applyOutboundJobExpired}) was not migrated to the cluster
     * path: {@code oms-fix-egress} sends NOS immediately on each {@code OrderAdmittedEvent} replay
     * and retries indefinitely on {@code SessionNotFound}, so there is no aged-out outbound job to
     * reject. The legacy {@code ExecutionReportApplier} was deleted in slice 3g-2 (the simulated
     * routing backend that was its last consumer was deleted in the same slice).
     *
     * <p><strong>Determinism on derived state.</strong> {@code market_context} /
     * {@code positions} / {@code position_history} / free-riding links are downstream-only; they
     * are not part of the cluster's order state machine. The cluster's
     * {@code (orderId, venueExecRef)} dedupe + the projector's
     * {@code executions(account_id, venue_exec_ref)} unique constraint together guarantee these
     * side effects fire exactly once per applied ER. Replay re-emits the same
     * {@link ExecutionAppliedEvent} bytes (snapshot v3 covers both indexes); the projector skips
     * the side effects when {@code tryInsertTrade} returns empty (already-applied row), so there
     * is no double-counting on positions / history / market_context merges.
     *
     * <p><strong>Determinism contract.</strong> {@code newStatusCode} on the wire is an
     * {@link OrderStatus} ordinal; the cluster keeps the byte constants in sync with the enum
     * (see {@link OmsAdmissionClusteredService} status code section). {@code rejectCodeOrZero}
     * on a venue reject is a {@link RejectCode} ordinal supplied by the slice 3d
     * {@link com.balh.oms.fix.FixInboundClusterSink} translator.
     *
     * <p><strong>Version contract.</strong> The cluster's in-memory {@code AdmittedOrder.version}
     * counter and Postgres's {@code orders.version} column are <em>not</em> in lockstep — Postgres
     * bumps an extra time at admission ({@code NEW → WORKING} CAS inside
     * {@link OrderControlAdmission#persistAdmission}) that the cluster does not mirror, so a fresh
     * admission lands at cluster {@code version=0} but Postgres {@code version=1}. The projector's
     * {@code orders} CAS therefore uses Postgres's current {@code order.version()} as the expected,
     * not {@code ev.newVersion() - 1}. The domain envelope's {@code newSeq} is the post-CAS Postgres
     * version ({@code order.version() + 1}); the cluster's {@code ev.newVersion()} is informational
     * (logged on mismatch but not enforced).
     */
    void applyExecutionAppliedEvent(ExecutionAppliedEvent ev, long newPosition) {
        Optional<Order> opt = ordersRepository.findById(ev.orderId());
        if (opt.isEmpty()) {
            // The projector reads the same recording as the slice 2d/2b-2 OrderAdmitted path; the
            // cluster orders ApplyExecutionReportCommand strictly after the AcceptOrderCommand for
            // the same orderId, so the orders row must exist by the time we see the
            // ExecutionAppliedEvent for it. If it does not, log loudly and advance the cursor so the
            // projector does not stall — the only way to land here is a recording recreation that
            // truncated the OrderAdmitted event but kept the ExecutionApplied event, which is an
            // operational anomaly that needs human inspection rather than a silent retry.
            log.warn(
                    "Projector: ExecutionAppliedEvent for unknown order {} (venueExecRef={}); skipping.",
                    ev.orderId(),
                    ev.venueExecRef());
            cursorRepository.advanceWithRecording(
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                newPosition);
            return;
        }
        Order order = opt.get();
        boolean applied = switch (ev.execTypeCode()) {
            case ApplyExecutionReportCommand.EXEC_TYPE_TRADE -> applyTradeProjection(ev, order);
            case ApplyExecutionReportCommand.EXEC_TYPE_CANCEL -> applyCancelProjection(ev, order);
            case ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT -> applyVenueRejectProjection(ev, order);
            case ApplyExecutionReportCommand.EXEC_TYPE_REPLACE -> applyReplaceProjection(ev, order);
            case ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT -> applyCancelRejectProjection(ev, order);
            case ApplyExecutionReportCommand.EXEC_TYPE_REPLACE_REJECT -> applyReplaceRejectProjection(ev, order);
            default -> {
                log.warn(
                        "Projector: ExecutionAppliedEvent with unknown execTypeCode {} for order {}; skipping.",
                        ev.execTypeCode(),
                        ev.orderId());
                yield false;
            }
        };
        if (!applied) {
            // Duplicate replay path: executions row was already committed in a prior transaction;
            // orders and domain_event_outbox were committed together with it. Just advance the
            // cursor so we make progress.
            log.debug(
                    "Projector: ExecutionAppliedEvent already projected for order {} venueExecRef={};"
                            + " advancing cursor only.",
                    ev.orderId(),
                    ev.venueExecRef());
        }
        cursorRepository.advanceWithRecording(
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                newPosition);
    }

    /**
     * Slice 4p — single-transaction body for one {@link OrderCancelAppliedEvent} (OMS-initiated
     * cancel from {@code LedgerInflightHoldFailureCompensator}). Distinct from
     * {@link #applyExecutionAppliedEvent}'s {@code EXEC_TYPE_CANCEL} branch in two ways:
     *
     * <ol>
     *   <li>No {@code executions} row insert. The cancel never touched a venue, so there is no
     *       venue execution to record. {@code positions} / {@code market_context} are also
     *       untouched: this cancel can only fire on a working / partially-filled order whose
     *       cumulative qty is unchanged by the cancel itself; any prior trade ER already wrote
     *       the trade-side rows.</li>
     *   <li>Different domain envelope shape. {@link DomainEventEnvelopeCodec#orderCancelled} is
     *       called with empty {@code venueId} / {@code venueExecRef} (the existing envelope tolerates
     *       these — see {@code orderCancelled} signature: both are plain {@code String} fields).
     *       Downstream BFF consumers can branch on empty venue fields if they want to surface
     *       OMS-initiated cancels distinctly.</li>
     * </ol>
     *
     * <p>Idempotency: re-delivery on cluster log replay sees {@code orders.status=CANCELLED}
     * already; the CAS using the projector's read-then-CAS pattern misses (expected version no
     * longer matches), the method logs at debug, and the cursor advances. The original
     * {@code domain_event_outbox} row from the first apply stands.
     *
     * <p>Unknown order on this branch is the same anomaly described on
     * {@link #applyExecutionAppliedEvent}: the cluster orders {@link OrderAdmittedEvent} strictly
     * before any {@link OrderCancelAppliedEvent} for the same order, so seeing the cancel without
     * the orders row implies a recording recreation that truncated admit but kept cancel — log
     * loudly and advance the cursor.
     */
    void applyOrderCancelAppliedEvent(OrderCancelAppliedEvent ev, long newPosition) {
        if (isShardMisroute(ev.orderId(), ev.shardId(), newPosition)) {
            return;
        }
        Optional<Order> opt = ordersRepository.findById(ev.orderId());
        if (opt.isEmpty()) {
            log.warn(
                    "Projector: OrderCancelAppliedEvent for unknown order {} (reason={}); skipping.",
                    ev.orderId(),
                    ev.reason());
            cursorRepository.advanceWithRecording(
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                newPosition);
            return;
        }
        Order order = opt.get();
        if (order.status() == OrderStatus.CANCELLED) {
            // Duplicate replay path. The first apply committed orders.status=CANCELLED and the
            // domain_event_outbox row together; re-emitting the envelope here would write a second
            // outbox row for the same logical cancel, which downstream fanout would deliver twice.
            log.debug(
                    "Projector: OrderCancelAppliedEvent already projected for order {}; advancing cursor only.",
                    ev.orderId());
            cursorRepository.advanceWithRecording(
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                newPosition);
            return;
        }
        int pgExpectedVersion = order.version();
        boolean cas = ordersRepository.updateFillOrCancelWithCas(
                order.id(),
                pgExpectedVersion,
                order.cumFilledQuantity(),
                OrderStatus.CANCELLED,
                /* terminalReason = */ null,
                Instant.ofEpochMilli(ev.cancelledAtMillis()));
        if (!cas) {
            // CAS missed — somebody beat us to a terminal status. The most common race is a venue
            // fill landing between the inflight-hold failure and this compensator cancel; the
            // projector saw EXEC_TYPE_TRADE/FILLED first, this cancel is now stale. We log and
            // advance the cursor — the order's terminal state stands and we do NOT emit an
            // OrderCancelled envelope for an order that filled.
            log.warn(
                    "Projector: orders CAS missed on OrderCancelApplied projection for order {} (pgExpectedVersion={}, current status={}); cursor advances without envelope.",
                    order.id(),
                    pgExpectedVersion,
                    order.status());
            cursorRepository.advanceWithRecording(
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                newPosition);
            return;
        }
        int newSeq = pgExpectedVersion + 1;
        Order refreshed = ordersRepository.findById(order.id()).orElse(order);
        try {
            // OMS-initiated cancel: empty venueId / venueExecRef. The domain event envelope shape
            // already accepts plain Strings (see DomainEventEnvelopeCodec#orderCancelled); empty
            // strings are a downstream sentinel for "no venue interaction recorded".
            domainEventOutboxRepository.insert(
                    refreshed.id(),
                    envelopeCodec.orderCancelled(refreshed, newSeq, /* venueId = */ "", /* venueExecRef = */ ""));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "domain event serialisation failed for OMS-cancel of order " + refreshed.id(), e);
        }
        cursorRepository.advanceWithRecording(
                PROJECTOR_ID,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                requireCurrentRecordingId(),
                newPosition);
    }

    /**
     * @return {@code true} if a fresh {@code executions} row was inserted (and therefore the rest
     *         of the projection — market_context + positions + free-riding + orders CAS + domain
     *         envelope — was applied); {@code false} on duplicate replay (caller advances cursor
     *         only). Wrapped in {@link #TIMER_TRADE_APPLY} so the legacy applier's dashboard panel
     *         keeps measuring the same span: best-ex evidence merge + insert + CAS + envelope.
     */
    private boolean applyTradeProjection(ExecutionAppliedEvent ev, Order order) {
        return Boolean.TRUE.equals(
                meterRegistry.timer(TIMER_TRADE_APPLY).record(() -> applyTradeProjectionTimed(ev, order)));
    }

    private Boolean applyTradeProjectionTimed(ExecutionAppliedEvent ev, Order order) {
        BigDecimal lastQty = scaledToBigDecimal(ev.lastQtyScaled(), QUANTITY_SCALE);
        BigDecimal lastPx = ev.lastPxScaled() == 0L
                ? null
                : scaledToBigDecimal(ev.lastPxScaled(), PRICE_SCALE);
        BigDecimal newCum = scaledToBigDecimal(ev.newCumQtyScaled(), QUANTITY_SCALE);
        BigDecimal leaves = order.quantity().subtract(newCum);
        String raw = ev.rawEnvelopeJson();

        // Slice 3e-2: best-execution evidence merge runs BEFORE the executions insert (mirrors
        // legacy applier order). On duplicate replay the merge is still safe — it is an idempotent
        // upsert and Postgres `||` is associative on JSON, so re-applying the same patch yields
        // the same snapshot_json. We accept this minor cost on the duplicate path because the
        // legacy applier did the same and downstream best-ex consumers expect at-least-once on
        // the merge.
        mergeMarketContextEvidenceForTrade(order, ev, lastQty, lastPx, leaves, newCum);

        Instant venueTs = Instant.ofEpochSecond(0L, ev.venueTsNanos());
        java.time.LocalDate tradeDate = settlementDateCalculator.computeTradeDate(venueTs);
        // Slice 2b-1: use the symbol-aware resolver so an effective `instrument_settlement_profile`
        // row drives the cycle. Empty profile (the skeleton's default state until ops loads
        // data) falls back to the configured default cycle — same outcome as Slice 1, just
        // routed through the lookup so future profile loads take effect without a code change.
        java.time.LocalDate expectedSettlementDate = settlementDateCalculator.resolveExpectedSettlementDate(
                tradeDate, order.instrumentSymbol());
        Optional<Long> insertedId = executionsRepository.tryInsertTrade(
                order.id(),
                order.accountId(),
                ev.venueId(),
                venueTs,
                ev.venueExecRef(),
                lastQty,
                lastPx,
                leaves,
                newCum,
                raw,
                tradeDate,
                expectedSettlementDate);
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return false;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();

        // Slice 3e-2: free-riding attribution links must be appended to the just-inserted row
        // before the orders CAS so the link write commits in the same transaction. Excludes the
        // just-inserted execution from the lookup (legacy semantics — a row cannot fund itself).
        if (config.getSettlement().isFreeRidingAttributionEnabled() && order.side() == Side.BUY) {
            List<Long> funding = executionsRepository.findUnsettledBuyTradeExecutionIdsForAttribution(
                    order.accountId(),
                    order.instrumentSymbol(),
                    insertedId.get(),
                    config.getSettlement().getFreeRidingAttributionMaxFundingExecutions());
            if (!funding.isEmpty()) {
                executionsRepository.appendUnsettledFundedByExecutionIds(insertedId.get(), funding);
                meterRegistry.counter(METRIC_FREE_RIDING_ATTRIBUTION).increment();
            }
        }

        // Slice 3e-2: positions + position_history. SELL fills also write back the
        // pending-buy-vs-settled split onto the executions row so the operator mark-failed unwind
        // path (PositionsRepository#revertPositionForMarkTradeFailed) can reverse this fill exactly.
        UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
        Optional<SellFillPositionSplit> sellSplit =
                positionsRepository.recordTradeFill(order, insertedId.get(), lastQty, custody);
        if (order.side() == Side.SELL && sellSplit.isEmpty()) {
            throw new IllegalStateException(
                    "SELL trade fill did not update positions (insufficient quantity) orderId="
                            + order.id()
                            + " executionId="
                            + insertedId.get()
                            + " fillQty="
                            + lastQty);
        }
        sellSplit.ifPresent(split -> executionsRepository.updateSellFillPositionSplit(insertedId.get(), split));

        OrderStatus newStatus = OrderStatus.values()[ev.newStatusCode()];
        Instant terminalAt = newStatus == OrderStatus.FILLED ? appliedAtInstant(ev) : null;
        int pgExpectedVersion = order.version();
        boolean cas = ordersRepository.updateFillOrCancelWithCas(
                order.id(), pgExpectedVersion, newCum, newStatus, /* terminalReason = */ null, terminalAt);
        if (!cas) {
            // The orders row's Postgres version moved past pgExpectedVersion between findById and
            // the CAS — possible if a parallel writer (legacy applier on another JVM during the
            // 3e/3g overlap window) projected a sibling event ahead of us. The executions row we
            // just inserted is valid; the orders row's eventually-consistent state will reflect
            // the latest event seen on either path.
            log.warn(
                    "Projector: orders CAS missed on TRADE projection for order {} (pgExpectedVersion={});"
                            + " orders row advanced beyond the projector's read.",
                    order.id(),
                    pgExpectedVersion);
            return true;
        }
        int newSeq = pgExpectedVersion + 1;
        Order refreshed = ordersRepository.findById(order.id()).orElse(order);
        try {
            String envelopeJson;
            if (newStatus == OrderStatus.PARTIALLY_FILLED) {
                envelopeJson = envelopeCodec.orderPartiallyFilled(
                        refreshed,
                        newSeq,
                        newCum,
                        lastQty,
                        lastPx,
                        ev.venueId(),
                        ev.venueExecRef());
            } else {
                BigDecimal vwap = executionsRepository.weightedAverageTradePrice(order.id());
                envelopeJson = envelopeCodec.orderFilled(
                        refreshed,
                        newSeq,
                        newCum,
                        vwap,
                        ev.venueId(),
                        ev.venueExecRef());
                meterRegistry.counter(METRIC_ORDER_FILLED_EVENTS).increment();
            }
            domainEventOutboxRepository.insert(order.id(), envelopeJson);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("domain event serialisation failed for order " + order.id(), e);
        }
        return true;
    }

    private boolean applyCancelProjection(ExecutionAppliedEvent ev, Order order) {
        Optional<Long> insertedId = executionsRepository.tryInsertCancel(
                order.id(),
                order.accountId(),
                ev.venueId(),
                Instant.ofEpochSecond(0L, ev.venueTsNanos()),
                ev.venueExecRef(),
                order.cumFilledQuantity(),
                ev.rawEnvelopeJson());
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return false;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();
        int pgExpectedVersion = order.version();
        boolean cas = ordersRepository.updateFillOrCancelWithCas(
                order.id(),
                pgExpectedVersion,
                order.cumFilledQuantity(),
                OrderStatus.CANCELLED,
                /* terminalReason = */ null,
                appliedAtInstant(ev));
        if (!cas) {
            log.warn(
                    "Projector: orders CAS missed on CANCEL projection for order {} (pgExpectedVersion={}).",
                    order.id(),
                    pgExpectedVersion);
            return true;
        }
        int newSeq = pgExpectedVersion + 1;
        Order refreshed = ordersRepository.findById(order.id()).orElse(order);
        try {
            domainEventOutboxRepository.insert(
                    order.id(),
                    envelopeCodec.orderCancelled(refreshed, newSeq, ev.venueId(), ev.venueExecRef()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("domain event serialisation failed for order " + order.id(), e);
        }
        return true;
    }

    /**
     * Wed-demo addition. Projects a venue REPLACE ACK (ER 35=5) emitted by the cluster after a
     * 35=G we sent was accepted by the broker. The semantics, mirroring the cluster's apply path
     * in {@link com.balh.oms.cluster.OmsAdmissionClusteredService} (search for
     * {@code EXEC_TYPE_REPLACE}):
     *
     * <ul>
     *   <li>{@code ev.lastQtyScaled()} carries the broker-authoritative <strong>new total
     *       OrderQty</strong> (1e9 fixed-point) — <em>not</em> a fill quantity. There is no fill
     *       on a pure replace.</li>
     *   <li>{@code ev.lastPxScaled()} carries the new limit price (1e6 fixed-point; 0 means
     *       market / unchanged). The cluster only overwrites the row's price when this is
     *       {@code > 0}, so we mirror that gate here.</li>
     *   <li>{@code ev.newCumQtyScaled()} echoes the unchanged pre-replace cumulative quantity
     *       (cluster does not mutate cumQty on replace).</li>
     *   <li>{@code ev.newStatusCode()} echoes the cluster's recomputed status (WORKING /
     *       PARTIALLY_FILLED / FILLED depending on whether the new total qty leaves any quantity
     *       to work or is already covered by prior fills).</li>
     * </ul>
     *
     * <p>Projection side effects, in one transaction:
     * <ol>
     *   <li>{@code executions} REPLACE audit row via {@link ExecutionsRepository#tryInsertReplace};
     *       idempotent on {@code (account_id, venue_exec_ref)} so duplicate replay is safe.</li>
     *   <li>{@code orders} CAS on {@code version} overwrites {@code quantity} +
     *       {@code limit_price} + {@code status}; {@code cum_filled_quantity} stays put.</li>
     *   <li>{@code domain_event_outbox} insert of the {@link DomainEventEnvelopeCodec#orderReplaced}
     *       envelope so the BFF consumer mirrors the new qty/price onto {@code customer_orders}
     *       without a second OMS read.</li>
     * </ol>
     *
     * <p>Duplicate-replay path: the {@code tryInsertReplace} returns empty when the
     * {@code (account_id, venue_exec_ref)} pair already exists. We mirror the trade / cancel
     * projection contract: return {@code false} so the caller logs at debug and advances the
     * cursor without re-emitting the envelope.
     */
    private boolean applyReplaceProjection(ExecutionAppliedEvent ev, Order order) {
        BigDecimal newQty = scaledToBigDecimal(ev.lastQtyScaled(), QUANTITY_SCALE);
        BigDecimal newLimitPrice = ev.lastPxScaled() == 0L
                ? order.limitPrice()
                : scaledToBigDecimal(ev.lastPxScaled(), PRICE_SCALE);
        BigDecimal newCum = scaledToBigDecimal(ev.newCumQtyScaled(), QUANTITY_SCALE);
        BigDecimal leaves = newQty.subtract(newCum);

        Optional<Long> insertedId = executionsRepository.tryInsertReplace(
                order.id(),
                order.accountId(),
                ev.venueId(),
                Instant.ofEpochSecond(0L, ev.venueTsNanos()),
                ev.venueExecRef(),
                newQty,
                newLimitPrice,
                leaves,
                newCum,
                ev.rawEnvelopeJson());
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return false;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();

        OrderStatus newStatus = OrderStatus.values()[ev.newStatusCode()];
        int pgExpectedVersion = order.version();
        boolean cas = ordersRepository.updateReplaceWithCas(
                order.id(), pgExpectedVersion, newQty, newLimitPrice, newStatus);
        if (!cas) {
            log.warn(
                    "Projector: orders CAS missed on REPLACE projection for order {} (pgExpectedVersion={}); orders row advanced beyond the projector's read.",
                    order.id(),
                    pgExpectedVersion);
            return true;
        }
        int newSeq = pgExpectedVersion + 1;
        Order refreshed = ordersRepository.findById(order.id()).orElse(order);
        try {
            domainEventOutboxRepository.insert(
                    order.id(),
                    envelopeCodec.orderReplaced(
                            refreshed, newSeq, newQty, newLimitPrice, ev.venueId(), ev.venueExecRef()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "domain event serialisation failed for REPLACE of order " + order.id(), e);
        }
        return true;
    }

    /**
     * Wed-demo addition. Projects a venue OrderCancelReject (ER 35=9 against a prior 35=F cancel)
     * emitted by the cluster. The order is unchanged on the venue's books and in our cluster —
     * status, cumQty, qty, price all stay put — so the projector only:
     *
     * <ol>
     *   <li>writes a {@code CANCEL_REJECT} audit row in {@code executions}
     *       (idempotent on {@code (account_id, venue_exec_ref)}), and</li>
     *   <li>emits the {@code OrderCancelRejected} envelope to {@code domain_event_outbox} so the
     *       BFF can surface a "cancel rejected by broker" toast without polling.</li>
     * </ol>
     *
     * <p>No {@code orders} CAS. The cluster bumps its in-memory version (so the next ER for the
     * same order doesn't see a stale snapshot), but the Postgres row has nothing to change; a
     * version-only bump in Postgres would only serve to confuse downstream consumers that key off
     * {@code (orderId, version)} for state snapshots.
     *
     * <p>{@code currentSeq} on the envelope is the current {@code orders.version} (not + 1) —
     * matches {@link OrderCancelRejectedEvent} doc.
     */
    private boolean applyCancelRejectProjection(ExecutionAppliedEvent ev, Order order) {
        Optional<Long> insertedId = executionsRepository.tryInsertCancelReject(
                order.id(),
                order.accountId(),
                ev.venueId(),
                Instant.ofEpochSecond(0L, ev.venueTsNanos()),
                ev.venueExecRef(),
                order.cumFilledQuantity(),
                ev.rawEnvelopeJson());
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return false;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();
        try {
            domainEventOutboxRepository.insert(
                    order.id(),
                    envelopeCodec.orderCancelRejected(order, order.version(), ev.venueId(), ev.venueExecRef()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "domain event serialisation failed for CANCEL_REJECT of order " + order.id(), e);
        }
        return true;
    }

    /**
     * Wed-demo addition. Mirror of {@link #applyCancelRejectProjection} for a 35=9 against a prior
     * 35=G replace. Same lifecycle: audit row + envelope, no {@code orders} mutation.
     */
    private boolean applyReplaceRejectProjection(ExecutionAppliedEvent ev, Order order) {
        Optional<Long> insertedId = executionsRepository.tryInsertReplaceReject(
                order.id(),
                order.accountId(),
                ev.venueId(),
                Instant.ofEpochSecond(0L, ev.venueTsNanos()),
                ev.venueExecRef(),
                order.cumFilledQuantity(),
                ev.rawEnvelopeJson());
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return false;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();
        try {
            domainEventOutboxRepository.insert(
                    order.id(),
                    envelopeCodec.orderReplaceRejected(order, order.version(), ev.venueId(), ev.venueExecRef()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "domain event serialisation failed for REPLACE_REJECT of order " + order.id(), e);
        }
        return true;
    }

    private boolean applyVenueRejectProjection(ExecutionAppliedEvent ev, Order order) {
        // Slice 3e-2: legacy applier writes a stub-only market_context row before the executions
        // insert so orders that reject before any trade still appear in best-ex evidence reports
        // ("we routed it, the venue rejected it"). Idempotent upsert (ON CONFLICT DO NOTHING) so
        // a partial-fill-then-reject scenario does not stomp the trade-time merged snapshot.
        marketContextRepository.ensureStubSnapshot(
                order.id(), config.getRouting().getMarketContextStubJson());

        Optional<Long> insertedId = executionsRepository.tryInsertVenueReject(
                order.id(),
                order.accountId(),
                ev.venueId(),
                Instant.ofEpochSecond(0L, ev.venueTsNanos()),
                ev.venueExecRef(),
                order.cumFilledQuantity(),
                ev.rawEnvelopeJson());
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return false;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();
        RejectCode terminalReason = decodeRejectCode(ev.rejectCodeOrZero());
        int pgExpectedVersion = order.version();
        boolean cas = ordersRepository.updateWithCas(
                order.id(),
                pgExpectedVersion,
                OrderStatus.REJECTED,
                terminalReason,
                /* acceptedAt = */ null,
                appliedAtInstant(ev));
        if (!cas) {
            log.warn(
                    "Projector: orders CAS missed on VENUE_REJECT projection for order {} (pgExpectedVersion={}).",
                    order.id(),
                    pgExpectedVersion);
            return true;
        }
        int newSeq = pgExpectedVersion + 1;
        Order refreshed = ordersRepository.findById(order.id()).orElse(order);
        try {
            domainEventOutboxRepository.insert(
                    order.id(),
                    envelopeCodec.orderRejectedAfterVenue(refreshed, terminalReason, newSeq));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("domain event serialisation failed for order " + order.id(), e);
        }
        return true;
    }

    /**
     * Builds the venue-evidence JSON patch via {@link MarketContextVenueEvidence#toJsonPatch} and
     * merges it into {@code market_context.snapshot_json} (slice 3e-2). The optional NBBO reference
     * comes from {@link #resolveNbbo} — config-stub when {@code oms.routing.nbbo-reference-...} is
     * on, live HTTP when {@code oms.marketdata.enabled=true} + {@code nbbo-in-market-context} on.
     *
     * <p>Why we synthesise an {@link ExecutionTradeCommand} from the
     * {@link ExecutionAppliedEvent}: the legacy {@link MarketContextVenueEvidence} helper signature
     * takes the legacy command type. Reusing it here keeps the JSON patch byte-for-byte identical
     * to the legacy applier's output during the slice 3e-2 / 3g overlap, so downstream best-ex
     * consumers see exactly the same evidence shape from either writer.
     */
    private void mergeMarketContextEvidenceForTrade(
            Order order,
            ExecutionAppliedEvent ev,
            BigDecimal lastQty,
            BigDecimal lastPx,
            BigDecimal leaves,
            BigDecimal newCum) {
        Instant venueTs = Instant.ofEpochSecond(0L, ev.venueTsNanos());
        ExecutionTradeCommand syntheticCmd = new ExecutionTradeCommand(
                order.id(),
                ev.venueId(),
                venueTs,
                ev.venueExecRef(),
                lastQty,
                lastPx,
                leaves,
                newCum);
        try {
            Optional<MarketContextVenueEvidence.NbboQuoteRef> nbbo = resolveNbbo(order, venueTs);
            String patch = MarketContextVenueEvidence.toJsonPatch(objectMapper, order, syntheticCmd, nbbo);
            marketContextRepository.mergeVenueFillEvidence(
                    order.id(),
                    venueTs,
                    config.getRouting().getMarketContextStubJson(),
                    patch);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "market_context evidence serialisation failed for order " + order.id(), e);
        }
    }

    /**
     * Resolves an optional NBBO reference for the market_context patch. Mirrors
     * {@link com.balh.oms.returnpath.ExecutionReportApplier#resolveNbbo} so the projector and
     * legacy applier produce identical patches given the same config:
     *
     * <ul>
     *   <li>if {@code oms.routing.nbbo-reference-in-market-context-enabled=false} → empty;</li>
     *   <li>else if {@code oms.marketdata.enabled=true} +
     *       {@code oms.marketdata.nbbo-in-market-context-enabled=true} and the
     *       {@link MarketdataPlatformHttpClient} bean is present → live HTTP fetch (timed in
     *       {@link #TIMER_NBBO_FETCH});</li>
     *   <li>else if both stub prices are positive → config-stub reference;</li>
     *   <li>else → empty (e.g. NBBO flag on but stub prices zero — patch carries venue fields
     *       only, no {@code nbboClassReference}).</li>
     * </ul>
     */
    private Optional<MarketContextVenueEvidence.NbboQuoteRef> resolveNbbo(Order order, Instant venueTs) {
        var routing = config.getRouting();
        if (!routing.isNbboReferenceInMarketContextEnabled()) {
            return Optional.empty();
        }
        var marketdata = config.getMarketdata();
        if (marketdata.isNbboInMarketContextEnabled() && marketdata.isEnabled()) {
            MarketdataPlatformHttpClient client = marketdataHttp.getIfAvailable();
            if (client != null) {
                Optional<MarketdataNbboQuote> live = meterRegistry
                        .timer(TIMER_NBBO_FETCH, "source", "http")
                        .record(() -> client.fetchNbbo(order.instrumentSymbol()));
                if (live != null && live.isPresent()) {
                    MarketdataNbboQuote q = live.get();
                    return Optional.of(new MarketContextVenueEvidence.NbboQuoteRef(
                            q.bid(), q.ask(), q.asOf(), "NBBO_MARKETDATA_HTTP"));
                }
            }
        }
        BigDecimal bid = routing.getNbboStubBidPrice();
        BigDecimal ask = routing.getNbboStubAskPrice();
        if (bid.compareTo(BigDecimal.ZERO) <= 0 || ask.compareTo(BigDecimal.ZERO) <= 0) {
            return Optional.empty();
        }
        return Optional.of(new MarketContextVenueEvidence.NbboQuoteRef(bid, ask, venueTs));
    }

    private static BigDecimal scaledToBigDecimal(long scaled, BigDecimal scale) {
        // 1e9 / 1e6 fixed-point on the wire; trailing zeros stripped so column comparisons against
        // canonical BigDecimal values do not surprise on scale (Postgres NUMERIC tolerates either,
        // but assertions in tests use isEqualByComparingTo for exactness regardless).
        return BigDecimal.valueOf(scaled)
                .divide(scale, /* scale = */ 10, RoundingMode.UNNECESSARY)
                .stripTrailingZeros();
    }

    private static Instant appliedAtInstant(ExecutionAppliedEvent ev) {
        // ev.appliedAtMillis() carries the cluster's epoch-millis timestamp (Phase 4j rename) but we
        // intentionally use Instant.now() here so terminal_at reflects Postgres-write time, not the
        // cluster apply time. The cluster's authoritative version + status are what drive correctness;
        // this column is observability only.
        return Instant.now();
    }

    private static RejectCode decodeRejectCode(byte code) {
        if (code <= 0) {
            return RejectCode.VENUE_REJECT;
        }
        RejectCode[] values = RejectCode.values();
        if (code >= values.length) {
            return RejectCode.VENUE_REJECT;
        }
        return values[code];
    }

    private static PendingControlEvent toPendingControlEvent(OrderAdmittedEvent ev) {
        // orderTimestamp drives StaleJobGuard ("did this event sit in the journal so long that
        // pre-trade risk decisions are unsafe?"). For a downstream projector, the cluster log is
        // the source of truth and replaying older events on restart is normal — a stale guard
        // would erroneously reject legitimate admissions during catch-up. Use Instant.now() so
        // staleness measures projector-apply latency, which is by definition near-zero.
        // (Phase 4j renamed ev.acceptedAtNanos() to ev.acceptedAtMillis() to match the actual
        // Aeron Cluster unit; we still ignore it here for the StaleJobGuard rationale above.)
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
