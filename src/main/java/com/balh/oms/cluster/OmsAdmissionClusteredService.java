package com.balh.oms.cluster;

import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Aeron {@link ClusteredService} hosting the OMS order-admission state machine.
 *
 * <p>Per ADR 0001 ({@code docs/adr/0001-aeron-cluster-substrate.md}), this is
 * the source of truth for "is this order admitted". Postgres becomes a
 * downstream projection driven by the events this service emits.
 *
 * <h3>Determinism rules (enforced by review and by future lint)</h3>
 * <ul>
 *   <li>No {@code Instant.now()}, {@code System.currentTimeMillis()},
 *       {@code System.nanoTime()}, {@code UUID.randomUUID()},
 *       {@code java.util.Random}. Time and ids come from command payloads or
 *       the cluster-supplied {@code timestamp} parameter.</li>
 *   <li>No external I/O (no Postgres, no HTTP, no FIX, no logging at INFO+
 *       inside the hot path). External effects happen at the edge.</li>
 *   <li>No reflection, no Spring annotations on this class. Wired by hand
 *       from {@code AeronClusterContext}.</li>
 * </ul>
 *
 * <h3>State (in-memory)</h3>
 * <ul>
 *   <li>{@code idempotencyIndex}: {@code (accountId, clientIdempotencyKey) ->
 *       AdmittedOrder}. Used to short-circuit duplicate {@link AcceptOrderCommand}s
 *       and emit a {@link OrderAcceptedEvent} with {@code duplicate=true}.</li>
 *   <li>{@code orderIndex}: {@code orderId -> AdmittedOrder}. Used by later
 *       phases that mutate accepted orders (cancel, fill, etc.). Today only
 *       used to keep both indexes consistent.</li>
 * </ul>
 *
 * <h3>Snapshot format (transitional)</h3>
 *
 * <p>This first scaffold uses a hand-rolled binary snapshot fragment. SBE
 * replaces it before any cross-language consumer attaches (see ADR 0001). The
 * snapshot is a single fragment with header
 * {@link #SNAPSHOT_MAGIC} + schema version + order count, followed by
 * {@code count} {@link AdmittedOrder} entries.
 *
 * <p>Snapshot size scales linearly with admitted order count; the buffer below
 * is {@link ExpandableArrayBuffer} so large snapshots grow on demand. Phase 2+
 * fragments the snapshot across multiple Aeron messages once realistic order
 * cardinalities land.
 */
public class OmsAdmissionClusteredService implements ClusteredService {

    private static final Logger log = LoggerFactory.getLogger(OmsAdmissionClusteredService.class);

    /** Magic number identifying an OMS admission snapshot fragment ("OMSA" in ASCII). */
    static final int SNAPSHOT_MAGIC = 0x4F4D5341;

    /**
     * Snapshot schema version. Bumped only on incompatible state changes.
     *
     * <p>v1 (slice 2): orderIndex + idempotencyIndex, no per-order status / cumQty / executionRefs.
     *
     * <p>v2 (slice 3c): {@link AdmittedOrder} gains {@code statusCode} and {@code cumQtyScaled} so
     * the cluster's deterministic state machine owns the order lifecycle; trailing
     * {@code (orderId, venueExecRef)} list captures the
     * {@link ApplyExecutionReportCommand} idempotency set so a snapshot recovery never re-applies a
     * previously-seen execution report.
     *
     * <p>v3 (slice 3d): adds a trailing {@code (senderCompId, msgSeqNum)} dedupe index so a broker
     * resend of the same FIX {@code MsgSeqNum} after a session gap does not re-apply an ER. The
     * earlier {@code (orderId, venueExecRef)} guard catches identical {@code ExecID}s; this guard
     * catches the broker quirk where a resend recycles {@code MsgSeqNum} but reissues a fresh
     * {@code ExecID}. Per ADR 0001 § Discipline (no backwards compat in dev), v1 / v2 snapshots
     * are rejected — production is rebuilt from the cluster log on restart, no older prod snapshot
     * exists today.
     */
    /**
     * Phase 4 Tier 2.5 phase E-3b bumped this from {@code 3} to {@code 4} when {@link AdmittedOrder}
     * gained {@code shardId} so the cluster log carries the order's owning shard for every emitted
     * {@link OrderCancelAppliedEvent} (and any future shard-aware emissions). Because {@link
     * SnapshotLoader#onFragment} fails fast on a version mismatch, pre-E-3b snapshots are not
     * load-compatible with this build &mdash; operators upgrading from E-2 must wipe
     * {@code archive/} and {@code cluster/} (or the cluster-node's {@code aeron-cluster}) on each
     * member <em>before</em> starting the E-3b binary; the cluster will rebuild state from the
     * recorded log.
     */
    static final int SNAPSHOT_SCHEMA_VERSION = 4;

    /** Phase 2.1 readiness counter — see {@code plans/oms-cluster-recovery-and-hardening.md}. */
    public static final int READINESS_COUNTER_TYPE_ID = 2_000_002;

    public static final String READINESS_COUNTER_LABEL = "oms-cluster-ready";

    public static final long READINESS_VALUE_NOT_READY = 0L;

    public static final long READINESS_VALUE_READY = 1L;

    /** Phase 3 count-only reconcile — open (non-terminal) orders in {@link #orderIndex}. */
    public static final int OPEN_ORDERS_COUNT_COUNTER_TYPE_ID = 2_000_010;

    public static final String OPEN_ORDERS_COUNT_COUNTER_LABEL = "oms-cluster-open-orders-count";

    private static final String ENV_READINESS_ALLOW_EMPTY_REPLAY = "OMS_READINESS_ALLOW_EMPTY_REPLAY";

    /** Initial buffer capacity for command processing. Grown on demand. */
    private static final int INITIAL_BUFFER_CAPACITY = 1024;

    /** Initial map capacity (per-shard). Avoids early resizes for warm-up loads. */
    private static final int INITIAL_INDEX_CAPACITY = 4096;

    private final Map<IdempotencyKey, AdmittedOrder> idempotencyIndex =
            new HashMap<>(INITIAL_INDEX_CAPACITY);
    private final Map<UUID, AdmittedOrder> orderIndex = new HashMap<>(INITIAL_INDEX_CAPACITY);

    /**
     * Per-order set of {@code venueExecRef}s the cluster has already applied an
     * {@link ApplyExecutionReportCommand} for. Provides cluster-level
     * {@code (orderId, venueExecRef)} idempotency so a duplicate ER from QuickFIX (e.g. broker
     * resend) does not double-emit {@link ExecutionAppliedEvent}.
     *
     * <p>The set lives <em>only</em> in the state machine — Postgres no longer dedupes ERs in
     * {@code ExecutionReportApplier} as of slice 3e. Snapshot encode/decode round-trips this index
     * along with {@link #orderIndex}; a leader-handover or replay reconstructs the same set
     * byte-for-byte.
     */
    private final Map<UUID, Set<String>> executionRefIndex = new HashMap<>(INITIAL_INDEX_CAPACITY);

    /**
     * Per-{@code senderCompId} set of {@code msgSeqNum}s the cluster has already applied an
     * {@link ApplyExecutionReportCommand} for. This is the FIX wire-level dedupe guard: a broker
     * may resend a FIX {@code ExecutionReport} with the <em>same</em> {@code MsgSeqNum} after a
     * session-level reconnect (e.g. session-level retransmit, garbled link followed by resend
     * request); the {@code (orderId, venueExecRef)} guard catches identical {@code ExecID}s, but
     * if the broker reissues a fresh {@code ExecID} on the resend, this index is the only thing
     * keeping a duplicate ER out.
     *
     * <p>Slice 3c reserved {@code senderCompId} / {@code msgSeqNum} on the wire of
     * {@link ApplyExecutionReportCommand}; slice 3d turns the guard on. Empty
     * {@code senderCompId} (e.g. cluster-internal tests, slice 3c codec tests) opts out — wire
     * dedupe is FIX-specific, and we do not want to penalise non-FIX callers that legitimately
     * always pass {@code msgSeqNum=0}.
     *
     * <p><strong>Memory:</strong> the set grows monotonically per session. A FIX session that
     * runs for years could in theory accumulate millions of entries; in practice broker sessions
     * roll over daily / weekly with a {@code ResetSeqNumFlag=Y} that comes through as a new
     * {@code senderCompId} epoch (ops convention), so a {@code clear} on a future session-reset
     * command will compact the index. Slice 3d does not introduce that command; the index is left
     * unbounded for now and revisited if any FIX session genuinely keeps the same
     * {@code (senderCompId, msgSeqNum)} space across more than ~1M ERs.
     */
    private final Map<String, Set<Integer>> senderSeqIndex = new HashMap<>(INITIAL_INDEX_CAPACITY);

    /**
     * HTTP-layer idempotency keys per order for {@link RequestCancelOrderCommand} +
     * {@link RequestReplaceOrderCommand}. The first character of each stored key is the kind
     * discriminator: {@code 'c'} for cancel, {@code 'r'} for replace. A re-delivered command with
     * a matching {@code (orderId, kind+clientRequestKey)} is a silent no-op so HTTP retries do not
     * issue a second 35=F / 35=G to the broker. Not snapshot-persisted: a snapshot-then-restart
     * loses this dedupe; a duplicate retry across a snapshot would emit a second venue request,
     * which the broker handles as "unknown order" 35=9 — bounded and operator-visible. Adding to
     * the snapshot is a follow-up if we ever see customer-frontend doubles in production logs.
     */
    private final Map<UUID, Set<String>> requestedKeysIndex = new HashMap<>(INITIAL_INDEX_CAPACITY);

    private final ExpandableArrayBuffer egressBuffer = new ExpandableArrayBuffer(INITIAL_BUFFER_CAPACITY);

    /**
     * Buffer for {@link OrderAdmittedEvent} payloads written to {@link #eventsPublication}. Held separately
     * from {@link #egressBuffer} because the projection event is larger than the per-session
     * {@link OrderAcceptedEvent} and growing the egress buffer to the same size would waste memory on every
     * cluster session offer.
     */
    private final ExpandableArrayBuffer eventsBuffer = new ExpandableArrayBuffer(INITIAL_BUFFER_CAPACITY);

    // ---- Phase 4 slice 4b: snapshot observability ----
    // Cluster-resident code is plain Java + Agrona (no Spring magic in ClusteredService, per ADR 0001
    // §Discipline). The MeterRegistry is supplied via the constructor by OmsClusterNodeBootstrap; tests
    // that don't care about metrics use the default ctor and Metrics.globalRegistry (safe noop fallback
    // for unit tests).

    private final MeterRegistry meterRegistry;
    private final Timer snapshotWriteTimer;
    private final Timer snapshotLoadTimer;
    private final Counter snapshotWriteCounter;
    private final Counter snapshotLoadCounter;
    private final DistributionSummary snapshotWriteBytes;
    private final DistributionSummary snapshotLoadBytes;

    // ---- Phase 4 Tier 2.5 phase D-investigate: per-emit publish-timer + back-pressure counter ----
    // Falsifier for the "per-admit egress publish is the wall" hypothesis from the post-D-6 runbook.
    // emitAdmitted writes OrderAdmittedEvent to eventsPublication (projector consumes); emitAccepted
    // writes OrderAcceptedEvent via session.offer (ingress client demuxes the egress reply). Each
    // runs on the single cluster service thread and busy-waits via Thread.yield on Aeron back-pressure
    // (offer return < 0). The timer records total per-call wall time (success + busy-wait combined);
    // the counter increments per Thread.yield tick so a non-zero count signals BACK_PRESSURED.
    //
    // Determinism note: this uses Timer.start(meterRegistry) which reads System.nanoTime() under the
    // hood — same pattern as the snapshot timers above (line 385). Allowed because the recorded
    // duration is observability-only on a JVM-local registry; it does not influence emitted event
    // payloads, in-memory state, or what gets written to the cluster log. Replay re-fires these
    // meters identically modulo wall-clock noise; the per-replay deltas reset with the JVM, so
    // production scrapes only see steady-state activity.
    private final Timer eventsPublishTimerAdmitted;
    private final Counter eventsBackPressureCounterAdmitted;
    private final Timer sessionPublishTimerAccepted;
    private final Counter sessionBackPressureCounterAccepted;
    private final Counter cancelUnknownOrderCounter;

    /** Log marker when {@link #applyCancelOrder} misses {@code orderIndex} (ingress may return 410). */
    static final String CANCEL_UNKNOWN_ORDER_LOG_MARKER = "cluster-cancel-unknown-order";

    /**
     * Phase 4 slice 4h — drives the {@code oms.cluster.snapshot.age_seconds} freshness gauge that
     * the snapshot-freshness alert in {@code oms/docs/cluster-slo.md} is wired against.
     *
     * <p>Updated only on successful {@link #onTakeSnapshot(ExclusivePublication)} completion. Initial
     * value is set to the wall-clock time of service construction so a freshly booted cluster
     * reports a small, growing age (rather than the full epoch) until the first snapshot fires.
     * {@link #loadSnapshot(Image)} deliberately does <em>not</em> reset the timestamp: Aeron does
     * not expose the original write time of a loaded snapshot, and treating "load" as "fresh"
     * would hide the case where a member booted from a stale snapshot file with no subsequent
     * snapshot cron tick. The cluster bootstrap re-constructs this service per JVM, so the
     * "construct time = boot time" invariant holds.
     *
     * <p>{@code volatile} because the consensus-module thread writes via {@code onTakeSnapshot}
     * and the metrics-exporter HTTP thread reads via the {@link Gauge} accessor.
     */
    private volatile long lastSnapshotWriteEpochMs;

    private Cluster cluster;

    /** Commands applied since {@link #onStart} — used for post-replay validation logging only. */
    private long sessionMessageCountSinceStart;

    private long startWallClockMillis;

    private boolean replayValidationLogged;

    /** Set in {@link #onStart} when Aeron supplies a snapshot image to load. */
    private boolean snapshotLoadedOnStart;

    private io.aeron.Counter readyCounter;

    private io.aeron.Counter openOrdersCountCounter;

    /**
     * Side publication carrying {@link OrderAdmittedEvent}s for the Postgres projector (Phase 2).
     *
     * <p>Created on {@link #onStart(Cluster, Image)} via {@code cluster.aeron().addExclusivePublication}.
     * The corresponding Aeron Archive recording is started by the cluster bootstrap
     * ({@code OmsClusterNodeBootstrap.startEventsRecording}) <em>before</em> the cluster comes up, so the
     * Archive sees the publication as it appears and records every event from position 0 — which means
     * the projector's cursor advances along the same byte stream the Archive recorded.
     *
     * <p>Lifecycle: opened on {@code onStart}, closed on {@code onTerminate}. Replay (after a snapshot
     * load) does <em>not</em> re-emit prior events on this publication — the recording already holds
     * them; the projector reads from the recording, not from the live publication.
     */
    private ExclusivePublication eventsPublication;

    /**
     * Default constructor — meters register against {@link Metrics#globalRegistry}. Used by tests that
     * do not assert on snapshot observability and by historical call sites; production
     * {@code OmsClusterNodeBootstrap} uses {@link #OmsAdmissionClusteredService(MeterRegistry)} so the
     * embedded Prometheus exporter actually sees the meters.
     */
    public OmsAdmissionClusteredService() {
        this(Metrics.globalRegistry);
    }

    /**
     * Phase 4 slice 4b: meters wired through the cluster-node JVM's
     * {@code OmsClusterNodeMetricsExporter}. {@code outcome} tag distinguishes
     * snapshot writes (leader-driven by {@code onTakeSnapshot}) from snapshot loads
     * (replica recovery via {@code onStart(snapshotImage != null)}). Pre-registers all 6 meters so
     * {@code /metrics} exposes the names with zero-counts on a freshly booted cluster (operators can
     * set up dashboards / alerts before the first snapshot fires).
     */
    public OmsAdmissionClusteredService(MeterRegistry meterRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        Tags writeTags = Tags.of("outcome", "write");
        Tags loadTags = Tags.of("outcome", "load");
        this.snapshotWriteTimer = Timer.builder("oms.cluster.snapshot.duration")
                .description("Wall-clock time spent inside OmsAdmissionClusteredService.onTakeSnapshot / loadSnapshot.")
                .tags(writeTags)
                .register(meterRegistry);
        this.snapshotLoadTimer = Timer.builder("oms.cluster.snapshot.duration")
                .description("Wall-clock time spent inside OmsAdmissionClusteredService.onTakeSnapshot / loadSnapshot.")
                .tags(loadTags)
                .register(meterRegistry);
        this.snapshotWriteCounter = Counter.builder("oms.cluster.snapshot.events")
                .description("Snapshot writes (outcome=write) and loads (outcome=load) seen by the cluster service.")
                .tags(writeTags)
                .register(meterRegistry);
        this.snapshotLoadCounter = Counter.builder("oms.cluster.snapshot.events")
                .description("Snapshot writes (outcome=write) and loads (outcome=load) seen by the cluster service.")
                .tags(loadTags)
                .register(meterRegistry);
        this.snapshotWriteBytes = DistributionSummary.builder("oms.cluster.snapshot.bytes")
                .description("Bytes written to the snapshot publication (write) or consumed from the snapshot image (load).")
                .baseUnit("bytes")
                .tags(writeTags)
                .register(meterRegistry);
        this.snapshotLoadBytes = DistributionSummary.builder("oms.cluster.snapshot.bytes")
                .description("Bytes written to the snapshot publication (write) or consumed from the snapshot image (load).")
                .baseUnit("bytes")
                .tags(loadTags)
                .register(meterRegistry);
        // Phase 4 slice 4h — snapshot freshness gauge. Pre-registered so /metrics exposes the name on
        // a freshly booted cluster, and so dashboards / alerts wire to a present series before the
        // first snapshot. See class-level javadoc on `lastSnapshotWriteEpochMs` for the
        // "init = construct time, never reset by load" rationale.
        this.lastSnapshotWriteEpochMs = System.currentTimeMillis();
        Gauge.builder("oms.cluster.snapshot.age_seconds", this, OmsAdmissionClusteredService::currentSnapshotAgeSeconds)
                .description(
                        "Wall-clock seconds since the leader last successfully wrote a snapshot via"
                                + " onTakeSnapshot. Initial value = service-construct time (so a fresh boot"
                                + " starts near zero); not reset by snapshot load.")
                .baseUnit("seconds")
                .register(meterRegistry);

        // Phase 4 Tier 2.5 phase D-investigate — per-emit publish observability. Pre-registered so
        // /metrics on the cluster-node JVM (port 8089 by default) exposes the names with zero counts
        // on a freshly booted cluster, before the first admit fires.
        this.eventsPublishTimerAdmitted = Timer.builder("oms.cluster.service.events_offer_seconds")
                .description("Per-call wall time of eventsPublication.offer including back-pressure busy-wait.")
                .tag("event_kind", "admitted")
                .register(meterRegistry);
        this.eventsBackPressureCounterAdmitted = Counter.builder("oms.cluster.service.events_offer_back_pressure_total")
                .description("Aeron back-pressure ticks (Thread.yield iterations) per emit on eventsPublication.")
                .tag("event_kind", "admitted")
                .register(meterRegistry);
        this.sessionPublishTimerAccepted = Timer.builder("oms.cluster.service.session_offer_seconds")
                .description("Per-call wall time of session.offer (egress AcceptedEvent) including back-pressure busy-wait.")
                .tag("event_kind", "accepted")
                .register(meterRegistry);
        this.sessionBackPressureCounterAccepted = Counter.builder("oms.cluster.service.session_offer_back_pressure_total")
                .description("Aeron back-pressure ticks (Thread.yield iterations) per session.offer for AcceptedEvent.")
                .tag("event_kind", "accepted")
                .register(meterRegistry);
        this.cancelUnknownOrderCounter = Counter.builder("oms.cluster.cancel_unknown_order_total")
                .description(
                        "CancelOrderCommand applied with no matching orderIndex entry (silent no-op;"
                                + " typical after journal wipe while Postgres still shows WORKING)")
                .register(meterRegistry);
    }

    /**
     * Phase 4 slice 4h — accessor for the {@code oms.cluster.snapshot.age_seconds} gauge.
     * Returns wall-clock seconds since {@link #lastSnapshotWriteEpochMs} (construct time on a
     * fresh boot; reset on every successful {@code onTakeSnapshot}).
     */
    private double currentSnapshotAgeSeconds() {
        long ageMs = System.currentTimeMillis() - lastSnapshotWriteEpochMs;
        if (ageMs < 0L) {
            // Defensive: clock skew on the host can briefly go backwards. Clamp to zero so the
            // gauge never goes negative (which would be confusing in a freshness alert query).
            return 0.0;
        }
        return ageMs / 1000.0;
    }

    /** Test hook — replay validation fires once on first {@link #onRoleChange}. */
    boolean replayValidationLoggedForTest() {
        return replayValidationLogged;
    }

    /** Test hook — session messages counted since {@link #onStart}. */
    long sessionMessageCountSinceStartForTest() {
        return sessionMessageCountSinceStart;
    }

    /** Test hook — current readiness counter value, or -1 if absent. */
    long readinessCounterValueForTest() {
        return readyCounter == null ? -1L : readyCounter.get();
    }

    /**
     * Whether replay validation completed and the cluster may accept admission commands
     * (Phase 2.1 shutdown snapshot gate).
     */
    public boolean isReadyForClusterAdmission() {
        return readyCounter != null && readyCounter.get() == READINESS_VALUE_READY;
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        this.sessionMessageCountSinceStart = 0L;
        this.startWallClockMillis = System.currentTimeMillis();
        this.replayValidationLogged = false;
        this.snapshotLoadedOnStart = snapshotImage != null;
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
        // Determinism note: addExclusivePublication is a deterministic side effect — same cluster
        // configuration produces the same publication on every member / replay. The publication is the
        // bridge to the Archive recording, which is the durable projection signal.
        this.eventsPublication = cluster.aeron().addExclusivePublication(
                OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID);

        if (cluster.aeron() != null) {
            if (readyCounter != null) {
                readyCounter.close();
                readyCounter = null;
            }
            readyCounter = cluster.aeron().addCounter(READINESS_COUNTER_TYPE_ID, READINESS_COUNTER_LABEL);
            readyCounter.setOrdered(READINESS_VALUE_NOT_READY);
            if (openOrdersCountCounter != null) {
                openOrdersCountCounter.close();
                openOrdersCountCounter = null;
            }
            openOrdersCountCounter =
                    cluster.aeron().addCounter(OPEN_ORDERS_COUNT_COUNTER_TYPE_ID, OPEN_ORDERS_COUNT_COUNTER_LABEL);
            syncOpenOrdersCountCounter();
        }

        log.info(
                "OmsAdmissionClusteredService started; orders={}, role={}, eventsPub={}/{} readinessCounterId={} openOrdersCounterId={}",
                orderIndex.size(),
                cluster.role(),
                OmsClusterWireFormat.EVENTS_CHANNEL,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                readyCounter == null ? -1 : readyCounter.id(),
                openOrdersCountCounter == null ? -1 : openOrdersCountCounter.id());
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        // No per-session state today. Cluster client sessions are stateless; idempotency lives in the state machine.
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        // No per-session state to clean up. See onSessionOpen.
    }

    @Override
    public void onSessionMessage(
            ClientSession session,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length,
            Header header) {
        sessionMessageCountSinceStart++;
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            // Malformed; ignore. Logging here would break determinism on replay if log writes throw.
            return;
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        switch (typeId) {
            case OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER ->
                    applyAcceptOrder(session, timestamp, buffer, offset, length);
            case OmsClusterWireFormat.TYPE_ID_BATCH_ACCEPT_ORDER ->
                    applyBatchAcceptOrder(session, timestamp, buffer, offset, length);
            case OmsClusterWireFormat.TYPE_ID_APPLY_EXECUTION_REPORT ->
                    applyExecutionReport(timestamp, buffer, offset, length);
            case OmsClusterWireFormat.TYPE_ID_CANCEL_ORDER ->
                    applyCancelOrder(timestamp, buffer, offset, length);
            case OmsClusterWireFormat.TYPE_ID_REQUEST_CANCEL_ORDER ->
                    applyRequestCancelOrder(timestamp, buffer, offset, length);
            case OmsClusterWireFormat.TYPE_ID_REQUEST_REPLACE_ORDER ->
                    applyRequestReplaceOrder(timestamp, buffer, offset, length);
            default -> {
                // Unknown command — silently ignored. A real reject would require an event;
                // adding an UnknownCommandRejected event is a Phase 2 concern.
            }
        }
        syncOpenOrdersCountCounter();
    }

    private void syncOpenOrdersCountCounter() {
        if (openOrdersCountCounter == null) {
            return;
        }
        long open = 0L;
        for (AdmittedOrder order : orderIndex.values()) {
            if (!isTerminal(order.statusCode())) {
                open++;
            }
        }
        openOrdersCountCounter.setOrdered(open);
    }

    /** Test hook — published open-order count counter value, or -1 if absent. */
    long openOrdersCountCounterValueForTest() {
        return openOrdersCountCounter == null ? -1L : openOrdersCountCounter.get();
    }

    /**
     * Phase 4 Tier 2.5 phase D-6: dispatch each inner {@link AcceptOrderCommand} packed in a
     * {@link BatchAcceptOrderCommand} frame through the existing
     * {@link #applyAcceptOrder(io.aeron.cluster.service.ClientSession, long, DirectBuffer, int, int)}
     * path. Inner commands are applied in batch arrival order (deterministic); each one emits its
     * own {@link OrderAcceptedEvent} / {@link OrderRejectedEvent} keyed on its own {@code correlationId}
     * so the {@link OmsClusterIngressClient} egress demux is identical to the unbatched path.
     *
     * <p>Determinism: this method does no time / random / external I/O — it is pure dispatch into
     * existing deterministic logic. A malformed batch frame ({@link IllegalArgumentException} from
     * {@link BatchAcceptOrderCommand#forEachInner}) is silently skipped to preserve the
     * {@link #onSessionMessage} contract that bad frames must not break replay.
     *
     * <p>Idempotency: the batch frame is one cluster-log entry; on replay the same batch
     * re-decodes the same N inner commands in the same order. Per-order
     * {@code (accountId, clientIdempotencyKey)} dedupe in {@link #idempotencyIndex} short-circuits
     * each inner command identically to the unbatched path.
     */
    private void applyBatchAcceptOrder(
            io.aeron.cluster.service.ClientSession session,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length) {
        try {
            BatchAcceptOrderCommand.forEachInner(buffer, offset, length,
                    (innerBuf, innerOffset, innerLen) ->
                            applyAcceptOrder(session, timestamp, innerBuf, innerOffset, innerLen));
        } catch (IllegalArgumentException e) {
            // Malformed batch — silent like onSessionMessage: log writes during apply must not
            // throw on replay (file-system error during INFO would break determinism).
        }
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        // No timers scheduled today. Slice that adds market-hours / timeout cancels will use them.
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        Timer.Sample sample = Timer.start(meterRegistry);
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(INITIAL_BUFFER_CAPACITY);
        int p = 0;
        buffer.putInt(p, SNAPSHOT_MAGIC);
        p += Integer.BYTES;
        buffer.putInt(p, SNAPSHOT_SCHEMA_VERSION);
        p += Integer.BYTES;
        buffer.putInt(p, orderIndex.size());
        p += Integer.BYTES;
        for (AdmittedOrder o : orderIndex.values()) {
            p = o.encode(buffer, p);
        }
        // executionRefIndex: int orderCount, then per order { msb, lsb, int refCount, string[] refs }.
        // Empty entries are skipped (an order with no applied ERs has nothing to dedupe), so the
        // count is over orders-with-at-least-one-ref.
        int orderCountWithRefs = 0;
        for (Set<String> refs : executionRefIndex.values()) {
            if (refs != null && !refs.isEmpty()) {
                orderCountWithRefs++;
            }
        }
        buffer.putInt(p, orderCountWithRefs);
        p += Integer.BYTES;
        for (Map.Entry<UUID, Set<String>> e : executionRefIndex.entrySet()) {
            Set<String> refs = e.getValue();
            if (refs == null || refs.isEmpty()) {
                continue;
            }
            buffer.putLong(p, e.getKey().getMostSignificantBits());
            p += Long.BYTES;
            buffer.putLong(p, e.getKey().getLeastSignificantBits());
            p += Long.BYTES;
            buffer.putInt(p, refs.size());
            p += Integer.BYTES;
            for (String ref : refs) {
                byte[] bytes = ref.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                buffer.putInt(p, bytes.length);
                buffer.putBytes(p + Integer.BYTES, bytes);
                p += Integer.BYTES + bytes.length;
            }
        }
        // senderSeqIndex (v3): int senderCount, then per sender { string senderCompId, int seqCount, int[] seqs }.
        // Empty senders are skipped, same shape as the executionRefIndex above.
        int senderCountWithSeqs = 0;
        for (Set<Integer> seqs : senderSeqIndex.values()) {
            if (seqs != null && !seqs.isEmpty()) {
                senderCountWithSeqs++;
            }
        }
        buffer.putInt(p, senderCountWithSeqs);
        p += Integer.BYTES;
        for (Map.Entry<String, Set<Integer>> e : senderSeqIndex.entrySet()) {
            Set<Integer> seqs = e.getValue();
            if (seqs == null || seqs.isEmpty()) {
                continue;
            }
            byte[] senderBytes = e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            buffer.putInt(p, senderBytes.length);
            buffer.putBytes(p + Integer.BYTES, senderBytes);
            p += Integer.BYTES + senderBytes.length;
            buffer.putInt(p, seqs.size());
            p += Integer.BYTES;
            for (Integer seq : seqs) {
                buffer.putInt(p, seq);
                p += Integer.BYTES;
            }
        }
        long pos;
        while ((pos = snapshotPublication.offer(buffer, 0, p)) < 0L) {
            // BACK_PRESSURED / NOT_CONNECTED / ADMIN_ACTION — retry.
            // Aeron's standard snapshot publish loop. No external IO, no allocation here.
            if (pos == ExclusivePublication.CLOSED || pos == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("snapshot publication closed; pos=" + pos);
            }
            Thread.yield();
        }
        sample.stop(snapshotWriteTimer);
        snapshotWriteCounter.increment();
        snapshotWriteBytes.record(p);
        // Phase 4 slice 4h — drives oms.cluster.snapshot.age_seconds. Set AFTER the publish loop
        // returns success, so a back-pressured publish that throws does not advance freshness.
        lastSnapshotWriteEpochMs = System.currentTimeMillis();
    }

    private void loadSnapshot(Image snapshotImage) {
        Timer.Sample sample = Timer.start(meterRegistry);
        SnapshotLoader loader = new SnapshotLoader();
        while (!snapshotImage.isEndOfStream()) {
            int frags = snapshotImage.poll(loader, 1);
            if (frags == 0) {
                Thread.yield();
            }
        }
        sample.stop(snapshotLoadTimer);
        snapshotLoadCounter.increment();
        snapshotLoadBytes.record(loader.totalBytes);
        log.info("loaded admission snapshot: orders={}", orderIndex.size());
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        log.info("OmsAdmissionClusteredService role change -> {}", newRole);

        // Phase 2.4 (plans/oms-cluster-recovery-and-hardening.md): first role change after onStart
        // marks end of archive replay into the state machine. One greppable line for operators
        // and restart-pop-oms-cluster.sh (parity with ledger Phase 2.6).
        if (!replayValidationLogged) {
            replayValidationLogged = true;
            long replayElapsedMs = System.currentTimeMillis() - startWallClockMillis;
            log.info(
                    "replay validation: replayedMessages={} elapsedMs={} snapshotLoaded={}"
                            + " currentState[orders={} idempotency={} executionRefs={} senderSeq={}]",
                    sessionMessageCountSinceStart,
                    replayElapsedMs,
                    snapshotLoadedOnStart,
                    orderIndex.size(),
                    idempotencyIndex.size(),
                    executionRefIndex.size(),
                    senderSeqIndex.size());

            boolean emptyReplayNoSnapshot =
                    sessionMessageCountSinceStart == 0L
                            && orderIndex.isEmpty()
                            && idempotencyIndex.isEmpty()
                            && !snapshotLoadedOnStart;
            if (emptyReplayNoSnapshot) {
                log.warn(
                        "ANOMALY: replay completed with zero messages and zero in-memory state."
                                + " If the archive on disk is non-empty this indicates a broken"
                                + " recovery path. Follow oms/docs/runbooks/oms-cluster-recovery-incident.md.");
            }

            boolean allowReady = !emptyReplayNoSnapshot || parseAllowEmptyReplayFromEnv();
            if (readyCounter != null && allowReady) {
                readyCounter.setOrdered(READINESS_VALUE_READY);
                log.info(
                        "readiness counter -> READY (id={} value={})",
                        readyCounter.id(),
                        readyCounter.get());
            } else if (readyCounter != null) {
                log.warn(
                        "readiness counter remains NOT_READY (empty replay, OMS_READINESS_ALLOW_EMPTY_REPLAY=false)");
            }
            syncOpenOrdersCountCounter();
        }
    }

    private static boolean parseAllowEmptyReplayFromEnv() {
        String raw = System.getenv(ENV_READINESS_ALLOW_EMPTY_REPLAY);
        return raw != null && ("1".equals(raw.trim()) || "true".equalsIgnoreCase(raw.trim()));
    }

    @Override
    public void onTerminate(Cluster cluster) {
        log.info("OmsAdmissionClusteredService terminating; orders={}", orderIndex.size());
        CloseHelper.quietClose(eventsPublication);
        eventsPublication = null;
        if (readyCounter != null) {
            readyCounter.close();
            readyCounter = null;
        }
        if (openOrdersCountCounter != null) {
            openOrdersCountCounter.close();
            openOrdersCountCounter = null;
        }
    }

    // ------------------------------------------------------------------------
    // Command handlers (deterministic apply)
    // ------------------------------------------------------------------------

    private void applyAcceptOrder(
            ClientSession session, long clusterTimestampMillis, DirectBuffer buffer, int offset, int length) {
        AcceptOrderCommand cmd = AcceptOrderCommand.decode(buffer, offset, length);

        IdempotencyKey key = new IdempotencyKey(cmd.accountId(), cmd.clientIdempotencyKey());
        AdmittedOrder existing = idempotencyIndex.get(key);
        if (existing != null) {
            // Idempotent re-hit. The projector already saw the first emission; do not re-emit on the side
            // publication. Per-session egress still tells the originating client {duplicate=true}.
            emitAccepted(session, cmd.correlationId(), existing, clusterTimestampMillis, true);
            return;
        }

        AdmittedOrder admitted = new AdmittedOrder(
                cmd.orderId(),
                cmd.accountId(),
                cmd.clientIdempotencyKey(),
                cmd.accountIdHash(),
                cmd.instrumentSymbol(),
                cmd.side(),
                cmd.quantityScaled(),
                cmd.limitPriceScaledOrZero(),
                cmd.timeInForceCode(),
                cmd.ledgerBalanceIdOrNull(),
                /* version = */ 0,
                clusterTimestampMillis,
                /* statusCode = */ STATUS_WORKING,
                /* cumQtyScaled = */ 0L,
                cmd.shardId());
        idempotencyIndex.put(key, admitted);
        orderIndex.put(admitted.orderId(), admitted);

        emitAdmitted(cmd, clusterTimestampMillis, admitted.version());
        emitAccepted(session, cmd.correlationId(), admitted, clusterTimestampMillis, false);
    }

    /**
     * Apply path for venue execution reports (slice 3c). Determines the post-apply order state from
     * {@code cmd.execTypeCode}, mutates {@link #orderIndex} / {@link #idempotencyIndex} /
     * {@link #executionRefIndex} in place, and emits one {@link ExecutionAppliedEvent} on the side
     * publication. No external I/O; no logging at INFO+ on the hot path.
     *
     * <p><strong>Determinism:</strong> the cluster timestamp is supplied by the cluster, not read
     * from the wall clock. Branch decisions look only at {@code cmd} and the in-memory state — both
     * of which are reconstructed identically on every member and on replay.
     *
     * <p><strong>Idempotency:</strong> if {@code (orderId, venueExecRef)} is already in
     * {@link #executionRefIndex}, the apply is a no-op (no event emission, no state mutation, no
     * version bump). Re-delivery of the same ER over the cluster log therefore advances the log
     * position without producing a second projection-side row, which is what makes "duplicate ERs
     * from broker resends are silently absorbed" a cluster-side guarantee rather than a projector
     * concern.
     *
     * <p><strong>Unknown order:</strong> arriving with an {@code orderId} the cluster has never
     * admitted is silently ignored today. The legacy {@code ExecutionReportApplier} returned
     * {@code UNKNOWN_ORDER} as a status code; we will fold a deterministic
     * {@code OrderRejectedEvent} for unknown-order ERs into a follow-up slice if operations finds it
     * useful to surface them as projection rows. Slice 3c keeps the apply path side-effect-free for
     * unknown orders so a misrouted ER cannot wedge the projector.
     *
     * <p><strong>Invalid state:</strong> arriving for an already-terminal order
     * ({@code FILLED / CANCELLED / REJECTED}) is also a silent no-op. Same reasoning: we do not
     * want a late venue retry to flap a terminal order back to a live state.
     */
    private void applyExecutionReport(
            long clusterTimestampMillis, DirectBuffer buffer, int offset, int length) {
        ApplyExecutionReportCommand cmd = ApplyExecutionReportCommand.decode(buffer, offset, length);

        // Wire-level (senderCompId, msgSeqNum) dedupe (slice 3d). Empty senderCompId opts out —
        // see {@link #senderSeqIndex}. We check this BEFORE looking up the order so a wire-replay
        // is dropped at the cheapest possible point and unknown-order behaviour is reserved for
        // genuine misroutes (the alternative — order-lookup-then-wire-dedupe — would cause a
        // resend after the order is already terminal to take the "isTerminal" branch instead of
        // the "wire dedupe" branch, hiding the broker quirk from operators).
        Set<Integer> seenSeqs = null;
        if (!cmd.senderCompId().isEmpty()) {
            seenSeqs = senderSeqIndex.get(cmd.senderCompId());
            if (seenSeqs != null && seenSeqs.contains(cmd.msgSeqNum())) {
                return;
            }
        }

        AdmittedOrder order = orderIndex.get(cmd.orderId());
        if (order == null) {
            // Unknown order — silently ignored. See class-doc on apply-path discipline.
            return;
        }
        if (isTerminal(order.statusCode())) {
            // Already-terminal order — no-op. Late venue retries cannot flap a terminal order back
            // to a live status; the projector's {@code orders} CAS would reject anyway, but the
            // cluster has the same rule so the dedup set stays in sync with reality.
            return;
        }

        Set<String> seen = executionRefIndex.get(cmd.orderId());
        if (seen != null && seen.contains(cmd.venueExecRef())) {
            // Cluster-level (orderId, venueExecRef) dedupe. The first apply already emitted the
            // event; the projector wrote the executions row from the recording. No second emission
            // means no second projector row.
            return;
        }

        long newCumQty = order.cumQtyScaled();
        byte newStatus = order.statusCode();
        long newQuantityScaled = order.quantityScaled();
        long newLimitPriceScaledOrZero = order.limitPriceScaledOrZero();
        switch (cmd.execTypeCode()) {
            case ApplyExecutionReportCommand.EXEC_TYPE_TRADE -> {
                if (cmd.lastQtyScaled() <= 0L) {
                    // Trade ER with no quantity is malformed; ignore. Legacy applier never accepted
                    // a zero-quantity fill either; we mirror that here without a state mutation.
                    return;
                }
                long candidateCum = order.cumQtyScaled() + cmd.lastQtyScaled();
                if (candidateCum > order.quantityScaled()) {
                    // Fill overflow. {@code ExecutionReportApplier} threw IllegalStateException here
                    // and rolled back the TX; in the cluster we cannot throw without halting the
                    // state machine, so we silently drop the malformed fill and keep the prior
                    // state. Operations alerting on missing fills surfaces this; recovery is a
                    // venue-side ops concern, not a cluster correctness concern.
                    return;
                }
                newCumQty = candidateCum;
                newStatus = (newCumQty >= order.quantityScaled()) ? STATUS_FILLED : STATUS_PARTIALLY_FILLED;
            }
            case ApplyExecutionReportCommand.EXEC_TYPE_CANCEL -> newStatus = STATUS_CANCELLED;
            case ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT -> newStatus = STATUS_REJECTED;
            case ApplyExecutionReportCommand.EXEC_TYPE_REPLACE -> {
                // Wed-demo addition. ER ET=5 (REPLACED) from the broker carries the
                // authoritative new total OrderQty and limit price; cumQty is unchanged.
                // {@code lastQtyScaled} on a REPLACE wire-record is the new total OrderQty
                // (not a trade quantity); {@code lastPxScaled} is the new limit price (0
                // means market / unchanged). Reuses ER plumbing so the FIX inbound sink
                // only emits one command shape.
                if (cmd.lastQtyScaled() <= 0L) {
                    return;
                }
                if (cmd.lastQtyScaled() < order.cumQtyScaled()) {
                    // Broker honored a replace below our cum-fill (would imply a partial
                    // fill we don't know about). Refuse to apply — operations alert.
                    return;
                }
                newQuantityScaled = cmd.lastQtyScaled();
                if (cmd.lastPxScaled() > 0L) {
                    newLimitPriceScaledOrZero = cmd.lastPxScaled();
                }
                // Status derived from cumQty vs new qty (mirrors TRADE branch).
                if (newCumQty >= newQuantityScaled) {
                    newStatus = STATUS_FILLED;
                } else if (newCumQty > 0L) {
                    newStatus = STATUS_PARTIALLY_FILLED;
                } else {
                    newStatus = STATUS_WORKING;
                }
            }
            case ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT, ApplyExecutionReportCommand.EXEC_TYPE_REPLACE_REJECT -> {
                // 35=9 OrderCancelReject: the broker declined our cancel / modify. The order
                // is unchanged on the venue's books; cluster state stays as-is too. We still
                // bump version + emit one ExecutionAppliedEvent so the projector can write
                // the {@code OrderCancelRejected} / {@code OrderReplaceRejected} envelope
                // to {@code domain_event_outbox} (no orders.status mutation; the projector
                // branches on execTypeCode).
                // No status / cumQty / qty / price change; fall through to the version bump
                // + emit below.
            }
            default -> {
                // Unknown execTypeCode — silently dropped, same shape as unknown command typeIds in
                // {@link #onSessionMessage}.
                return;
            }
        }

        int newVersion = order.version() + 1;
        AdmittedOrder mutated = new AdmittedOrder(
                order.orderId(),
                order.accountId(),
                order.clientIdempotencyKey(),
                order.accountIdHash(),
                order.instrumentSymbol(),
                order.side(),
                newQuantityScaled,
                newLimitPriceScaledOrZero,
                order.timeInForceCode(),
                order.ledgerBalanceIdOrNull(),
                newVersion,
                order.acceptedAtMillis(),
                newStatus,
                newCumQty,
                order.shardId());
        orderIndex.put(mutated.orderId(), mutated);
        idempotencyIndex.put(
                new IdempotencyKey(mutated.accountId(), mutated.clientIdempotencyKey()), mutated);
        if (seen == null) {
            seen = new HashSet<>(4);
            executionRefIndex.put(cmd.orderId(), seen);
        }
        seen.add(cmd.venueExecRef());

        // Wire-level dedupe entry — only when the FIX caller supplied a senderCompId. Cluster-
        // internal callers (slice 3c codec tests, future cluster-driven ERs) keep senderCompId
        // empty and opt out of the wire dedupe.
        if (!cmd.senderCompId().isEmpty()) {
            if (seenSeqs == null) {
                seenSeqs = new HashSet<>(4);
                senderSeqIndex.put(cmd.senderCompId(), seenSeqs);
            }
            seenSeqs.add(cmd.msgSeqNum());
        }

        emitExecutionApplied(cmd, mutated, clusterTimestampMillis);
    }

    /**
     * Slice 4p — apply path for OMS-initiated {@link CancelOrderCommand}s. Mirrors the discipline
     * of {@link #applyExecutionReport}: idempotent on unknown / terminal orders, single state
     * mutation on a live order, single {@link OrderCancelAppliedEvent} emission on the side
     * publication.
     *
     * <p><strong>Idempotency:</strong>
     * <ul>
     *   <li>Unknown {@code orderId}: silent no-op. Compensator may race a never-admitted order
     *       (e.g. caller cancelled the inflight outbox row before admission committed).</li>
     *   <li>Already-terminal order ({@code FILLED} / {@code CANCELLED} / {@code REJECTED}): silent
     *       no-op. Crucially this is the documented race window: a venue fill that lands between
     *       the inflight-hold failure and the cancel command leaves the user with an unfunded
     *       position. Slice 4q's coalescer is the synchronous fix; slice 4p's compensator is the
     *       eventually-consistent backstop.</li>
     *   <li>Working / partially-filled: mutate {@link AdmittedOrder#statusCode()} to
     *       {@link #STATUS_CANCELLED}, bump {@link AdmittedOrder#version()}, emit one event.</li>
     * </ul>
     *
     * <p>No new dedupe set is introduced: a re-delivered {@link CancelOrderCommand} for the same
     * {@code orderId} sees the order is terminal and falls through the second bullet above. This
     * is the same pattern as {@link #applyExecutionReport}'s "already-terminal order" guard.
     */
    private void applyCancelOrder(
            long clusterTimestampMillis, DirectBuffer buffer, int offset, int length) {
        CancelOrderCommand cmd = CancelOrderCommand.decode(buffer, offset, length);

        AdmittedOrder order = orderIndex.get(cmd.orderId());
        if (order == null) {
            // Unknown order — silently ignored on the wire; ingress observes via Postgres poll → 410.
            cancelUnknownOrderCounter.increment();
            log.warn(
                    "{} orderId={} correlationId={} reason={}",
                    CANCEL_UNKNOWN_ORDER_LOG_MARKER,
                    cmd.orderId(),
                    cmd.correlationId(),
                    trimCancelReasonForLog(cmd.reason()));
            return;
        }
        if (isTerminal(order.statusCode())) {
            // Already-terminal — silent no-op. Crucially includes the fill-before-cancel race.
            return;
        }

        int newVersion = order.version() + 1;
        AdmittedOrder mutated = new AdmittedOrder(
                order.orderId(),
                order.accountId(),
                order.clientIdempotencyKey(),
                order.accountIdHash(),
                order.instrumentSymbol(),
                order.side(),
                order.quantityScaled(),
                order.limitPriceScaledOrZero(),
                order.timeInForceCode(),
                order.ledgerBalanceIdOrNull(),
                newVersion,
                order.acceptedAtMillis(),
                STATUS_CANCELLED,
                order.cumQtyScaled(),
                order.shardId());
        orderIndex.put(mutated.orderId(), mutated);
        idempotencyIndex.put(
                new IdempotencyKey(mutated.accountId(), mutated.clientIdempotencyKey()), mutated);

        emitOrderCancelApplied(mutated, clusterTimestampMillis, cmd.reason());
    }

    private static String trimCancelReasonForLog(String reason) {
        if (reason == null || reason.isEmpty()) {
            return "";
        }
        final int max = 240;
        String trimmed = reason.trim();
        return trimmed.length() > max ? trimmed.substring(0, max) : trimmed;
    }

    /**
     * Wed-demo addition: user-initiated cancel routed to the broker via FIX 35=F.
     *
     * <p>Distinct from {@link #applyCancelOrder} (which is the internal inflight-failure path that
     * never touches a venue and CANCELS the order immediately): this admits a venue-routed cancel,
     * leaves the order's status untouched, and emits an {@link OrderCancelRequestedEvent} for
     * {@code oms-fix-egress} to consume + send 35=F to the broker. The order only flips to
     * CANCELLED once the broker's ER (ET=4 via {@link ApplyExecutionReportCommand#EXEC_TYPE_CANCEL})
     * lands and walks through {@link #applyExecutionReport}.
     */
    private void applyRequestCancelOrder(
            long clusterTimestampMillis, DirectBuffer buffer, int offset, int length) {
        RequestCancelOrderCommand cmd = RequestCancelOrderCommand.decode(buffer, offset, length);

        AdmittedOrder order = orderIndex.get(cmd.orderId());
        if (order == null) {
            // Unknown order — silent no-op. HTTP layer races admission and a retry will land it.
            return;
        }
        if (isTerminal(order.statusCode())) {
            // Order is already FILLED / CANCELLED / REJECTED — no broker request to send.
            return;
        }
        // HTTP idempotency: dedupe on ('c' + clientRequestKey) per orderId. Empty key opts out.
        if (!cmd.clientRequestKey().isEmpty()) {
            String dedupeKey = "c:" + cmd.clientRequestKey();
            Set<String> seen = requestedKeysIndex.computeIfAbsent(cmd.orderId(), k -> new HashSet<>(2));
            if (!seen.add(dedupeKey)) {
                return;
            }
        }
        emitOrderCancelRequested(order, clusterTimestampMillis, cmd.clientRequestKey(), cmd.reason());
    }

    /**
     * Wed-demo addition: user-initiated modify (qty + limit price) routed to the broker via FIX
     * 35=G. Emits an {@link OrderReplaceRequestedEvent} for {@code oms-fix-egress}; the order's
     * quantity / price stay at their current values until the broker's ER (ET=5 via
     * {@link ApplyExecutionReportCommand#EXEC_TYPE_REPLACE}) lands.
     */
    private void applyRequestReplaceOrder(
            long clusterTimestampMillis, DirectBuffer buffer, int offset, int length) {
        RequestReplaceOrderCommand cmd = RequestReplaceOrderCommand.decode(buffer, offset, length);

        AdmittedOrder order = orderIndex.get(cmd.orderId());
        if (order == null) {
            return;
        }
        if (isTerminal(order.statusCode())) {
            return;
        }
        // Cumulative-fill overflow: the broker cannot honor a replace below what we've already
        // filled. Silent drop (HTTP layer surfaces 409 by reading the unchanged order state).
        if (cmd.newQuantityScaled() < order.cumQtyScaled()) {
            return;
        }
        if (!cmd.clientRequestKey().isEmpty()) {
            String dedupeKey = "r:" + cmd.clientRequestKey();
            Set<String> seen = requestedKeysIndex.computeIfAbsent(cmd.orderId(), k -> new HashSet<>(2));
            if (!seen.add(dedupeKey)) {
                return;
            }
        }
        emitOrderReplaceRequested(
                order,
                clusterTimestampMillis,
                cmd.newQuantityScaled(),
                cmd.newLimitPriceScaledOrZero(),
                cmd.clientRequestKey(),
                cmd.reason());
    }

    private void emitOrderCancelRequested(
            AdmittedOrder order, long requestedAtMillis, String clientRequestKey, String reason) {
        if (eventsPublication == null) {
            return;
        }
        OrderCancelRequestedEvent ev = new OrderCancelRequestedEvent(
                order.orderId(),
                order.quantityScaled(),
                order.cumQtyScaled(),
                requestedAtMillis,
                order.shardId(),
                order.side(),
                order.accountId(),
                order.instrumentSymbol(),
                clientRequestKey,
                reason);
        int len = ev.encode(eventsBuffer, 0);
        long pos;
        while ((pos = eventsPublication.offer(eventsBuffer, 0, len)) < 0L) {
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("eventsPublication offer closed; pos=" + pos);
            }
            Thread.yield();
        }
    }

    private void emitOrderReplaceRequested(
            AdmittedOrder order,
            long requestedAtMillis,
            long newQuantityScaled,
            long newLimitPriceScaledOrZero,
            String clientRequestKey,
            String reason) {
        if (eventsPublication == null) {
            return;
        }
        OrderReplaceRequestedEvent ev = new OrderReplaceRequestedEvent(
                order.orderId(),
                order.quantityScaled(),
                order.limitPriceScaledOrZero(),
                newQuantityScaled,
                newLimitPriceScaledOrZero,
                requestedAtMillis,
                order.shardId(),
                order.side(),
                order.timeInForceCode(),
                order.accountId(),
                order.instrumentSymbol(),
                clientRequestKey,
                reason);
        int len = ev.encode(eventsBuffer, 0);
        long pos;
        while ((pos = eventsPublication.offer(eventsBuffer, 0, len)) < 0L) {
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("eventsPublication offer closed; pos=" + pos);
            }
            Thread.yield();
        }
    }

    private void emitOrderCancelApplied(AdmittedOrder mutated, long cancelledAtMillis, String reason) {
        if (eventsPublication == null) {
            return;
        }
        // Phase 4 Tier 2.5 phase E-3b: shardId is now a first-class field on AdmittedOrder, seeded
        // from cmd.shardId() at admission time. We propagate it here so OrderCancelAppliedEvent
        // carries the order's actual owning shard. This is what makes the projector's E-2 shard
        // guard (OmsPostgresProjector#guardShardOrDrop) work for cancels too: at oms.shard.count>1
        // a cancel emitted by shard 1 will not be silently dropped by shard 0's projector.
        OrderCancelAppliedEvent ev = new OrderCancelAppliedEvent(
                mutated.orderId(),
                cancelledAtMillis,
                mutated.version(),
                mutated.shardId(),
                mutated.accountId(),
                mutated.accountIdHash(),
                mutated.instrumentSymbol(),
                reason);
        int len = ev.encode(eventsBuffer, 0);
        long pos;
        while ((pos = eventsPublication.offer(eventsBuffer, 0, len)) < 0L) {
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("eventsPublication offer closed; pos=" + pos);
            }
            Thread.yield();
        }
    }

    private void emitExecutionApplied(
            ApplyExecutionReportCommand cmd, AdmittedOrder mutated, long appliedAtMillis) {
        if (eventsPublication == null) {
            return;
        }
        ExecutionAppliedEvent ev = new ExecutionAppliedEvent(
                mutated.orderId(),
                mutated.cumQtyScaled(),
                cmd.lastQtyScaled(),
                cmd.lastPxScaled(),
                cmd.venueTsNanos(),
                appliedAtMillis,
                mutated.version(),
                cmd.execTypeCode(),
                mutated.statusCode(),
                cmd.rejectCodeOrZero(),
                mutated.accountId(),
                cmd.venueId(),
                cmd.venueExecRef(),
                cmd.rawEnvelopeJson());
        int len = ev.encode(eventsBuffer, 0);
        long pos;
        while ((pos = eventsPublication.offer(eventsBuffer, 0, len)) < 0L) {
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("eventsPublication offer closed; pos=" + pos);
            }
            Thread.yield();
        }
    }

    private void emitAdmitted(AcceptOrderCommand cmd, long acceptedAtMillis, int version) {
        if (eventsPublication == null) {
            // Defensive: cluster is shutting down. Not expected on the apply path; the consensus module
            // halts message delivery before onTerminate, but we guard so a late frame does not NPE.
            return;
        }
        OrderAdmittedEvent ev = OrderAdmittedEvent.fromAdmittedCommand(cmd, acceptedAtMillis, version);
        int len = ev.encode(eventsBuffer, 0);
        Timer.Sample sample = Timer.start(meterRegistry);
        int backPressureTicks = 0;
        long pos;
        while ((pos = eventsPublication.offer(eventsBuffer, 0, len)) < 0L) {
            // BACK_PRESSURED / NOT_CONNECTED is normal during steady-state and during projector reconnect.
            // The Archive subscribes to this publication on the cluster member and is always present.
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                sample.stop(eventsPublishTimerAdmitted);
                if (backPressureTicks > 0) {
                    eventsBackPressureCounterAdmitted.increment(backPressureTicks);
                }
                throw new IllegalStateException("eventsPublication offer closed; pos=" + pos);
            }
            backPressureTicks++;
            Thread.yield();
        }
        sample.stop(eventsPublishTimerAdmitted);
        if (backPressureTicks > 0) {
            eventsBackPressureCounterAdmitted.increment(backPressureTicks);
        }
    }

    private void emitAccepted(
            ClientSession session,
            long correlationId,
            AdmittedOrder admitted,
            long acceptedAtMillis,
            boolean duplicate) {
        OrderAcceptedEvent ev = new OrderAcceptedEvent(
                correlationId, admitted.orderId(), admitted.version(), duplicate, acceptedAtMillis);
        int len = ev.encode(egressBuffer, 0);
        Timer.Sample sample = Timer.start(meterRegistry);
        int backPressureTicks = 0;
        long pos;
        while ((pos = session.offer(egressBuffer, 0, len)) < 0L) {
            // BACK_PRESSURED is normal under load — retry. Closed/terminal positions throw.
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                sample.stop(sessionPublishTimerAccepted);
                if (backPressureTicks > 0) {
                    sessionBackPressureCounterAccepted.increment(backPressureTicks);
                }
                throw new IllegalStateException("session.offer closed; pos=" + pos);
            }
            backPressureTicks++;
            Thread.yield();
        }
        sample.stop(sessionPublishTimerAccepted);
        if (backPressureTicks > 0) {
            sessionBackPressureCounterAccepted.increment(backPressureTicks);
        }
    }

    // ------------------------------------------------------------------------
    // Test / projection helpers (read-only views of state)
    // ------------------------------------------------------------------------

    /** Visible for tests; do not mutate the returned reference. */
    public AdmittedOrder lookupByIdempotency(String accountId, String clientIdempotencyKey) {
        return idempotencyIndex.get(new IdempotencyKey(accountId, clientIdempotencyKey));
    }

    /** Visible for tests; do not mutate the returned reference. */
    public AdmittedOrder lookupByOrderId(UUID orderId) {
        return orderIndex.get(orderId);
    }

    /** Visible for tests. Total admitted orders in the state machine. */
    public int admittedOrderCount() {
        return orderIndex.size();
    }

    /**
     * Visible for tests. Returns whether the cluster has already applied an
     * {@link ApplyExecutionReportCommand} for {@code (orderId, venueExecRef)}; mirrors the dedupe
     * guard inside {@link #applyExecutionReport}. Returns {@code false} for unknown orders.
     */
    public boolean hasAppliedExecutionRef(UUID orderId, String venueExecRef) {
        Set<String> refs = executionRefIndex.get(orderId);
        return refs != null && refs.contains(venueExecRef);
    }

    /**
     * Visible for tests. Returns whether the cluster has already accepted an
     * {@link ApplyExecutionReportCommand} with the given {@code (senderCompId, msgSeqNum)} pair —
     * mirrors the wire-level dedupe guard inside {@link #applyExecutionReport}. Returns
     * {@code false} for empty {@code senderCompId} (wire-dedupe opt-out).
     */
    public boolean hasAppliedSenderSeq(String senderCompId, int msgSeqNum) {
        if (senderCompId == null || senderCompId.isEmpty()) {
            return false;
        }
        Set<Integer> seqs = senderSeqIndex.get(senderCompId);
        return seqs != null && seqs.contains(msgSeqNum);
    }

    private final class SnapshotLoader implements io.aeron.logbuffer.FragmentHandler {

        /**
         * Total bytes across all snapshot fragments seen by this loader. Snapshots today fit in a
         * single fragment, but we sum-on-fragment so the metric stays correct if Phase 2+ ever
         * splits the snapshot across multiple Aeron messages (see class doc §Snapshot format).
         */
        private long totalBytes;

        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
            totalBytes += length;
            int p = offset;
            int magic = buffer.getInt(p);
            p += Integer.BYTES;
            if (magic != SNAPSHOT_MAGIC) {
                throw new IllegalStateException("snapshot magic mismatch: 0x" + Integer.toHexString(magic));
            }
            int schemaVersion = buffer.getInt(p);
            p += Integer.BYTES;
            if (schemaVersion != SNAPSHOT_SCHEMA_VERSION) {
                throw new IllegalStateException("unsupported snapshot schema version " + schemaVersion);
            }
            int count = buffer.getInt(p);
            p += Integer.BYTES;
            for (int i = 0; i < count; i++) {
                AdmittedOrder o = AdmittedOrder.decode(buffer, p);
                p += o.encodedLength();
                idempotencyIndex.put(new IdempotencyKey(o.accountId(), o.clientIdempotencyKey()), o);
                orderIndex.put(o.orderId(), o);
            }
            int orderCountWithRefs = buffer.getInt(p);
            p += Integer.BYTES;
            for (int i = 0; i < orderCountWithRefs; i++) {
                long msb = buffer.getLong(p);
                p += Long.BYTES;
                long lsb = buffer.getLong(p);
                p += Long.BYTES;
                int refCount = buffer.getInt(p);
                p += Integer.BYTES;
                Set<String> refs = new HashSet<>(refCount);
                for (int j = 0; j < refCount; j++) {
                    int refLen = buffer.getInt(p);
                    byte[] bytes = new byte[refLen];
                    buffer.getBytes(p + Integer.BYTES, bytes);
                    refs.add(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
                    p += Integer.BYTES + refLen;
                }
                executionRefIndex.put(new UUID(msb, lsb), refs);
            }
            // senderSeqIndex (v3).
            int senderCountWithSeqs = buffer.getInt(p);
            p += Integer.BYTES;
            for (int i = 0; i < senderCountWithSeqs; i++) {
                int senderLen = buffer.getInt(p);
                byte[] senderBytes = new byte[senderLen];
                buffer.getBytes(p + Integer.BYTES, senderBytes);
                String sender = new String(senderBytes, java.nio.charset.StandardCharsets.UTF_8);
                p += Integer.BYTES + senderLen;
                int seqCount = buffer.getInt(p);
                p += Integer.BYTES;
                Set<Integer> seqs = new HashSet<>(seqCount);
                for (int j = 0; j < seqCount; j++) {
                    seqs.add(buffer.getInt(p));
                    p += Integer.BYTES;
                }
                senderSeqIndex.put(sender, seqs);
            }
        }
    }

    /** Composite key for the idempotency index. Records auto-generate equals/hashCode. */
    record IdempotencyKey(String accountId, String clientIdempotencyKey) {}

    // ---- Order status wire codes (slice 3c) ------------------------------------------------------
    // Kept in sync with {@code com.balh.oms.domain.OrderStatus} ordinal values so the projector can
    // round-trip {@link ExecutionAppliedEvent#newStatusCode()} into {@code OrderStatus} via
    // {@code OrderStatus.values()[newStatusCode]}. Adding a new status to the enum requires bumping
    // the snapshot schema version and reviewing every call site that compares against these
    // constants.

    /** Equivalent to {@code OrderStatus.WORKING.ordinal()}. */
    static final byte STATUS_WORKING = 2;

    /** Equivalent to {@code OrderStatus.PARTIALLY_FILLED.ordinal()}. */
    static final byte STATUS_PARTIALLY_FILLED = 3;

    /** Equivalent to {@code OrderStatus.FILLED.ordinal()}. */
    static final byte STATUS_FILLED = 4;

    /** Equivalent to {@code OrderStatus.CANCELLED.ordinal()}. */
    static final byte STATUS_CANCELLED = 5;

    /** Equivalent to {@code OrderStatus.REJECTED.ordinal()}. */
    static final byte STATUS_REJECTED = 6;

    private static boolean isTerminal(byte statusCode) {
        return statusCode == STATUS_FILLED
                || statusCode == STATUS_CANCELLED
                || statusCode == STATUS_REJECTED;
    }

    /**
     * Snapshot of an admitted order held in the state machine.
     *
     * <p>This is intentionally a small, dense record. It is the source of truth
     * for "what does the cluster know about this order"; Postgres is a
     * downstream projection of these.
     */
    public record AdmittedOrder(
            UUID orderId,
            String accountId,
            String clientIdempotencyKey,
            String accountIdHash,
            String instrumentSymbol,
            byte side,
            long quantityScaled,
            long limitPriceScaledOrZero,
            byte timeInForceCode,
            String ledgerBalanceIdOrNull,
            int version,
            long acceptedAtMillis,
            byte statusCode,
            long cumQtyScaled,
            int shardId) {

        int encode(MutableDirectBuffer buffer, int offset) {
            int p = offset;
            buffer.putLong(p, orderId.getMostSignificantBits());
            p += Long.BYTES;
            buffer.putLong(p, orderId.getLeastSignificantBits());
            p += Long.BYTES;
            buffer.putInt(p, version);
            p += Integer.BYTES;
            buffer.putLong(p, acceptedAtMillis);
            p += Long.BYTES;
            buffer.putLong(p, quantityScaled);
            p += Long.BYTES;
            buffer.putLong(p, limitPriceScaledOrZero);
            p += Long.BYTES;
            buffer.putLong(p, cumQtyScaled);
            p += Long.BYTES;
            // Phase 4 Tier 2.5 phase E-3b: shardId is preserved across replay/snapshot so the
            // cluster's emit paths (cancel today, future shard-aware emissions) carry the order's
            // owning shard without an out-of-band lookup. SNAPSHOT_SCHEMA_VERSION bumped to 4 in
            // the same slice; older snapshots are detected at SnapshotLoader.onFragment.
            buffer.putInt(p, shardId);
            p += Integer.BYTES;
            buffer.putByte(p++, side);
            buffer.putByte(p++, timeInForceCode);
            buffer.putByte(p++, statusCode);
            buffer.putByte(p++, (byte) (ledgerBalanceIdOrNull == null ? 0 : 1));
            p = writeString(buffer, p, accountId);
            p = writeString(buffer, p, clientIdempotencyKey);
            p = writeString(buffer, p, accountIdHash);
            p = writeString(buffer, p, instrumentSymbol);
            if (ledgerBalanceIdOrNull != null) {
                p = writeString(buffer, p, ledgerBalanceIdOrNull);
            }
            return p;
        }

        static AdmittedOrder decode(DirectBuffer buffer, int offset) {
            int p = offset;
            long msb = buffer.getLong(p);
            p += Long.BYTES;
            long lsb = buffer.getLong(p);
            p += Long.BYTES;
            int version = buffer.getInt(p);
            p += Integer.BYTES;
            long acceptedAtMillis = buffer.getLong(p);
            p += Long.BYTES;
            long quantityScaled = buffer.getLong(p);
            p += Long.BYTES;
            long limitPriceScaledOrZero = buffer.getLong(p);
            p += Long.BYTES;
            long cumQtyScaled = buffer.getLong(p);
            p += Long.BYTES;
            int shardId = buffer.getInt(p);
            p += Integer.BYTES;
            byte side = buffer.getByte(p++);
            byte timeInForceCode = buffer.getByte(p++);
            byte statusCode = buffer.getByte(p++);
            byte hasLedgerBalanceId = buffer.getByte(p++);
            String accountId = readString(buffer, p);
            p += stringByteLenAt(buffer, p);
            String clientIdempotencyKey = readString(buffer, p);
            p += stringByteLenAt(buffer, p);
            String accountIdHash = readString(buffer, p);
            p += stringByteLenAt(buffer, p);
            String instrumentSymbol = readString(buffer, p);
            p += stringByteLenAt(buffer, p);
            String ledgerBalanceId = null;
            if (hasLedgerBalanceId == 1) {
                ledgerBalanceId = readString(buffer, p);
            }
            return new AdmittedOrder(
                    new UUID(msb, lsb),
                    accountId,
                    clientIdempotencyKey,
                    accountIdHash,
                    instrumentSymbol,
                    side,
                    quantityScaled,
                    limitPriceScaledOrZero,
                    timeInForceCode,
                    ledgerBalanceId,
                    version,
                    acceptedAtMillis,
                    statusCode,
                    cumQtyScaled,
                    shardId);
        }

        int encodedLength() {
            int p = 0;
            // 6 longs (msb, lsb, acceptedAtMillis, quantityScaled, limitPriceScaledOrZero, cumQtyScaled).
            p += Long.BYTES * 6;
            // 2 ints (version, shardId).
            p += Integer.BYTES * 2;
            // 4 bytes (side, tif, statusCode, hasLedgerBalanceId).
            p += 4;
            p += stringByteLen(accountId);
            p += stringByteLen(clientIdempotencyKey);
            p += stringByteLen(accountIdHash);
            p += stringByteLen(instrumentSymbol);
            if (ledgerBalanceIdOrNull != null) {
                p += stringByteLen(ledgerBalanceIdOrNull);
            }
            return p;
        }

        private static int writeString(MutableDirectBuffer buffer, int offset, String s) {
            byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            buffer.putInt(offset, bytes.length);
            buffer.putBytes(offset + Integer.BYTES, bytes);
            return offset + Integer.BYTES + bytes.length;
        }

        private static String readString(DirectBuffer buffer, int offset) {
            int len = buffer.getInt(offset);
            byte[] bytes = new byte[len];
            buffer.getBytes(offset + Integer.BYTES, bytes);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        /**
         * UTF-8 byte length of {@code s} on the wire (4-byte length prefix + UTF-8 payload).
         * <strong>Allocates</strong> a {@code byte[]} via {@link String#getBytes(java.nio.charset.Charset)};
         * used at snapshot encode time where there is no buffer yet to read from. Decode-time cursor
         * advance uses the allocation-free {@link #stringByteLenAt} (slice 4f). Snapshot load
         * still calls {@link #encodedLength()} on each {@code AdmittedOrder} (line ~721), which
         * itself calls this — that is a known follow-up: replace it with a cursor-tracking
         * decode helper to make snapshot LOAD allocation-free as well. Snapshot loads are rare
         * (JVM startup) so the per-replay cost is small relative to the per-command hot path.
         */
        private static int stringByteLen(String s) {
            return Integer.BYTES + s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }

        /**
         * Number of bytes a string field occupies on the wire, read directly from the 4-byte
         * length prefix. Allocation-free; mirrors
         * {@code AcceptOrderCommand#stringByteLenAt}.
         */
        private static int stringByteLenAt(DirectBuffer buffer, int offset) {
            return Integer.BYTES + buffer.getInt(offset);
        }
    }
}
