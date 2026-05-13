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
    static final int SNAPSHOT_SCHEMA_VERSION = 3;

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

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
        // Determinism note: addExclusivePublication is a deterministic side effect — same cluster
        // configuration produces the same publication on every member / replay. The publication is the
        // bridge to the Archive recording, which is the durable projection signal.
        this.eventsPublication = cluster.aeron().addExclusivePublication(
                OmsClusterWireFormat.EVENTS_CHANNEL, OmsClusterWireFormat.EVENTS_STREAM_ID);
        log.info(
                "OmsAdmissionClusteredService started; orders={}, role={}, eventsPub={}/{}",
                orderIndex.size(),
                cluster.role(),
                OmsClusterWireFormat.EVENTS_CHANNEL,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
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
        if (length < OmsClusterWireFormat.HEADER_LENGTH) {
            // Malformed; ignore. Logging here would break determinism on replay if log writes throw.
            return;
        }
        int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
        switch (typeId) {
            case OmsClusterWireFormat.TYPE_ID_ACCEPT_ORDER ->
                    applyAcceptOrder(session, timestamp, buffer, offset, length);
            case OmsClusterWireFormat.TYPE_ID_APPLY_EXECUTION_REPORT ->
                    applyExecutionReport(timestamp, buffer, offset, length);
            default -> {
                // Unknown command — silently ignored. A real reject would require an event;
                // adding an UnknownCommandRejected event is a Phase 2 concern.
            }
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
    }

    @Override
    public void onTerminate(Cluster cluster) {
        log.info("OmsAdmissionClusteredService terminating; orders={}", orderIndex.size());
        CloseHelper.quietClose(eventsPublication);
        eventsPublication = null;
    }

    // ------------------------------------------------------------------------
    // Command handlers (deterministic apply)
    // ------------------------------------------------------------------------

    private void applyAcceptOrder(
            ClientSession session, long clusterTimestampNs, DirectBuffer buffer, int offset, int length) {
        AcceptOrderCommand cmd = AcceptOrderCommand.decode(buffer, offset, length);

        IdempotencyKey key = new IdempotencyKey(cmd.accountId(), cmd.clientIdempotencyKey());
        AdmittedOrder existing = idempotencyIndex.get(key);
        if (existing != null) {
            // Idempotent re-hit. The projector already saw the first emission; do not re-emit on the side
            // publication. Per-session egress still tells the originating client {duplicate=true}.
            emitAccepted(session, cmd.correlationId(), existing, clusterTimestampNs, true);
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
                clusterTimestampNs,
                /* statusCode = */ STATUS_WORKING,
                /* cumQtyScaled = */ 0L);
        idempotencyIndex.put(key, admitted);
        orderIndex.put(admitted.orderId(), admitted);

        emitAdmitted(cmd, clusterTimestampNs, admitted.version());
        emitAccepted(session, cmd.correlationId(), admitted, clusterTimestampNs, false);
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
            long clusterTimestampNs, DirectBuffer buffer, int offset, int length) {
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
                order.quantityScaled(),
                order.limitPriceScaledOrZero(),
                order.timeInForceCode(),
                order.ledgerBalanceIdOrNull(),
                newVersion,
                order.acceptedAtNanos(),
                newStatus,
                newCumQty);
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

        emitExecutionApplied(cmd, mutated, clusterTimestampNs);
    }

    private void emitExecutionApplied(
            ApplyExecutionReportCommand cmd, AdmittedOrder mutated, long appliedAtNanos) {
        if (eventsPublication == null) {
            return;
        }
        ExecutionAppliedEvent ev = new ExecutionAppliedEvent(
                mutated.orderId(),
                mutated.cumQtyScaled(),
                cmd.lastQtyScaled(),
                cmd.lastPxScaled(),
                cmd.venueTsNanos(),
                appliedAtNanos,
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

    private void emitAdmitted(AcceptOrderCommand cmd, long acceptedAtNanos, int version) {
        if (eventsPublication == null) {
            // Defensive: cluster is shutting down. Not expected on the apply path; the consensus module
            // halts message delivery before onTerminate, but we guard so a late frame does not NPE.
            return;
        }
        OrderAdmittedEvent ev = OrderAdmittedEvent.fromAdmittedCommand(cmd, acceptedAtNanos, version);
        int len = ev.encode(eventsBuffer, 0);
        long pos;
        while ((pos = eventsPublication.offer(eventsBuffer, 0, len)) < 0L) {
            // BACK_PRESSURED / NOT_CONNECTED is normal during steady-state and during projector reconnect.
            // The Archive subscribes to this publication on the cluster member and is always present.
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("eventsPublication offer closed; pos=" + pos);
            }
            Thread.yield();
        }
    }

    private void emitAccepted(
            ClientSession session,
            long correlationId,
            AdmittedOrder admitted,
            long acceptedAtNanos,
            boolean duplicate) {
        OrderAcceptedEvent ev = new OrderAcceptedEvent(
                correlationId, admitted.orderId(), admitted.version(), duplicate, acceptedAtNanos);
        int len = ev.encode(egressBuffer, 0);
        long pos;
        while ((pos = session.offer(egressBuffer, 0, len)) < 0L) {
            // BACK_PRESSURED is normal under load — retry. Closed/terminal positions throw.
            if (pos == io.aeron.Publication.CLOSED || pos == io.aeron.Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("session.offer closed; pos=" + pos);
            }
            Thread.yield();
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
            long acceptedAtNanos,
            byte statusCode,
            long cumQtyScaled) {

        int encode(MutableDirectBuffer buffer, int offset) {
            int p = offset;
            buffer.putLong(p, orderId.getMostSignificantBits());
            p += Long.BYTES;
            buffer.putLong(p, orderId.getLeastSignificantBits());
            p += Long.BYTES;
            buffer.putInt(p, version);
            p += Integer.BYTES;
            buffer.putLong(p, acceptedAtNanos);
            p += Long.BYTES;
            buffer.putLong(p, quantityScaled);
            p += Long.BYTES;
            buffer.putLong(p, limitPriceScaledOrZero);
            p += Long.BYTES;
            buffer.putLong(p, cumQtyScaled);
            p += Long.BYTES;
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
            long acceptedAtNanos = buffer.getLong(p);
            p += Long.BYTES;
            long quantityScaled = buffer.getLong(p);
            p += Long.BYTES;
            long limitPriceScaledOrZero = buffer.getLong(p);
            p += Long.BYTES;
            long cumQtyScaled = buffer.getLong(p);
            p += Long.BYTES;
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
                    acceptedAtNanos,
                    statusCode,
                    cumQtyScaled);
        }

        int encodedLength() {
            int p = 0;
            // 6 longs (msb, lsb, acceptedAtNanos, quantityScaled, limitPriceScaledOrZero, cumQtyScaled).
            p += Long.BYTES * 6;
            // 1 int (version).
            p += Integer.BYTES;
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
