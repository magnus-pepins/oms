package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.observability.metrics.OmsPipelineLatencyBounds;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.logbuffer.Header;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Spring-managed Aeron Cluster client used by the HTTP / gRPC ingress JVM to
 * submit admission commands to the OMS cluster and await the deterministic
 * response.
 *
 * <p>Phase 1a artifact of the Aeron Cluster substrate plan
 * ({@code system-documentation/plans/oms-aeron-cluster-substrate.md} and
 * {@code oms/docs/adr/0001-aeron-cluster-substrate.md}). Phase 1b wires it into
 * {@link com.balh.oms.ingress.OrderIngressService}; Phase 1c added a background
 * thread so cluster sessions survive idle periods between submits (Aeron's
 * default {@code ConsensusModule.sessionTimeoutNs} is 5s — without keep-alives
 * a quiet accept channel for &gt;5s would yank the session and the next submit
 * would fail with {@code cluster_unavailable}). Slice 4n folded that heartbeat
 * into the {@linkplain #egressPollerLoop() egress poller thread} so it also
 * demuxes cluster replies for in-flight submits.
 *
 * <h2>Threading model</h2>
 *
 * Aeron's {@link AeronCluster} is thread-confined: {@code offer},
 * {@code pollEgress}, and {@code sendKeepAlive} must not be called concurrently.
 * Phase 4 slice 4n pipelined the client so a single ingress-replica JVM is no
 * longer capped at one cluster commit at a time:
 *
 * <ul>
 *   <li>Calling threads (HTTP request threads inside
 *       {@link #submitAcceptOrder(AcceptOrderCommand, Duration)}) acquire
 *       {@link #clientLock} only long enough to call {@link AeronCluster#offer offer}.
 *       On success they release the lock and park on a
 *       {@link CompletableFuture} registered in {@link #pending} keyed by
 *       correlation id. On back-pressure they release the lock, park outside
 *       the lock, and retry — slow back-pressure no longer blocks every other
 *       in-flight submit.</li>
 *   <li>A dedicated daemon thread, {@linkplain #egressPollerLoop()}, holds the
 *       lock briefly each pass to call {@link AeronCluster#pollEgress pollEgress}
 *       (and, every {@link OmsConfig.Cluster.Client#getHeartbeatIntervalMs()}
 *       ms, {@link AeronCluster#sendKeepAlive sendKeepAlive}). Egress events
 *       arrive via {@link #egressListener} and complete the matching future in
 *       {@link #pending} directly — no more correlation-id-keyed map drained
 *       under the submit's own lock hold.</li>
 * </ul>
 *
 * Lock holds are bounded to single-digit microseconds in steady state, so per-JVM
 * commit throughput is limited by Aeron's {@code offer} rate, not by cluster RTT.
 *
 * <p>{@link #submitApplyExecutionReport(ApplyExecutionReportCommand, Duration)} is a
 * fire-and-forget offer (no session egress reply). A dedicated ER-offer daemon serializes
 * {@link AeronCluster#offer} under {@link #clientLock} and drains bursts of up to
 * {@link OmsConfig.Cluster.Client.ErOffer#getMaxPerLockPass()} frames per lock acquisition so
 * venue-egress virtual-thread
 * completions do not contend on the lock at 400+ routes/s (wire format has no batch ER frame).
 *
 * <h2>Determinism (per ADR 0001 §Discipline)</h2>
 *
 * The cluster client is the {@em edge}: this is where {@code Instant.now()},
 * {@code UUID.randomUUID()}, etc. are allowed. The {@link AcceptOrderCommand}
 * built by callers carries those values into the cluster. The cluster service
 * itself never observes wall time or random ids — that is what makes replay
 * deterministic across leader / follower / cold start.
 */
/**
 * <h2>Profile gating</h2>
 *
 * <p>Phase 4 Tier 2.5 phase E-3b demoted this class from a Spring {@code @Component} to a
 * factory-built {@code @Bean}: a single ingress JVM can now host {@code N} instances (one per
 * shard) when {@code oms.shard.count > 1}. The lifecycle annotations ({@link PostConstruct},
 * {@link PreDestroy}) still drive the connect / close handshake because Spring honours them on
 * any managed bean. Profile + property gating now live on
 * {@link com.balh.oms.cluster.OmsClusterClientsConfiguration} (and the back-compat shim it
 * exposes for FIX-egress at {@code oms.shard.count=1}).
 */
public class OmsClusterIngressClient {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterIngressClient.class);

    /**
     * Error handler installed on the underlying {@link AeronCluster} (and via it, on the internal
     * {@link io.aeron.Aeron}). Aeron's stock {@code DEFAULT_ERROR_HANDLER} calls {@code System.exit(-1)}
     * on {@link DriverTimeoutException}, which would tear down the OMS Spring Boot JVM whenever the
     * cluster's MediaDriver disappears (planned shutdown, network blip, restart). For a managed
     * Spring bean we want a transient driver loss to surface as a {@link io.aeron.exceptions.AeronException}
     * on the next {@code submitAcceptOrder} (mapped to {@link com.balh.oms.ingress.ClusterAdmissionException}
     * 503 by the controller), <em>not</em> a process exit. Logging only, never {@code System.exit}.
     */
    private static final ErrorHandler LOGGING_ERROR_HANDLER = (Throwable t) -> {
        if (t instanceof DriverTimeoutException) {
            log.warn(
                    "OMS cluster client lost MediaDriver heartbeat (driver timeout); subsequent submits will fail with cluster_unavailable until the cluster is reachable again",
                    t);
        } else {
            log.error("OMS cluster client background error", t);
        }
    };

    /**
     * Safety cap on {@link #pollEgressDrain(AeronCluster)} rounds per lock hold so a pathological
     * egress flood cannot spin forever under one {@link #clientLock} acquisition.
     */
    private static final int EGRESS_DRAIN_CAP = 256;

    /**
     * Per submit-thread encode scratch for {@link #submitAcceptOrder}. {@link ExpandableArrayBuffer}
     * is not thread-safe; one buffer per calling thread avoids a per-request allocation on the
     * burst hot path (Pop! 200 RPS showed commit_round_trip tail mass above 10 ms buckets from
     * combined poller skip + young-gen churn).
     */
    private static final ThreadLocal<ExpandableArrayBuffer> OFFER_BUFFER =
            ThreadLocal.withInitial(
                    () -> new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES));

    /** {@link Thread#join(long)} budget when stopping the egress poller thread on close. */
    private static final long POLLER_JOIN_MS = 2_000L;

    private final OmsConfig.Cluster.Client config;
    private final AtomicLong correlationIds = new AtomicLong(1);

    /**
     * In-flight {@link #submitAcceptOrder} calls awaiting their cluster reply, keyed by the
     * caller's correlation id. The submit thread {@link Map#put puts} an entry and parks on the
     * future; the egress poller thread (via {@link #egressListener}) {@link Map#remove removes}
     * the entry and {@link CompletableFuture#complete completes} it.
     *
     * <p>Slice 4n: replaces the previous correlation-id-keyed {@code received} map drained under
     * the submit's own lock hold. With this map a submit thread no longer holds
     * {@link #clientLock} while waiting for the cluster's egress round-trip, so per-JVM commit
     * throughput is no longer capped at {@code 1 / cluster_rtt}.
     */
    private final ConcurrentHashMap<Long, CompletableFuture<AdmissionResult>> pending =
            new ConcurrentHashMap<>();

    /**
     * Unfair (default) {@link ReentrantLock}: {@link AeronCluster} is thread-confined so offer,
     * {@link AeronCluster#pollEgress pollEgress}, and {@link AeronCluster#sendKeepAlive sendKeepAlive}
     * share one lock. Under burst admit (Pop! 200 RPS, 100 concurrent {@link #submitAcceptOrder}
     * threads) a <em>fair</em> lock queues the egress poller behind every waiting offer thread —
     * measured {@code ingress_cluster_accept_ms} rose to ~287 ms because OrderAccepted demux waited
     * for hundreds of fair-queue turns. Unfair barging plus {@link #signalEgressPoller()} after
     * each successful offer lets the poller interleave sub-ms {@link #pollEgressDrain} passes.
     * Blocking {@code lockInterruptibly()} (not slice-4n {@code tryLock} skip) is retained so egress
     * is never dropped on contention.
     *
     * <p>Burst path uses per-message {@link #offerWithBackpressure} when
     * {@link OmsConfig.Cluster.Client.AdmitBatch} is disabled (the default); the admit-batcher
     * daemon is not started in that mode.
     */
    private final ReentrantLock clientLock = new ReentrantLock();

    private final EgressListener egressListener = new EgressListener() {
        @Override
        public void onMessage(
                long clusterSessionId,
                long timestamp,
                DirectBuffer buffer,
                int offset,
                int length,
                Header header) {
            if (length < OmsClusterWireFormat.HEADER_LENGTH) {
                log.warn("cluster egress: dropped under-sized frame (length={})", length);
                return;
            }
            int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
            long correlationId;
            AdmissionResult result;
            try {
                if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED) {
                    OrderAcceptedEvent ev = OrderAcceptedEvent.decode(buffer, offset);
                    correlationId = ev.correlationId();
                    result = new AdmissionResult.Accepted(ev);
                } else if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_REJECTED) {
                    OrderRejectedEvent ev = OrderRejectedEvent.decode(buffer, offset);
                    correlationId = ev.correlationId();
                    result = new AdmissionResult.Rejected(ev);
                } else {
                    log.warn("cluster egress: unknown typeId={} length={}", typeId, length);
                    return;
                }
            } catch (RuntimeException e) {
                log.warn("cluster egress: failed to decode typeId={} length={}", typeId, length, e);
                return;
            }
            completeWaiter(correlationId, result);
        }

        @Override
        public void onSessionEvent(
                long correlationId,
                long clusterSessionId,
                long leadershipTermId,
                int leaderMemberId,
                EventCode code,
                String detail) {
            if (code != EventCode.OK) {
                log.info(
                        "cluster session event: code={} detail={} sessionId={} leaderId={}",
                        code,
                        detail,
                        clusterSessionId,
                        leaderMemberId);
            }
        }
    };

    /**
     * Slice 4n: complete the awaiting submit's future. Called from {@link #egressListener}
     * (under {@link #clientLock} held by the egress poller) and from unit tests that exercise
     * the demux path without spinning up a real cluster. {@link CompletableFuture#complete} is
     * non-blocking (just unparks the waiter); we hold the lock here only because the call site
     * inside {@code pollEgress} happens to hold it. A {@code null} entry means the caller
     * already gave up — drop the reply.
     */
    void completeWaiter(long correlationId, AdmissionResult result) {
        CompletableFuture<AdmissionResult> waiter = pending.remove(correlationId);
        if (waiter != null) {
            waiter.complete(result);
        } else {
            log.debug("cluster egress: orphan reply for correlationId={} (caller gave up)",
                    correlationId);
        }
    }

    /** Visible for testing — number of in-flight submits awaiting an egress reply. */
    int pendingCountForTest() {
        return pending.size();
    }

    /** Visible for testing — ER offers queued but not yet offered to the cluster. */
    int erOfferQueueDepthForTest() {
        return erOfferQueue.size();
    }

    /** Runtime backlog signal for venue-egress throttle decisions. */
    public int erOfferQueueDepth() {
        return erOfferQueue.size();
    }

    private volatile AeronCluster client;
    private volatile Thread egressPollerThread;
    private volatile boolean closing;

    // ---- Phase 4 Tier 2.5 phase D-6: admit-batcher state (only used when admit-batch enabled) ----

    /** Queue entry for the admit-batcher: one in-flight {@link AcceptOrderCommand} awaiting batch flush. */
    private record PendingBatchSubmit(
            AcceptOrderCommand cmd,
            long deadlineNanos,
            CompletableFuture<AdmissionResult> future,
            Timer.Sample sample) {}

    /**
     * Bounded MPSC queue that the admit-batcher daemon drains. Submit threads {@code offer()}
     * with park-and-retry on full; the daemon {@code poll()}s up to {@code maxBatchSize} per
     * pass. Null when admit-batching is disabled (slice 4n single-message path).
     */
    private final BlockingQueue<PendingBatchSubmit> admitBatchQueue;
    private volatile Thread admitBatcherThread;

    /**
     * One pre-encoded {@link ApplyExecutionReportCommand} awaiting offer by the ER daemon.
     * Callers encode on their thread and copy wire bytes so the daemon only calls
     * {@link AeronCluster#offer} under {@link #clientLock}.
     */
    private record PendingErSubmit(
            byte[] wireBytes, int wireLength, long deadlineNanos, CompletableFuture<Void> future) {}

    /**
     * Bounded queue for {@link #submitApplyExecutionReport}. Always active — unlike admit-batching
     * this does not change wire shape; it only serializes offers and drains bursts per lock hold.
     */
    private final BlockingQueue<PendingErSubmit> erOfferQueue;
    private volatile Thread erOfferDaemonThread;

    // ---- Phase 4 slice 4c: commit-round-trip timer ----
    // Single timer name `oms.cluster.client.commit_round_trip` covers both submit methods so a
    // single SLO ("p99 of caller-side cluster wait") works across the OMS topology. The `command`
    // tag distinguishes the two semantic shapes — accept_order is offer + egress reply round-trip,
    // apply_execution_report is fire-and-forget offer (cluster reply lands on a recorded side
    // stream consumed by the projector, not as session egress; see submitApplyExecutionReport's
    // class doc) — so dashboards / alerts can filter on `command` for the right SLO.
    //
    // Timers are pre-registered for all 6 (command × outcome) combinations so /actuator/prometheus
    // shows zero-counts on cold boot. Without that, Prometheus alert expressions like
    // `rate(...{outcome="commit"}[5m]) == 0` would silently match nothing instead of firing.

    static final String TIMER_NAME = "oms.cluster.client.commit_round_trip";
    static final String TAG_COMMAND = "command";
    static final String TAG_OUTCOME = "outcome";
    static final String COMMAND_ACCEPT_ORDER = "accept_order";
    static final String COMMAND_APPLY_EXECUTION_REPORT = "apply_execution_report";
    static final String COMMAND_CANCEL_ORDER = "cancel_order";
    static final String COMMAND_REQUEST_CANCEL_ORDER = "request_cancel_order";
    static final String COMMAND_REQUEST_REPLACE_ORDER = "request_replace_order";

    /**
     * Per-submit terminal state. Maps to the {@code outcome} tag on
     * {@link #TIMER_NAME} ({@link #lowerName()} is the tag value seen by Prometheus).
     */
    enum Outcome {
        /** Method completed normally (egress reply arrived for accept; offer succeeded for apply). */
        COMMIT,
        /** Method threw {@link TimeoutException} — back-pressure or egress wait exceeded the caller's deadline. */
        TIMEOUT,
        /** Any other failure (not-connected, interrupted, decode error, Aeron exception). */
        ERROR;

        String lowerName() {
            return name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    private final MeterRegistry meterRegistry;
    private final EnumMap<Outcome, Timer> acceptOrderTimers;
    private final EnumMap<Outcome, Timer> applyExecutionReportTimers;
    private final EnumMap<Outcome, Timer> cancelOrderTimers;
    private final EnumMap<Outcome, Timer> requestCancelOrderTimers;
    private final EnumMap<Outcome, Timer> requestReplaceOrderTimers;

    /**
     * Back-compat overload used by ITs and other tests that don't care about meter assertions.
     * Meters register against {@link Metrics#globalRegistry}, which is a noop-safe composite when
     * no actual registry is attached. Production wiring goes through
     * {@link com.balh.oms.cluster.OmsClusterClientsConfiguration} so
     * {@code /actuator/prometheus} on the cluster-client JVMs sees the real timers.
     */
    public OmsClusterIngressClient(OmsConfig config) {
        this(config, Metrics.globalRegistry);
    }

    /**
     * Convenience constructor: takes the whole {@link OmsConfig} and reads the flat
     * {@code oms.cluster.client} block. Used by tests and by the back-compat code path for
     * {@code oms.shard.count=1}. For multi-shard ingress JVMs the factory builds each client
     * with the per-shard {@link #OmsClusterIngressClient(OmsConfig.Cluster.Client, MeterRegistry)}
     * constructor below.
     */
    public OmsClusterIngressClient(OmsConfig config, MeterRegistry meterRegistry) {
        this(Objects.requireNonNull(config, "config").getCluster().getClient(), meterRegistry);
    }

    /**
     * Phase 4 Tier 2.5 phase E-3b primary constructor. Takes a resolved
     * {@link OmsConfig.Cluster.Client} so {@link com.balh.oms.cluster.OmsClusterClientsConfiguration}
     * can pass an already-merged per-shard config (template + shard override) without each
     * shard's client having to know about {@code OmsConfig.Shard.id}.
     */
    public OmsClusterIngressClient(
            OmsConfig.Cluster.Client clientConfig, MeterRegistry meterRegistry) {
        this.config = Objects.requireNonNull(clientConfig, "clientConfig");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.acceptOrderTimers = registerTimers(meterRegistry, COMMAND_ACCEPT_ORDER);
        this.applyExecutionReportTimers = registerTimers(meterRegistry, COMMAND_APPLY_EXECUTION_REPORT);
        this.cancelOrderTimers = registerTimers(meterRegistry, COMMAND_CANCEL_ORDER);
        this.requestCancelOrderTimers = registerTimers(meterRegistry, COMMAND_REQUEST_CANCEL_ORDER);
        this.requestReplaceOrderTimers = registerTimers(meterRegistry, COMMAND_REQUEST_REPLACE_ORDER);
        this.admitBatchQueue =
                this.config.getAdmitBatch().isEnabled()
                        ? new ArrayBlockingQueue<>(this.config.getAdmitBatch().getQueueCapacity())
                        : null;
        this.erOfferQueue =
                new ArrayBlockingQueue<>(this.config.getErOffer().getQueueCapacity());
    }

    private static EnumMap<Outcome, Timer> registerTimers(MeterRegistry registry, String command) {
        EnumMap<Outcome, Timer> timers = new EnumMap<>(Outcome.class);
        for (Outcome o : Outcome.values()) {
            timers.put(
                    o,
                    Timer.builder(TIMER_NAME)
                            .description(
                                    "Wall-clock time spent inside OmsClusterIngressClient submit methods waiting on the cluster"
                                            + " (offer + egress reply for accept_order; offer + back-pressure park for apply_execution_report).")
                            .tags(TAG_COMMAND, command, TAG_OUTCOME, o.lowerName())
                            // Phase 4j: enable bucket histogram so summarize_cluster_pipeline_deltas.py
                            // can extract p50/p99 from /actuator/prometheus the same way as the other
                            // pipeline timers. Without this, only count + sum surface, which is what
                            // surfaced as the "no buckets for commit_round_trip" caveat in the
                            // summarize script.
                            .publishPercentileHistogram()
                            .minimumExpectedValue(Duration.ofMillis(OmsPipelineLatencyBounds.MICROMETER_MIN_EXPECTED_MS))
                            .maximumExpectedValue(Duration.ofMillis(OmsPipelineLatencyBounds.MICROMETER_MAX_EXPECTED_MS))
                            .register(registry));
        }
        return timers;
    }

    /**
     * Allocates a unique correlation id for the next command. Callers must use
     * this for every command they submit to keep responses correlatable across
     * the client.
     */
    public long nextCorrelationId() {
        return correlationIds.getAndIncrement();
    }

    /** {@code true} once {@link #connect()} has produced a connected client and {@link #close()} has not run. */
    public boolean isConnected() {
        return client != null;
    }

    /**
     * Connects to the cluster using {@link AeronCluster#connect(AeronCluster.Context)}.
     * Retries (Aeron's own internal retry inside {@code connect}) within
     * {@link OmsConfig.Cluster.Client#getConnectTimeoutMs()}. Idempotent — a
     * second call after a successful connect is a no-op. Starts the
     * {@linkplain #egressPollerLoop() egress poller thread} on the first successful
     * connect.
     */
    @PostConstruct
    public void connect() {
        clientLock.lock();
        try {
            if (client != null) {
                return;
            }
            String aeronDirectory = config.getAeronDirectory();
            if (aeronDirectory == null || aeronDirectory.isBlank()) {
                throw new IllegalStateException(
                        "oms.cluster.client.aeron-directory must be set when oms.cluster.client.enabled=true");
            }
            AeronCluster.Context ctx = new AeronCluster.Context()
                    .aeronDirectoryName(aeronDirectory)
                    .ingressChannel(config.getIngressChannel())
                    .ingressEndpoints(config.getIngressEndpoints())
                    .egressChannel(config.getEgressChannel())
                    .egressListener(egressListener)
                    .errorHandler(LOGGING_ERROR_HANDLER)
                    .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(config.getMessageTimeoutMs()));

            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getConnectTimeoutMs());
            RuntimeException lastFailure = null;
            while (System.nanoTime() < deadline) {
                try {
                    this.client = AeronCluster.connect(ctx.clone());
                    OmsConfig.Cluster.Client.ErOffer erCfg = config.getErOffer();
                    log.info(
                            "OMS cluster client connected: aeronDir={} ingressEndpoints={} admitBatch={} erOffer={}",
                            aeronDirectory,
                            config.getIngressEndpoints(),
                            config.getAdmitBatch().isEnabled()
                                    ? "enabled(maxBatchSize=" + config.getAdmitBatch().getMaxBatchSize()
                                            + ", flushNanos=" + config.getAdmitBatch().getFlushIntervalNanos()
                                            + ", queueCap=" + config.getAdmitBatch().getQueueCapacity() + ")"
                                    : "disabled",
                            "maxPerLockPass=" + erCfg.getMaxPerLockPass()
                                    + ", queueCap=" + erCfg.getQueueCapacity()
                                    + ", drainNanos=" + erCfg.getDrainIntervalNanos());
                    startEgressPollerLocked();
                    startAdmitBatcherLocked();
                    startErOfferDaemonLocked();
                    return;
                } catch (RuntimeException e) {
                    lastFailure = e;
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                }
            }
            throw new IllegalStateException(
                    "OMS cluster client failed to connect within "
                            + config.getConnectTimeoutMs() + "ms",
                    lastFailure);
        } finally {
            clientLock.unlock();
        }
    }

    /** Closes the underlying {@link AeronCluster}. Idempotent. */
    @PreDestroy
    public void close() {
        closing = true;
        Thread t = egressPollerThread;
        egressPollerThread = null;
        if (t != null) {
            t.interrupt();
            try {
                t.join(POLLER_JOIN_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        Thread bt = admitBatcherThread;
        admitBatcherThread = null;
        if (bt != null) {
            bt.interrupt();
            try {
                bt.join(POLLER_JOIN_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        Thread er = erOfferDaemonThread;
        erOfferDaemonThread = null;
        if (er != null) {
            er.interrupt();
            try {
                er.join(POLLER_JOIN_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        drainErOfferQueueOnClose();
        clientLock.lock();
        try {
            if (client != null) {
                try {
                    client.close();
                } catch (RuntimeException e) {
                    log.warn("OMS cluster client close failed", e);
                } finally {
                    client = null;
                }
            }
        } finally {
            clientLock.unlock();
        }
        // Slice 4n: wake any submitters parked on futures that will now never get a reply.
        // ConcurrentHashMap.values() iterates a weakly consistent snapshot; we drain by removing
        // each key as we go so a late onMessage() racing against close() can't double-complete.
        IllegalStateException reason = new IllegalStateException(
                "OMS cluster client closed before egress reply arrived");
        List<Long> draining = new ArrayList<>(pending.keySet());
        for (Long correlationId : draining) {
            CompletableFuture<AdmissionResult> waiter = pending.remove(correlationId);
            if (waiter != null) {
                waiter.completeExceptionally(reason);
            }
        }
    }

    /**
     * Submits an {@link ApplyExecutionReportCommand} to the cluster as a fire-and-forget offer:
     * the call returns once Aeron has accepted the command into the log (deterministic order
     * preserved across all consumers — projector, future analytics, leader handover replay), but
     * does <em>not</em> wait for an {@link ExecutionAppliedEvent} reply. The cluster service emits
     * that event on the side publication recorded by Aeron Archive; consumers (projector, future
     * downstream analytics) read it from the recording, not as session egress, so a per-call wait
     * here would only mean blocking on a poll loop that produces nothing for this thread.
     *
     * <p>Used by {@code FixInboundClusterSink} on the {@code oms-fix-egress} JVM (slice 3d): the
     * inbound FIX {@code ExecutionReport} / {@code OrderCancelReject} hot path translates to this
     * command and offers it without holding a session-bound waiter. {@code (orderId, venueExecRef)}
     * dedupe still happens inside the cluster service ({@link OmsAdmissionClusteredService}); the
     * caller does not need to know whether this was a duplicate.
     *
     * <h3>Back-pressure handling</h3>
     *
     * <p>Same shape as {@link #submitAcceptOrder}: the {@link AeronCluster#offer offer} can return
     * negative on {@code BACK_PRESSURED} / {@code NOT_CONNECTED} / {@code ADMIN_ACTION}, and we
     * park {@link OmsConfig.Cluster.Client#getOfferBackpressureParkNanos()} ns and retry until the
     * deadline. On positive return we leave; we do not poll egress for this command.
     *
     * @throws IllegalStateException if not connected (see {@link #connect()}).
     * @throws TimeoutException if back-pressure persists past {@code timeout}.
     * @throws InterruptedException if the calling thread is interrupted while parked.
     */
    /**
     * Enqueues an ER cluster offer and returns immediately. The returned future completes once the
     * ER-offer daemon has accepted the command into the Aeron cluster log (or completes exceptionally
     * on timeout / disconnect). Callers that need wall-clock timing for Prometheus should attach
     * {@code whenComplete} rather than blocking a hot-path thread on {@link #submitApplyExecutionReport}.
     */
    public CompletableFuture<Void> submitApplyExecutionReportAsync(
            ApplyExecutionReportCommand cmd, Duration timeout) {
        Objects.requireNonNull(cmd, "cmd");
        Objects.requireNonNull(timeout, "timeout");
        if (client == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("OMS cluster client is not connected"));
        }

        ExpandableArrayBuffer buffer = OFFER_BUFFER.get();
        int written = cmd.encode(buffer, 0);
        byte[] wireBytes = new byte[written];
        buffer.getBytes(0, wireBytes, 0, written);

        CompletableFuture<Void> waiter = new CompletableFuture<>();
        PendingErSubmit submit =
                new PendingErSubmit(
                        wireBytes, written, System.nanoTime() + timeout.toNanos(), waiter);

        if (closing) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("OMS cluster client is closing"));
        }
        if (Thread.interrupted()) {
            return CompletableFuture.failedFuture(new InterruptedException());
        }
        if (erOfferQueue.offer(submit)) {
            signalErOfferDaemon();
            return waiter;
        }
        // Non-blocking back-pressure: fail fast so venue-egress can throttle via erOfferQueueDepth()
        // and re-queue completions instead of parking virtual threads on a full queue.
        return CompletableFuture.failedFuture(
                new TimeoutException(
                        "ER-offer queue full for correlationId=" + cmd.correlationId()));
    }

    public void submitApplyExecutionReport(ApplyExecutionReportCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        Objects.requireNonNull(cmd, "cmd");
        Objects.requireNonNull(timeout, "timeout");

        Timer.Sample sample = Timer.start(meterRegistry);
        Outcome outcome = Outcome.ERROR;
        try {
            CompletableFuture<Void> waiter = submitApplyExecutionReportAsync(cmd, timeout);
            long deadlineNanos = System.nanoTime() + timeout.toNanos();
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                if (waiter.isDone() && !waiter.isCompletedExceptionally()) {
                    outcome = Outcome.COMMIT;
                    return;
                }
                outcome = Outcome.TIMEOUT;
                throw new TimeoutException(
                        "ER-offer timeout for correlationId=" + cmd.correlationId());
            }
            try {
                waiter.get(remainingNanos, TimeUnit.NANOSECONDS);
                outcome = Outcome.COMMIT;
            } catch (java.util.concurrent.TimeoutException e) {
                outcome = Outcome.TIMEOUT;
                throw new TimeoutException(
                        "ER-offer timeout for correlationId=" + cmd.correlationId());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof InterruptedException ie) {
                    throw ie;
                }
                if (cause instanceof TimeoutException te) {
                    outcome = Outcome.TIMEOUT;
                    throw te;
                }
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new RuntimeException(
                        "ER-offer unexpected failure for correlationId=" + cmd.correlationId(), cause);
            }
        } finally {
            sample.stop(applyExecutionReportTimers.get(outcome));
        }
    }

    /** Phase B: fire-and-forget offer for {@link ApplyVenueResolutionCommand}. */
    public void submitApplyVenueResolution(ApplyVenueResolutionCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        Objects.requireNonNull(cmd, "cmd");
        Objects.requireNonNull(timeout, "timeout");

        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int written = cmd.encode(buffer, 0);
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        offerWithBackpressure(buffer, written, deadlineNanos, cmd.correlationId());
    }

    /**
     * Submits a {@link CancelOrderCommand} to the cluster as a fire-and-forget offer, mirroring
     * {@link #submitApplyExecutionReport}. Used by {@code LedgerInflightHoldFailureCompensator}
     * (slice 4p of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}) to cancel
     * an order whose buying-power hold failed on the async outbox path (Tier 2.5).
     *
     * <p>The cluster service applies the cancel deterministically and emits one
     * {@link OrderCancelAppliedEvent} on the side publication for the projector. The compensator
     * marks {@code ledger_inflight_outbox.compensated_at} once this method returns successfully
     * — the cluster log commit is the durability boundary; replay will re-apply the cancel
     * idempotently if the compensator crashes between offer-success and Postgres update.
     *
     * <h3>Back-pressure handling</h3>
     *
     * <p>Identical to {@link #submitApplyExecutionReport}: {@link AeronCluster#offer offer} can
     * return negative on {@code BACK_PRESSURED} / {@code NOT_CONNECTED} / {@code ADMIN_ACTION};
     * we park {@link OmsConfig.Cluster.Client#getOfferBackpressureParkNanos()} ns and retry until
     * the deadline. No egress wait — the projector reads from the recording.
     *
     * @throws IllegalStateException if not connected (see {@link #connect()}).
     * @throws TimeoutException if back-pressure persists past {@code timeout}.
     * @throws InterruptedException if the calling thread is interrupted while parked.
     */
    public void submitCancelOrder(CancelOrderCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        Objects.requireNonNull(cmd, "cmd");
        Objects.requireNonNull(timeout, "timeout");

        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int written = cmd.encode(buffer, 0);

        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        Timer.Sample sample = Timer.start(meterRegistry);
        Outcome outcome = Outcome.ERROR;
        try {
            offerWithBackpressure(buffer, written, deadlineNanos, cmd.correlationId());
            outcome = Outcome.COMMIT;
        } catch (TimeoutException e) {
            outcome = Outcome.TIMEOUT;
            throw e;
        } finally {
            sample.stop(cancelOrderTimers.get(outcome));
        }
    }

    /**
     * Wed-demo addition. Submits a {@link RequestCancelOrderCommand} fire-and-forget. The cluster
     * applies it deterministically and emits one {@link OrderCancelRequestedEvent} that
     * {@code oms-fix-egress} reads + turns into a 35=F OrderCancelRequest to the broker.
     *
     * <p>Distinct from {@link #submitCancelOrder} (which is the internal inflight-failure path
     * that immediately CANCELS the order without touching a venue): this one is the user-facing
     * cancel that round-trips the broker. The order's status only flips to CANCELLED once the
     * broker's 35=8 ET=4 ER lands and walks through {@code FixInboundClusterSink}.
     *
     * <p>Back-pressure semantics mirror {@link #submitCancelOrder}: offer-retry under
     * {@code BACK_PRESSURED} / {@code NOT_CONNECTED} / {@code ADMIN_ACTION} until the deadline.
     */
    public void submitRequestCancelOrder(RequestCancelOrderCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        submitFireAndForget(cmd::encode, cmd.correlationId(), timeout, requestCancelOrderTimers,
                "RequestCancelOrderCommand orderId=" + cmd.orderId());
    }

    /**
     * Wed-demo addition. Submits a {@link RequestReplaceOrderCommand} fire-and-forget. Mirrors
     * {@link #submitRequestCancelOrder}: cluster emits an {@link OrderReplaceRequestedEvent},
     * egress sends 35=G OrderCancelReplaceRequest, the order updates only when the broker's
     * 35=8 ET=5 lands.
     */
    public void submitRequestReplaceOrder(RequestReplaceOrderCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        submitFireAndForget(cmd::encode, cmd.correlationId(), timeout, requestReplaceOrderTimers,
                "RequestReplaceOrderCommand orderId=" + cmd.orderId());
    }

    /**
     * Common fire-and-forget submit. Buffer-encode the command, take the client lock, offer-retry
     * under back-pressure, release. No egress wait — caller proceeds as soon as the cluster log
     * commit succeeds. Used by {@link #submitRequestCancelOrder} +
     * {@link #submitRequestReplaceOrder} to keep their shapes identical (and to avoid copy-paste
     * drift on the back-pressure loop).
     */
    private void submitFireAndForget(
            java.util.function.ToIntBiFunction<org.agrona.MutableDirectBuffer, Integer> encoder,
            long correlationId,
            Duration timeout,
            EnumMap<Outcome, Timer> timers,
            String diagnosticContext)
            throws TimeoutException, InterruptedException {
        Objects.requireNonNull(timeout, "timeout");

        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int written = encoder.applyAsInt(buffer, 0);
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        Timer.Sample sample = Timer.start(meterRegistry);
        Outcome outcome = Outcome.ERROR;
        try {
            offerWithBackpressure(buffer, written, deadlineNanos, correlationId);
            outcome = Outcome.COMMIT;
        } catch (TimeoutException e) {
            outcome = Outcome.TIMEOUT;
            throw e;
        } finally {
            sample.stop(timers.get(outcome));
        }
    }

    /**
     * Submits an {@link AcceptOrderCommand} to the cluster and blocks the caller until the
     * matching {@link OrderAcceptedEvent} or {@link OrderRejectedEvent} egress arrives.
     *
     * <p>Slice 4n: pipelined. The submit thread takes {@link #clientLock} only long enough to
     * call {@link AeronCluster#offer offer}, then parks on a {@link CompletableFuture}
     * registered in {@link #pending}; the dedicated egress poller thread completes that future
     * when {@link AeronCluster#pollEgress pollEgress} delivers the matching reply. Back-pressure
     * retries also happen with the lock released, so a slow offer doesn't block other in-flight
     * submits on this JVM.
     *
     * @throws IllegalStateException if not connected (see {@link #connect()}).
     * @throws TimeoutException if no matching egress arrives within {@code timeout}.
     * @throws InterruptedException if the calling thread is interrupted while parked.
     */
    public AdmissionResult submitAcceptOrder(AcceptOrderCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        Objects.requireNonNull(cmd, "cmd");
        Objects.requireNonNull(timeout, "timeout");

        // Phase 4 Tier 2.5 phase D-6: when admit-batching is enabled, the calling thread
        // enqueues + parks on the future; the batcher daemon packs N admits into one Aeron
        // cluster message. Egress demux is identical (per-correlationId completion).
        if (admitBatchQueue != null) {
            return submitAcceptOrderViaBatcher(cmd, timeout);
        }

        ExpandableArrayBuffer buffer = OFFER_BUFFER.get();
        int written = cmd.encode(buffer, 0);

        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        long correlationId = cmd.correlationId();

        // Register the waiter BEFORE the offer so a sub-microsecond cluster reply that lands
        // before submit returns from `offer` finds the future to complete (otherwise onMessage
        // would log "orphan reply" and the submit would later time out for nothing).
        CompletableFuture<AdmissionResult> waiter = new CompletableFuture<>();
        if (pending.putIfAbsent(correlationId, waiter) != null) {
            // Correlation ids come from a per-client AtomicLong; collisions only happen if a
            // caller passes a hand-rolled command. Fail fast — the egress demux assumes uniqueness.
            throw new IllegalStateException(
                    "duplicate in-flight cluster correlationId=" + correlationId);
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        Outcome outcome = Outcome.ERROR;
        try {
            try {
                offerWithBackpressure(buffer, written, deadlineNanos, correlationId);
            } catch (RuntimeException | InterruptedException | TimeoutException e) {
                pending.remove(correlationId, waiter);
                if (e instanceof TimeoutException) {
                    outcome = Outcome.TIMEOUT;
                }
                throw e;
            }

            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                // Offer used the whole budget on back-pressure retries; salvage if the egress
                // poller already raced us to completion before checking the deadline.
                AdmissionResult salvaged = drainCompletedFuture(correlationId, waiter);
                if (salvaged != null) {
                    outcome = Outcome.COMMIT;
                    return salvaged;
                }
                outcome = Outcome.TIMEOUT;
                throw new TimeoutException(
                        "cluster egress wait timeout for correlationId=" + correlationId);
            }
            try {
                AdmissionResult result = waiter.get(remainingNanos, TimeUnit.NANOSECONDS);
                outcome = Outcome.COMMIT;
                return result;
            } catch (java.util.concurrent.TimeoutException e) {
                AdmissionResult salvaged = drainCompletedFuture(correlationId, waiter);
                if (salvaged != null) {
                    outcome = Outcome.COMMIT;
                    return salvaged;
                }
                outcome = Outcome.TIMEOUT;
                throw new TimeoutException(
                        "cluster egress wait timeout for correlationId=" + correlationId);
            } catch (ExecutionException e) {
                pending.remove(correlationId, waiter);
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new RuntimeException(
                        "cluster egress unexpected failure for correlationId=" + correlationId,
                        cause);
            }
        } finally {
            sample.stop(acceptOrderTimers.get(outcome));
        }
    }

    /**
     * Phase 4 Tier 2.5 phase D-6 — admit-batched submit path. Calling thread:
     * (1) registers its waiter in {@link #pending} (same demux contract as the unbatched path),
     * (2) enqueues a {@link PendingBatchSubmit} into {@link #admitBatchQueue} with park-and-retry
     *     on full, (3) waits on its future. The {@linkplain #admitBatcherLoop() batcher daemon}
     *     drains up to {@code maxBatchSize} entries per pass, packs them into one
     *     {@link BatchAcceptOrderCommand} frame, and offers via the existing client lock.
     */
    private AdmissionResult submitAcceptOrderViaBatcher(AcceptOrderCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        long correlationId = cmd.correlationId();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        CompletableFuture<AdmissionResult> waiter = new CompletableFuture<>();
        if (pending.putIfAbsent(correlationId, waiter) != null) {
            throw new IllegalStateException(
                    "duplicate in-flight cluster correlationId=" + correlationId);
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        Outcome outcome = Outcome.ERROR;

        PendingBatchSubmit submit = new PendingBatchSubmit(cmd, deadlineNanos, waiter, sample);
        try {
            // Park-and-retry enqueue. ArrayBlockingQueue.offer is wait-free on the fast path.
            long parkNanos = config.getAdmitBatch().getEnqueueParkNanos();
            while (true) {
                if (closing) {
                    pending.remove(correlationId, waiter);
                    throw new IllegalStateException("OMS cluster client is closing");
                }
                if (admitBatchQueue.offer(submit)) {
                    break;
                }
                if (System.nanoTime() > deadlineNanos) {
                    pending.remove(correlationId, waiter);
                    outcome = Outcome.TIMEOUT;
                    sample.stop(acceptOrderTimers.get(outcome));
                    throw new TimeoutException(
                            "admit-batch queue full past deadline for correlationId=" + correlationId);
                }
                parkOrThrow(parkNanos);
            }

            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                AdmissionResult salvaged = drainCompletedFuture(correlationId, waiter);
                if (salvaged != null) {
                    outcome = Outcome.COMMIT;
                    sample.stop(acceptOrderTimers.get(outcome));
                    return salvaged;
                }
                outcome = Outcome.TIMEOUT;
                sample.stop(acceptOrderTimers.get(outcome));
                throw new TimeoutException(
                        "admit-batch egress wait timeout for correlationId=" + correlationId);
            }
            try {
                AdmissionResult result = waiter.get(remainingNanos, TimeUnit.NANOSECONDS);
                outcome = Outcome.COMMIT;
                sample.stop(acceptOrderTimers.get(outcome));
                return result;
            } catch (java.util.concurrent.TimeoutException e) {
                AdmissionResult salvaged = drainCompletedFuture(correlationId, waiter);
                if (salvaged != null) {
                    outcome = Outcome.COMMIT;
                    sample.stop(acceptOrderTimers.get(outcome));
                    return salvaged;
                }
                outcome = Outcome.TIMEOUT;
                sample.stop(acceptOrderTimers.get(outcome));
                throw new TimeoutException(
                        "admit-batch egress wait timeout for correlationId=" + correlationId);
            } catch (ExecutionException e) {
                pending.remove(correlationId, waiter);
                sample.stop(acceptOrderTimers.get(outcome));
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new RuntimeException(
                        "admit-batch egress unexpected failure for correlationId=" + correlationId,
                        cause);
            }
        } catch (InterruptedException ie) {
            pending.remove(correlationId, waiter);
            sample.stop(acceptOrderTimers.get(outcome));
            throw ie;
        }
    }

    /**
     * Caller must hold {@link #clientLock}. Starts the admit-batcher daemon when admit-batching
     * is enabled. Idempotent.
     */
    private void startAdmitBatcherLocked() {
        if (admitBatchQueue == null || admitBatcherThread != null) {
            return;
        }
        Thread t = new Thread(this::admitBatcherLoop, "oms-cluster-client-admit-batcher");
        t.setDaemon(true);
        admitBatcherThread = t;
        t.start();
    }

    /**
     * Phase 4 Tier 2.5 phase D-6 daemon loop. Drains up to {@code maxBatchSize} pending submits
     * per pass, packs them into one {@link BatchAcceptOrderCommand}, offers via {@link #clientLock}.
     * On {@code BACK_PRESSURED}: parks {@link OmsConfig.Cluster.Client#getOfferBackpressureParkNanos()}
     * and retries with the same batch. On {@code NOT_CONNECTED} / Aeron exception: completes all
     * batched futures exceptionally and continues so subsequent batches can be tried.
     *
     * <p>Per-element deadline check before adding to the batch: a submit whose own per-call
     * timeout has already expired is dropped from the batch (its waiter has already been notified
     * by {@link #submitAcceptOrderViaBatcher}'s post-park check) — we don't waste a cluster log
     * slot on a request whose caller has given up.
     */
    private void admitBatcherLoop() {
        OmsConfig.Cluster.Client.AdmitBatch batchCfg = config.getAdmitBatch();
        int maxBatchSize = batchCfg.getMaxBatchSize();
        long flushIntervalNanos = batchCfg.getFlushIntervalNanos();
        long offerBackpressureParkNanos = config.getOfferBackpressureParkNanos();

        ExpandableArrayBuffer perCmdBuffer =
                new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        ExpandableArrayBuffer batchBuffer =
                new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_BATCH_COMMAND_BYTES);
        List<PendingBatchSubmit> drained = new ArrayList<>(maxBatchSize);

        while (!closing) {
            try {
                PendingBatchSubmit head = admitBatchQueue.poll(flushIntervalNanos, TimeUnit.NANOSECONDS);
                if (head == null) {
                    continue;
                }
                drained.clear();
                drained.add(head);
                while (drained.size() < maxBatchSize) {
                    PendingBatchSubmit p = admitBatchQueue.poll();
                    if (p == null) {
                        break;
                    }
                    drained.add(p);
                }

                long now = System.nanoTime();
                int dstOffset = BatchAcceptOrderCommand.firstInnerOffset(0);
                int kept = 0;
                List<PendingBatchSubmit> keptSubmits = new ArrayList<>(drained.size());
                for (PendingBatchSubmit s : drained) {
                    if (now > s.deadlineNanos()) {
                        failBatchSubmit(s, new TimeoutException(
                                "admit-batch submit expired before offer for correlationId="
                                        + s.cmd().correlationId()));
                        continue;
                    }
                    int innerLen = s.cmd().encode(perCmdBuffer, 0);
                    dstOffset = BatchAcceptOrderCommand.writeInner(
                            batchBuffer, dstOffset, perCmdBuffer, 0, innerLen);
                    keptSubmits.add(s);
                    kept++;
                }
                if (kept == 0) {
                    continue;
                }
                BatchAcceptOrderCommand.writeHeader(batchBuffer, 0, kept);
                int totalBytes = BatchAcceptOrderCommand.totalEncodedLength(dstOffset);

                offerBatchWithBackpressure(
                        batchBuffer, totalBytes, keptSubmits, offerBackpressureParkNanos);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.warn("admit-batcher loop error", e);
                // Fail any drained submits so callers don't hang on dead futures.
                for (PendingBatchSubmit s : drained) {
                    if (pending.remove(s.cmd().correlationId(), s.future())) {
                        s.future().completeExceptionally(e);
                    }
                }
            }
        }
        // Loop exit: closing == true. Drain remainder so callers parked on futures fail fast.
        IllegalStateException reason = new IllegalStateException(
                "OMS cluster client closed while admit-batch submit pending");
        admitBatchQueue.drainTo(drained);
        for (PendingBatchSubmit s : drained) {
            if (pending.remove(s.cmd().correlationId(), s.future())) {
                s.future().completeExceptionally(reason);
            }
        }
    }

    /**
     * Offer one batch buffer with back-pressure retry. Each retry honours the
     * <em>earliest</em> deadline among the batched submits — when that deadline passes, the
     * whole batch is failed (we don't want to keep trying once any caller has given up; the
     * cluster service would still apply the admit, but the projector / egress paths are
     * idempotent so that's safe).
     */
    private void offerBatchWithBackpressure(
            ExpandableArrayBuffer batchBuffer,
            int totalBytes,
            List<PendingBatchSubmit> batched,
            long parkNanos) {
        long earliestDeadline = Long.MAX_VALUE;
        for (PendingBatchSubmit s : batched) {
            if (s.deadlineNanos() < earliestDeadline) {
                earliestDeadline = s.deadlineNanos();
            }
        }
        while (!closing) {
            long offerResult;
            clientLock.lock();
            try {
                AeronCluster active = client;
                if (active == null) {
                    failBatchWith(batched, new IllegalStateException(
                            "OMS cluster client is not connected"));
                    return;
                }
                try {
                    offerResult = active.offer(batchBuffer, 0, totalBytes);
                } catch (RuntimeException e) {
                    failBatchWith(batched, e);
                    return;
                }
            } finally {
                clientLock.unlock();
            }
            if (offerResult >= 0L) {
                signalEgressPoller();
                return;
            }
            if (System.nanoTime() > earliestDeadline) {
                failBatchWith(batched, new TimeoutException(
                        "admit-batch offer back-pressure timeout"));
                return;
            }
            LockSupport.parkNanos(parkNanos);
        }
    }

    private void failBatchWith(List<PendingBatchSubmit> batched, Throwable cause) {
        for (PendingBatchSubmit s : batched) {
            failBatchSubmit(s, cause);
        }
    }

    private void failBatchSubmit(PendingBatchSubmit submit, Throwable cause) {
        if (pending.remove(submit.cmd().correlationId(), submit.future())) {
            submit.future().completeExceptionally(cause);
        }
    }

    /**
     * Slice 4n: take {@link #clientLock} only for the offer call; on back-pressure release the
     * lock, park, and retry. This keeps lock-hold to single-digit microseconds in steady state
     * so other in-flight submits and the egress poller make progress.
     */
    private void offerWithBackpressure(
            ExpandableArrayBuffer buffer, int written, long deadlineNanos, long correlationId)
            throws TimeoutException, InterruptedException {
        while (true) {
            clientLock.lockInterruptibly();
            long offerResult;
            try {
                AeronCluster active = client;
                if (active == null) {
                    throw new IllegalStateException("OMS cluster client is not connected");
                }
                offerResult = active.offer(buffer, 0, written);
            } finally {
                clientLock.unlock();
            }
            if (offerResult >= 0L) {
                signalEgressPoller();
                return;
            }
            if (System.nanoTime() > deadlineNanos) {
                throw new TimeoutException(
                        "cluster offer back-pressure timeout for correlationId=" + correlationId);
            }
            parkOrThrow(config.getOfferBackpressureParkNanos());
        }
    }

    /** Wake the egress poller so a freshly offered command gets demuxed without waiting a full park. */
    void signalEgressPollerForTest() {
        signalEgressPoller();
    }

    private void signalEgressPoller() {
        Thread poller = egressPollerThread;
        if (poller != null) {
            LockSupport.unpark(poller);
        }
    }

    /** Wake the ER-offer daemon so a freshly enqueued frame is offered without waiting a full park. */
    void signalErOfferDaemonForTest() {
        signalErOfferDaemon();
    }

    private void signalErOfferDaemon() {
        Thread daemon = erOfferDaemonThread;
        if (daemon != null) {
            LockSupport.unpark(daemon);
        }
    }

    /**
     * Drain all currently queued egress fragments in one lock hold. Returns the number of
     * {@link AeronCluster#pollEgress()} calls that returned a positive fragment count.
     */
    int pollEgressDrain(AeronCluster active) {
        int rounds = 0;
        while (rounds < EGRESS_DRAIN_CAP) {
            if (active.pollEgress() <= 0) {
                break;
            }
            rounds++;
        }
        return rounds;
    }

    /**
     * Race-resolution: when the submit hits its deadline, the egress poller may have completed
     * the future a few nanos earlier. {@code remove(key, value)} succeeds only if the entry is
     * still there — if the poller already removed it, we know the future is complete, so we
     * read it without blocking and return the salvaged value. Returns {@code null} if the
     * future is genuinely incomplete (real timeout).
     */
    private AdmissionResult drainCompletedFuture(
            long correlationId, CompletableFuture<AdmissionResult> waiter) {
        if (pending.remove(correlationId, waiter)) {
            return null;
        }
        if (waiter.isDone() && !waiter.isCompletedExceptionally()) {
            return waiter.getNow(null);
        }
        return null;
    }

    /** Caller must hold {@link #clientLock}. Starts the ER-offer daemon. Idempotent. */
    private void startErOfferDaemonLocked() {
        if (erOfferDaemonThread != null) {
            return;
        }
        Thread t = new Thread(this::erOfferDaemonLoop, "oms-cluster-client-er-offer");
        t.setDaemon(true);
        erOfferDaemonThread = t;
        t.start();
    }

    /**
     * Daemon loop: drain up to {@link OmsConfig.Cluster.Client.ErOffer#getMaxPerLockPass()} pre-encoded
     * ER frames per pass and offer as many as possible per {@link #clientLock} acquisition. Callers
     * park on per-submit futures — no direct lock contention from venue-egress virtual-thread
     * completions.
     */
    private void erOfferDaemonLoop() {
        OmsConfig.Cluster.Client.ErOffer erCfg = config.getErOffer();
        int maxPerLockPass = erCfg.getMaxPerLockPass();
        long parkNanos = config.getOfferBackpressureParkNanos();
        long drainIntervalNanos = erCfg.getDrainIntervalNanos();
        List<PendingErSubmit> drained = new ArrayList<>(maxPerLockPass);
        org.agrona.concurrent.UnsafeBuffer offerScratch =
                new org.agrona.concurrent.UnsafeBuffer(
                        new byte[OmsClusterWireFormat.MAX_COMMAND_BYTES]);

        while (!closing) {
            try {
                PendingErSubmit head =
                        erOfferQueue.poll(drainIntervalNanos, TimeUnit.NANOSECONDS);
                if (head == null) {
                    continue;
                }
                drained.clear();
                drained.add(head);
                erOfferQueue.drainTo(drained, maxPerLockPass - 1);
                offerErBurstWithBackpressure(drained, offerScratch, parkNanos, maxPerLockPass);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.warn("ER-offer daemon loop error", e);
                for (PendingErSubmit s : drained) {
                    failErSubmit(s, e);
                }
            }
        }
        drainErOfferQueueOnClose();
    }

    /**
     * Offer a drained burst. Holds {@link #clientLock} across sequential frames and, while the
     * cluster accepts offers, greedily polls additional queued frames up to
     * {@code maxPerLockPass} per extension — amortising lock trips across sustained bursts.
     * Releases and parks on cluster back-pressure before retrying the held frame.
     */
    private void offerErBurstWithBackpressure(
            List<PendingErSubmit> batch,
            org.agrona.concurrent.UnsafeBuffer offerScratch,
            long parkNanos,
            int maxPerLockPass) throws InterruptedException {
        int idx = 0;
        PendingErSubmit heldForRetry = null;
        boolean signaledPoller = false;

        while (!closing
                && (heldForRetry != null
                        || idx < batch.size()
                        || !erOfferQueue.isEmpty())) {
            clientLock.lockInterruptibly();
            boolean backPressured = false;
            try {
                AeronCluster active = client;
                if (active == null) {
                    if (heldForRetry != null) {
                        failErSubmit(
                                heldForRetry,
                                new IllegalStateException("OMS cluster client is not connected"));
                        heldForRetry = null;
                    }
                    while (idx < batch.size()) {
                        failErSubmit(
                                batch.get(idx),
                                new IllegalStateException("OMS cluster client is not connected"));
                        idx++;
                    }
                    return;
                }

                outer:
                while (!backPressured && !closing) {
                    PendingErSubmit current = heldForRetry;
                    if (current == null) {
                        if (idx >= batch.size()) {
                            current = erOfferQueue.poll();
                            if (current == null) {
                                break;
                            }
                        } else {
                            current = batch.get(idx);
                        }
                    } else {
                        heldForRetry = null;
                    }

                    if (System.nanoTime() > current.deadlineNanos()) {
                        failErSubmit(current, new TimeoutException("ER-offer expired before cluster offer"));
                        if (idx < batch.size() && batch.get(idx) == current) {
                            idx++;
                        }
                        continue;
                    }

                    offerScratch.wrap(current.wireBytes());
                    long offerResult;
                    try {
                        offerResult = active.offer(offerScratch, 0, current.wireLength());
                    } catch (RuntimeException e) {
                        failErSubmit(current, e);
                        if (idx < batch.size() && batch.get(idx) == current) {
                            idx++;
                        }
                        continue;
                    }
                    if (offerResult < 0L) {
                        heldForRetry = current;
                        backPressured = true;
                        break;
                    }
                    current.future().complete(null);
                    if (idx < batch.size() && batch.get(idx) == current) {
                        idx++;
                    }

                    int extensionBudget = maxPerLockPass - 1;
                    while (extensionBudget-- > 0 && !backPressured && !closing) {
                        PendingErSubmit ext = erOfferQueue.poll();
                        if (ext == null) {
                            break outer;
                        }
                        if (System.nanoTime() > ext.deadlineNanos()) {
                            failErSubmit(ext, new TimeoutException("ER-offer expired before cluster offer"));
                            continue;
                        }
                        offerScratch.wrap(ext.wireBytes());
                        try {
                            offerResult = active.offer(offerScratch, 0, ext.wireLength());
                        } catch (RuntimeException e) {
                            failErSubmit(ext, e);
                            continue;
                        }
                        if (offerResult < 0L) {
                            heldForRetry = ext;
                            backPressured = true;
                            break;
                        }
                        ext.future().complete(null);
                    }
                }
            } finally {
                clientLock.unlock();
            }

            if (backPressured) {
                PendingErSubmit retry = heldForRetry;
                if (retry != null && System.nanoTime() > retry.deadlineNanos()) {
                    failErSubmit(retry, new TimeoutException("ER-offer back-pressure timeout"));
                    heldForRetry = null;
                    continue;
                }
                if (!signaledPoller) {
                    signalEgressPoller();
                    signaledPoller = true;
                }
                parkOrThrow(parkNanos);
            }
        }
        if (idx > 0 && !signaledPoller) {
            signalEgressPoller();
        }
    }

    private void failErSubmit(PendingErSubmit submit, Throwable cause) {
        submit.future().completeExceptionally(cause);
    }

    /**
     * Race-resolution when the caller hits its deadline but the daemon completed the offer first.
     */
    private boolean drainCompletedErFuture(PendingErSubmit submit) {
        if (erOfferQueue.remove(submit)) {
            return false;
        }
        CompletableFuture<Void> waiter = submit.future();
        return waiter.isDone() && !waiter.isCompletedExceptionally();
    }

    private void drainErOfferQueueOnClose() {
        IllegalStateException reason =
                new IllegalStateException("OMS cluster client closed while ER offer pending");
        List<PendingErSubmit> remainder = new ArrayList<>();
        erOfferQueue.drainTo(remainder);
        for (PendingErSubmit s : remainder) {
            s.future().completeExceptionally(reason);
        }
    }

    /** Caller must hold {@link #clientLock}. */
    private void startEgressPollerLocked() {
        if (egressPollerThread != null) {
            return;
        }
        closing = false;
        Thread t = new Thread(this::egressPollerLoop, "oms-cluster-client-egress-poller");
        t.setDaemon(true);
        egressPollerThread = t;
        t.start();
    }

    /**
     * Slice 4n daemon loop: every {@link OmsConfig.Cluster.Client#getEgressPollParkNanos()} ns we
     * acquire {@link #clientLock} and {@linkplain #pollEgressDrain(AeronCluster) drain} all
     * queued egress so cluster replies for in-flight submits ({@link #pending}) complete their
     * futures promptly under burst offer contention. {@link AeronCluster#sendKeepAlive()} is
     * called inside the same lock acquisition every
     * {@link OmsConfig.Cluster.Client#getHeartbeatIntervalMs()} ms so the cluster session
     * survives idle periods. Submit threads {@linkplain #signalEgressPoller() unpark} this
     * thread after each successful offer so demux latency is not gated on
     * {@link OmsConfig.Cluster.Client#getEgressPollParkNanos()} alone.
     */
    private void egressPollerLoop() {
        long parkNanos = config.getEgressPollParkNanos();
        long heartbeatIntervalNanos = TimeUnit.MILLISECONDS.toNanos(config.getHeartbeatIntervalMs());
        long nextHeartbeatDeadlineNanos = System.nanoTime() + heartbeatIntervalNanos;
        while (!closing) {
            try {
                clientLock.lockInterruptibly();
                try {
                    AeronCluster active = client;
                    if (active != null) {
                        try {
                            pollEgressDrain(active);
                            if (System.nanoTime() >= nextHeartbeatDeadlineNanos) {
                                active.sendKeepAlive();
                                nextHeartbeatDeadlineNanos =
                                        System.nanoTime() + heartbeatIntervalNanos;
                            }
                        } catch (RuntimeException e) {
                            log.warn("OMS cluster client egress poll/keep-alive failed", e);
                        }
                    }
                } finally {
                    clientLock.unlock();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.warn("OMS cluster client egress poller loop error", e);
            }
            if (Thread.interrupted()) {
                return;
            }
            LockSupport.parkNanos(parkNanos);
        }
    }

    private static void parkOrThrow(long nanos) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        LockSupport.parkNanos(nanos);
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }
}
