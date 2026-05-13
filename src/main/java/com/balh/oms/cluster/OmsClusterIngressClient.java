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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
 * {@link com.balh.oms.ingress.OrderIngressService}; Phase 1c added the
 * background {@linkplain #heartbeatLoop() heartbeat thread} so cluster sessions
 * survive idle periods between submits (Aeron's default
 * {@code ConsensusModule.sessionTimeoutNs} is 5s — without keep-alives a quiet
 * accept channel for &gt;5s would yank the session and the next submit would
 * fail with {@code cluster_unavailable}).
 *
 * <h2>Threading model</h2>
 *
 * Aeron's {@link AeronCluster} is thread-confined: {@code offer},
 * {@code pollEgress}, and {@code sendKeepAlive} must not be called concurrently.
 * Two threads touch the client: the calling thread inside
 * {@link #submitAcceptOrder(AcceptOrderCommand, Duration)} and the daemon
 * heartbeat thread. They serialize on {@link #clientLock} (a
 * {@link ReentrantLock}). The heartbeat thread uses
 * {@link ReentrantLock#tryLock(long, TimeUnit) tryLock} so it does not stall
 * when a slow submit holds the lock — that submit's own
 * {@code pollEgress}/{@code sendKeepAlive} loop keeps the session alive
 * during its critical section.
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
 * <p>Slice 3d of the Aeron Cluster substrate plan extended the consumer set: the
 * {@code oms-fix-egress} JVM also offers commands now ({@link ApplyExecutionReportCommand} on
 * inbound venue ER). The {@code @Profile} therefore pins to
 * {@link OmsProfiles#CLUSTER_CLIENT_PROFILE} (ingress JVMs <em>and</em> {@code oms-fix-egress}),
 * and {@code @ConditionalOnProperty(oms.cluster.client.enabled=true)} provides the per-deployment
 * opt-in. Worker / projector profiles never load this bean even if their config flips the
 * property on, because they fall outside the profile expression.
 */
@Component
@Profile(OmsProfiles.CLUSTER_CLIENT_PROFILE)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
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
     * Max time the heartbeat thread spends trying to acquire {@link #clientLock} per pass before
     * skipping that tick. Bounded so that a slow submit can't choke the heartbeat indefinitely; the
     * submit's own {@code pollEgress} keeps the session alive while it holds the lock.
     */
    private static final long HEARTBEAT_LOCK_TRY_NANOS = 1_000_000L;

    /** {@link Thread#join(long)} budget when stopping the heartbeat thread on close. */
    private static final long HEARTBEAT_JOIN_MS = 2_000L;

    private final OmsConfig.Cluster.Client config;
    private final AtomicLong correlationIds = new AtomicLong(1);

    /**
     * Egress events received but not yet drained by the submitting thread.
     * Keyed by correlation id. Mutations and reads happen only while
     * {@link #clientLock} is held, so a plain {@link HashMap} is safe.
     */
    private final Map<Long, AdmissionResult> received = new HashMap<>();

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
            try {
                if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED) {
                    OrderAcceptedEvent ev = OrderAcceptedEvent.decode(buffer, offset);
                    received.put(ev.correlationId(), new AdmissionResult.Accepted(ev));
                } else if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_REJECTED) {
                    OrderRejectedEvent ev = OrderRejectedEvent.decode(buffer, offset);
                    received.put(ev.correlationId(), new AdmissionResult.Rejected(ev));
                } else {
                    log.warn("cluster egress: unknown typeId={} length={}", typeId, length);
                }
            } catch (RuntimeException e) {
                log.warn("cluster egress: failed to decode typeId={} length={}", typeId, length, e);
            }
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

    private volatile AeronCluster client;
    private volatile Thread heartbeatThread;
    private volatile boolean closing;

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

    /**
     * Back-compat overload used by ITs and other tests that don't care about meter assertions.
     * Meters register against {@link Metrics#globalRegistry}, which is a noop-safe composite when
     * no actual registry is attached. Production wiring goes through the {@code @Autowired} ctor
     * below so {@code /actuator/prometheus} on the cluster-client JVMs sees the real timers.
     */
    public OmsClusterIngressClient(OmsConfig config) {
        this(config, Metrics.globalRegistry);
    }

    @Autowired
    public OmsClusterIngressClient(OmsConfig config, MeterRegistry meterRegistry) {
        this.config = Objects.requireNonNull(config, "config").getCluster().getClient();
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.acceptOrderTimers = registerTimers(meterRegistry, COMMAND_ACCEPT_ORDER);
        this.applyExecutionReportTimers = registerTimers(meterRegistry, COMMAND_APPLY_EXECUTION_REPORT);
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
     * {@linkplain #heartbeatLoop() heartbeat thread} on the first successful
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
                    log.info(
                            "OMS cluster client connected: aeronDir={} ingressEndpoints={}",
                            aeronDirectory,
                            config.getIngressEndpoints());
                    startHeartbeatLocked();
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
        Thread t = heartbeatThread;
        heartbeatThread = null;
        if (t != null) {
            t.interrupt();
            try {
                t.join(HEARTBEAT_JOIN_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
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
    public void submitApplyExecutionReport(ApplyExecutionReportCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        Objects.requireNonNull(cmd, "cmd");
        Objects.requireNonNull(timeout, "timeout");

        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int written = cmd.encode(buffer, 0);

        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        Timer.Sample sample = Timer.start(meterRegistry);
        Outcome outcome = Outcome.ERROR;
        try {
            clientLock.lockInterruptibly();
            try {
                AeronCluster active = client;
                if (active == null) {
                    throw new IllegalStateException("OMS cluster client is not connected");
                }
                long offerResult;
                do {
                    offerResult = active.offer(buffer, 0, written);
                    if (offerResult < 0L) {
                        if (System.nanoTime() > deadlineNanos) {
                            outcome = Outcome.TIMEOUT;
                            throw new TimeoutException(
                                    "cluster offer back-pressure timeout for ApplyExecutionReportCommand orderId="
                                            + cmd.orderId() + " venueExecRef=" + cmd.venueExecRef());
                        }
                        parkOrThrow(config.getOfferBackpressureParkNanos());
                    }
                } while (offerResult < 0L);
                outcome = Outcome.COMMIT;
            } finally {
                clientLock.unlock();
            }
        } finally {
            sample.stop(applyExecutionReportTimers.get(outcome));
        }
    }

    /**
     * Submits an {@link AcceptOrderCommand} to the cluster and blocks the
     * caller until the matching {@link OrderAcceptedEvent} or
     * {@link OrderRejectedEvent} egress arrives.
     *
     * @throws IllegalStateException if not connected (see {@link #connect()}).
     * @throws TimeoutException if no matching egress arrives within {@code timeout}.
     * @throws InterruptedException if the calling thread is interrupted while
     *         parked between poll passes.
     */
    public AdmissionResult submitAcceptOrder(AcceptOrderCommand cmd, Duration timeout)
            throws TimeoutException, InterruptedException {
        Objects.requireNonNull(cmd, "cmd");
        Objects.requireNonNull(timeout, "timeout");

        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int written = cmd.encode(buffer, 0);

        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        Timer.Sample sample = Timer.start(meterRegistry);
        Outcome outcome = Outcome.ERROR;
        try {
            clientLock.lockInterruptibly();
            try {
                AeronCluster active = client;
                if (active == null) {
                    throw new IllegalStateException("OMS cluster client is not connected");
                }

                long offerResult;
                do {
                    offerResult = active.offer(buffer, 0, written);
                    if (offerResult < 0L) {
                        if (System.nanoTime() > deadlineNanos) {
                            outcome = Outcome.TIMEOUT;
                            throw new TimeoutException(
                                    "cluster offer back-pressure timeout for correlationId=" + cmd.correlationId());
                        }
                        parkOrThrow(config.getOfferBackpressureParkNanos());
                    }
                } while (offerResult < 0L);

                while (true) {
                    AdmissionResult result = received.remove(cmd.correlationId());
                    if (result != null) {
                        outcome = Outcome.COMMIT;
                        return result;
                    }
                    active.pollEgress();
                    result = received.remove(cmd.correlationId());
                    if (result != null) {
                        outcome = Outcome.COMMIT;
                        return result;
                    }
                    if (System.nanoTime() > deadlineNanos) {
                        outcome = Outcome.TIMEOUT;
                        throw new TimeoutException(
                                "cluster egress wait timeout for correlationId=" + cmd.correlationId());
                    }
                    parkOrThrow(config.getEgressPollParkNanos());
                }
            } finally {
                clientLock.unlock();
            }
        } finally {
            sample.stop(acceptOrderTimers.get(outcome));
        }
    }

    /** Caller must hold {@link #clientLock}. */
    private void startHeartbeatLocked() {
        if (heartbeatThread != null) {
            return;
        }
        closing = false;
        Thread t = new Thread(this::heartbeatLoop, "oms-cluster-client-heartbeat");
        t.setDaemon(true);
        heartbeatThread = t;
        t.start();
    }

    /**
     * Daemon loop: every {@link OmsConfig.Cluster.Client#getHeartbeatIntervalMs()} we try to
     * grab {@link #clientLock} (without blocking submits) and call
     * {@link AeronCluster#sendKeepAlive()} + {@link AeronCluster#pollEgress()}. If a submit holds
     * the lock, we skip — that submit is doing its own polling so the session stays warm.
     */
    private void heartbeatLoop() {
        long parkNanos = TimeUnit.MILLISECONDS.toNanos(config.getHeartbeatIntervalMs());
        while (!closing) {
            try {
                if (clientLock.tryLock(HEARTBEAT_LOCK_TRY_NANOS, TimeUnit.NANOSECONDS)) {
                    try {
                        AeronCluster active = client;
                        if (active != null) {
                            try {
                                active.sendKeepAlive();
                                active.pollEgress();
                            } catch (RuntimeException e) {
                                log.warn("OMS cluster client heartbeat call failed", e);
                            }
                        }
                    } finally {
                        clientLock.unlock();
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.warn("OMS cluster client heartbeat loop error", e);
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
