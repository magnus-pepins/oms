package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Spring-managed Aeron Cluster client used by the HTTP / gRPC ingress JVM to
 * submit admission commands to the OMS cluster and await the deterministic
 * response.
 *
 * <p>This is the Phase 1a artifact of the Aeron Cluster substrate plan
 * ({@code system-documentation/plans/oms-aeron-cluster-substrate.md} and
 * {@code oms/docs/adr/0001-aeron-cluster-substrate.md}). Only the cluster
 * client connection and synchronous {@code submitAcceptOrder} are wired here;
 * Phase 1b replaces the Chronicle path inside {@link com.balh.oms.ingress.OrderIngressService}
 * by calling this bean.
 *
 * <h2>Threading model (Phase 1a)</h2>
 *
 * Aeron's {@link AeronCluster} is thread-confined: {@code offer} and {@code pollEgress}
 * must not be called concurrently. We satisfy this by serializing every
 * client-touching method on a single intrinsic lock. At expected OMS volumes
 * (low thousands of orders / second) the per-call latency is dominated by Raft
 * commit and projector lag, not by lock contention; Phase 4 introduces a
 * dedicated agent thread with a queue if a benchmark shows the lock matters.
 *
 * <h2>Determinism (per ADR 0001 §Discipline)</h2>
 *
 * The cluster client is the {@em edge}: this is where {@code Instant.now()},
 * {@code UUID.randomUUID()}, etc. are allowed. The {@link AcceptOrderCommand}
 * built by callers carries those values into the cluster. The cluster service
 * itself never observes wall time or random ids — that is what makes replay
 * deterministic across leader / follower / cold start.
 */
@Component
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public class OmsClusterIngressClient {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterIngressClient.class);

    private final OmsConfig.Cluster.Client config;
    private final AtomicLong correlationIds = new AtomicLong(1);

    /**
     * Egress events received but not yet drained by the submitting thread.
     * Keyed by correlation id. Mutations and reads happen only while
     * {@link #clientLock} is held, so a plain {@link HashMap} is safe.
     */
    private final Map<Long, AdmissionResult> received = new HashMap<>();

    private final Object clientLock = new Object();

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

    public OmsClusterIngressClient(OmsConfig config) {
        this.config = Objects.requireNonNull(config, "config").getCluster().getClient();
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
     * second call after a successful connect is a no-op.
     */
    @PostConstruct
    public void connect() {
        synchronized (clientLock) {
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
        }
    }

    /** Closes the underlying {@link AeronCluster}. Idempotent. */
    @PreDestroy
    public void close() {
        synchronized (clientLock) {
            if (client != null) {
                try {
                    client.close();
                } catch (RuntimeException e) {
                    log.warn("OMS cluster client close failed", e);
                } finally {
                    client = null;
                }
            }
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

        synchronized (clientLock) {
            AeronCluster active = client;
            if (active == null) {
                throw new IllegalStateException("OMS cluster client is not connected");
            }

            long offerResult;
            do {
                offerResult = active.offer(buffer, 0, written);
                if (offerResult < 0L) {
                    if (System.nanoTime() > deadlineNanos) {
                        throw new TimeoutException(
                                "cluster offer back-pressure timeout for correlationId=" + cmd.correlationId());
                    }
                    parkOrThrow(config.getOfferBackpressureParkNanos());
                }
            } while (offerResult < 0L);

            while (true) {
                AdmissionResult result = received.remove(cmd.correlationId());
                if (result != null) {
                    return result;
                }
                active.pollEgress();
                result = received.remove(cmd.correlationId());
                if (result != null) {
                    return result;
                }
                if (System.nanoTime() > deadlineNanos) {
                    throw new TimeoutException(
                            "cluster egress wait timeout for correlationId=" + cmd.correlationId());
                }
                parkOrThrow(config.getEgressPollParkNanos());
            }
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
