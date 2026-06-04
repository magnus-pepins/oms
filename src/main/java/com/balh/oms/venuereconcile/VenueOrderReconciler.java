package com.balh.oms.venuereconcile;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.persistence.OrdersRepository.VenueReconcileCandidate;
import com.balh.oms.routing.VenueRoutingSymbols;
import com.balh.oms.venue.VenueRouteOrderClient;
import com.balh.oms.venue.VenueRouteTransportException;
import com.balh.venue.grpc.v1.OrderLiveness;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Golden-copy order-state reconciliation against the in-house venue.
 *
 * <p>Standard market-recovery discipline: the exchange is the authoritative record of resting-order
 * state, and downstream systems reconcile <em>down</em> from it on startup and periodically rather
 * than trusting a cached view. OMS believes an order is {@code WORKING} until something tells it
 * otherwise; the only paths that retract WORKING today are a fill or a live venue reject. When the
 * venue's resting set changes without emitting an event OMS consumes — most importantly a matching
 * rule change applied during cluster replay, which can retroactively reject an order that previously
 * rested — OMS is left with a phantom WORKING order ("Accepted" in the UI, absent from the book).
 *
 * <p>This reconciler closes that gap: it asks the venue, per order, "is this still live?" and
 * terminates the ones the venue has dropped via the same {@code EXEC_TYPE_VENUE_REJECT} path a live
 * route-failure uses, so the projector, drop-copy feed, and BFF/UI all cascade to the corrected
 * state. It runs on the {@code oms-venue-egress} JVM, which already holds the venue gRPC stub and the
 * cluster ingress client.
 *
 * <h2>Why it cannot wrongly reject a live order</h2>
 *
 * <ul>
 *   <li><b>Age gate.</b> Only orders WORKING longer than
 *       {@link OmsConfig.Cluster.VenueReconciler#getMinOrderAgeMs()} are considered. A venue fill
 *       reaches OMS via the resolver tail within its lag budget; an order still WORKING past that
 *       budget has no in-flight fill, so a NOT_LIVE answer is a genuine orphan, not a fill race.</li>
 *   <li><b>Authoritative only.</b> A transition happens <em>only</em> on a definitive
 *       {@link OrderLiveness#ORDER_LIVENESS_NOT_LIVE}. Transport errors or
 *       {@code ORDER_LIVENESS_UNSPECIFIED} are treated as inconclusive and skipped.</li>
 *   <li><b>Idempotent + guarded.</b> The reject carries a deterministic {@code venueExecRef}; the
 *       cluster dedupes on {@code (orderId, venueExecRef)} and no-ops on already-terminal orders, so
 *       a late fill that beats the reject still wins and re-runs are harmless.</li>
 * </ul>
 */
public final class VenueOrderReconciler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(VenueOrderReconciler.class);

    public static final String METRIC_CHECKED = "oms.venue.reconcile.checked_total";
    public static final String METRIC_ORPHANS_TERMINATED = "oms.venue.reconcile.orphans_terminated_total";
    public static final String METRIC_INCONCLUSIVE = "oms.venue.reconcile.inconclusive_total";

    static final String REASON_NOT_LIVE = "venue_not_live_on_reconcile";

    private final OmsConfig config;
    private final OrdersRepository ordersRepository;
    private final VenueRouteOrderClient venueClient;
    private final OmsClusterIngressClient clusterIngressClient;
    private final Clock clock;
    private final Counter checkedCounter;
    private final Counter orphansTerminatedCounter;
    private final Counter inconclusiveCounter;
    private final ScheduledExecutorService poller;

    public VenueOrderReconciler(
            OmsConfig config,
            OrdersRepository ordersRepository,
            VenueRouteOrderClient venueClient,
            OmsClusterIngressClient clusterIngressClient,
            Clock clock,
            MeterRegistry meterRegistryOrNull) {
        this.config = config;
        this.ordersRepository = ordersRepository;
        this.venueClient = venueClient;
        this.clusterIngressClient = clusterIngressClient;
        this.clock = clock;
        this.checkedCounter = counterOrNull(meterRegistryOrNull, METRIC_CHECKED);
        this.orphansTerminatedCounter = counterOrNull(meterRegistryOrNull, METRIC_ORPHANS_TERMINATED);
        this.inconclusiveCounter = counterOrNull(meterRegistryOrNull, METRIC_INCONCLUSIVE);
        this.poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oms-venue-reconciler");
            t.setDaemon(true);
            return t;
        });
    }

    private static Counter counterOrNull(MeterRegistry registry, String name) {
        return registry == null ? null : Counter.builder(name).register(registry);
    }

    public void start() {
        OmsConfig.Cluster.VenueReconciler cfg = config.getCluster().getVenueReconciler();
        poller.scheduleWithFixedDelay(
                this::reconcileSafely, cfg.getInitialDelayMs(), cfg.getPollIntervalMs(), TimeUnit.MILLISECONDS);
        log.info(
                "oms-venue-reconciler started: initialDelayMs={} pollIntervalMs={} minOrderAgeMs={} maxOrdersPerPass={}",
                cfg.getInitialDelayMs(),
                cfg.getPollIntervalMs(),
                cfg.getMinOrderAgeMs(),
                cfg.getMaxOrdersPerPass());
    }

    private void reconcileSafely() {
        try {
            reconcileOnce();
        } catch (RuntimeException e) {
            log.warn("oms-venue-reconciler pass threw: {}", e.getMessage(), e);
        }
    }

    /**
     * One reconciliation pass. Visible for tests. Returns the number of orphaned orders terminated.
     */
    public int reconcileOnce() {
        OmsConfig.Cluster.VenueReconciler cfg = config.getCluster().getVenueReconciler();
        Instant cutoff = clock.instant().minusMillis(cfg.getMinOrderAgeMs());
        List<VenueReconcileCandidate> candidates =
                ordersRepository.findWorkingVenueReconcileCandidates(cutoff, cfg.getMaxOrdersPerPass());
        int terminated = 0;
        for (VenueReconcileCandidate candidate : candidates) {
            if (!VenueRoutingSymbols.routesToInternalVenue(config, candidate.instrumentSymbol())) {
                continue;
            }
            if (checkedCounter != null) {
                checkedCounter.increment();
            }
            OrderLiveness liveness;
            try {
                liveness = venueClient.queryOrderStatus(candidate.id(), candidate.instrumentSymbol());
            } catch (VenueRouteTransportException e) {
                if (inconclusiveCounter != null) {
                    inconclusiveCounter.increment();
                }
                log.debug("oms-venue-reconciler query failed (skipping) orderId={}", candidate.id(), e);
                continue;
            }
            if (liveness != OrderLiveness.ORDER_LIVENESS_NOT_LIVE) {
                if (liveness != OrderLiveness.ORDER_LIVENESS_LIVE && inconclusiveCounter != null) {
                    inconclusiveCounter.increment();
                }
                continue;
            }
            if (terminateOrphan(candidate)) {
                terminated++;
            }
        }
        if (terminated > 0) {
            log.info("oms-venue-reconciler terminated {} phantom WORKING order(s) the venue had dropped", terminated);
        }
        return terminated;
    }

    private boolean terminateOrphan(VenueReconcileCandidate candidate) {
        long venueTsNanos = TimeUnit.MILLISECONDS.toNanos(clock.millis());
        ApplyExecutionReportCommand cmd =
                new ApplyExecutionReportCommand(
                        0L,
                        candidate.id(),
                        0L,
                        0L,
                        venueTsNanos,
                        0,
                        ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT,
                        (byte) RejectCode.VENUE_REJECT.ordinal(),
                        config.getVenue().getVenueId(),
                        "venue-reconcile-" + candidate.id() + "-v" + candidate.version(),
                        "",
                        "{\"reason\":\"" + REASON_NOT_LIVE + "\"}");
        Duration timeout = Duration.ofMillis(config.getCluster().getVenueReconciler().getQueryTimeoutMs());
        try {
            clusterIngressClient.submitApplyExecutionReport(cmd, timeout);
            if (orphansTerminatedCounter != null) {
                orphansTerminatedCounter.increment();
            }
            log.info(
                    "oms-venue-reconciler: venue reports orderId={} symbol={} NOT_LIVE while OMS shows WORKING; "
                            + "submitting VENUE_REJECT (reason={})",
                    candidate.id(),
                    candidate.instrumentSymbol(),
                    REASON_NOT_LIVE);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            log.warn("oms-venue-reconciler: VENUE_REJECT submit failed for orderId={}", candidate.id(), e);
            return false;
        }
    }

    @Override
    public void close() {
        poller.shutdownNow();
        try {
            poller.awaitTermination(2L, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
