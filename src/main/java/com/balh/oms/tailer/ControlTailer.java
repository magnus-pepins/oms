package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.ControlPostgresWritePath;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.observability.metrics.OmsPipelineMeterNames;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.balh.oms.persistence.FixNosRouteEnqueueClaimRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.routing.RouteDispatcher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

/**
 * Applies a control-plane event to Postgres after it has been read off the
 * Chronicle journal (slice 1) or — earlier in the lifecycle — directly from a
 * Disruptor handler. Both paths converge here.
 *
 * <p>When {@code oms.control.postgres-write-path=ingress}, Postgres mutations for {@code OrderAccepted} run in
 * {@link com.balh.oms.ingress.OrderIngressService}; this component only enqueues outbound routing after Chronicle
 * delivery when the order is already {@code WORKING} at the expected version.
 *
 * <p>Idempotent: every update goes through CAS on
 * {@code orders.version}. Re-applying the same event is a no-op.
 */
@Component
public class ControlTailer {

    private static final Logger log = LoggerFactory.getLogger(ControlTailer.class);

    private final OrderControlAdmission orderControlAdmission;
    private final OrdersRepository orders;
    private final StaleJobGuard stale;
    private final OmsConfig config;
    private final MeterRegistry meterRegistry;
    private final RouteDispatcher routeDispatcher;
    private final FixNosRouteEnqueueClaimRepository fixNosRouteEnqueueClaimRepository;

    public ControlTailer(
            OrderControlAdmission orderControlAdmission,
            OrdersRepository orders,
            StaleJobGuard stale,
            OmsConfig config,
            MeterRegistry meterRegistry,
            RouteDispatcher routeDispatcher,
            FixNosRouteEnqueueClaimRepository fixNosRouteEnqueueClaimRepository) {
        this.orderControlAdmission = orderControlAdmission;
        this.orders = orders;
        this.stale = stale;
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.routeDispatcher = routeDispatcher;
        this.fixNosRouteEnqueueClaimRepository = fixNosRouteEnqueueClaimRepository;
    }

    @Transactional
    public TailResult apply(PendingControlEvent event) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            TailResult result = applyBody(event);
            OmsPipelineMetrics.finishControlApply(meterRegistry, sample, result.name());
            return result;
        } catch (RuntimeException e) {
            OmsPipelineMetrics.finishControlApply(meterRegistry, sample, "exception");
            throw e;
        }
    }

    private TailResult applyBody(PendingControlEvent event) {
        if (config.getControl().getPostgresWritePath() == ControlPostgresWritePath.INGRESS) {
            return applyIngressTailDispatchOnly(event);
        }
        TailResult result = orderControlAdmission.persistAdmission(event);
        if (result == TailResult.APPLIED) {
            registerRouteDispatch(event.orderId());
        }
        return result;
    }

    /**
     * Chronicle tail path when ingress already performed CAS, {@code control_decisions}, and domain fanout rows.
     * Enqueues routing only when the order is {@code WORKING} at {@code event.orderVersion() + 1}.
     */
    private TailResult applyIngressTailDispatchOnly(PendingControlEvent event) {
        if (stale.isStale(event.orderTimestamp())) {
            log.debug(
                    "postgres-write-path=ingress: stale Chronicle event at tail orderId={} — skipping dispatch (no tail Postgres writes)",
                    event.orderId());
            return TailResult.SKIPPED_VERSION_MISMATCH;
        }
        var row = orders.findById(event.orderId());
        if (row.isEmpty()) {
            log.warn("Control event references unknown orderId={}", event.orderId());
            return TailResult.UNKNOWN_ORDER;
        }
        Order order = row.get();
        if (order.status() == OrderStatus.WORKING && order.version() == event.orderVersion() + 1) {
            if (shouldDedupOutboundEnqueueWithClaim()) {
                if (!fixNosRouteEnqueueClaimRepository.tryClaim(event.orderId())) {
                    meterRegistry.counter(OmsPipelineMeterNames.CONTROL_INGRESS_DISPATCH_ENQUEUE_CLAIM_SKIP).increment();
                    return TailResult.SKIPPED_VERSION_MISMATCH;
                }
            }
            registerRouteDispatch(event.orderId());
            return TailResult.APPLIED;
        }
        if (order.status() == OrderStatus.REJECTED) {
            return TailResult.SKIPPED_VERSION_MISMATCH;
        }
        log.warn(
                "postgres-write-path=ingress: unexpected order state at tail orderId={} status={} version={} eventVersion={}",
                event.orderId(),
                order.status(),
                order.version(),
                event.orderVersion());
        return TailResult.SKIPPED_VERSION_MISMATCH;
    }

    /**
     * When {@code noop} routing there is no outbound queue to pollute; skip claim inserts.
     */
    private boolean shouldDedupOutboundEnqueueWithClaim() {
        String b = config.getRouting().getBackend();
        return "fix".equalsIgnoreCase(b) || "simulated".equalsIgnoreCase(b);
    }

    private void registerRouteDispatch(UUID orderId) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            routeDispatcher.enqueueWorkingOrder(orderId);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                routeDispatcher.enqueueWorkingOrder(orderId);
            }
        });
    }

    public enum TailResult {
        APPLIED,
        SKIPPED_VERSION_MISMATCH,
        STALE_REJECTED,
        UNKNOWN_ORDER,
        BUYING_POWER_REJECTED,
        LEDGER_SERVICE_REJECTED,
        RISK_PIPELINE_REJECTED
    }
}
