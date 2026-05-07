package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.persistence.FixRouteStateRepository;
import com.balh.oms.persistence.FixRouteStateRow;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.returnpath.ExecutionReportApplier;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import quickfix.Session;
import quickfix.SessionNotFound;
import quickfix.fix44.NewOrderSingle;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Drains {@link FixRouteDispatcher} after logon and sends {@link NewOrderSingle} on the active session.
 */
public class FixOutboundDispatchWorker {

    private static final Logger log = LoggerFactory.getLogger(FixOutboundDispatchWorker.class);

    private final FixRouteDispatcher fixRouteDispatcher;
    private final FixSessionRegistry fixSessionRegistry;
    private final OrdersRepository ordersRepository;
    private final FixNewOrderSingleBuilder newOrderSingleBuilder;
    private final MeterRegistry meterRegistry;
    private final OmsConfig omsConfig;
    private final ExecutionReportApplier executionReportApplier;
    private final FixRouteStateRepository fixRouteStateRepository;
    private final FixOutboundTokenBucket fixOutboundTokenBucket;

    public FixOutboundDispatchWorker(
            FixRouteDispatcher fixRouteDispatcher,
            FixSessionRegistry fixSessionRegistry,
            OrdersRepository ordersRepository,
            FixNewOrderSingleBuilder newOrderSingleBuilder,
            MeterRegistry meterRegistry,
            OmsConfig omsConfig,
            ExecutionReportApplier executionReportApplier,
            FixRouteStateRepository fixRouteStateRepository,
            FixOutboundTokenBucket fixOutboundTokenBucket) {
        this.fixRouteDispatcher = fixRouteDispatcher;
        this.fixSessionRegistry = fixSessionRegistry;
        this.ordersRepository = ordersRepository;
        this.newOrderSingleBuilder = newOrderSingleBuilder;
        this.meterRegistry = meterRegistry;
        this.omsConfig = omsConfig;
        this.executionReportApplier = executionReportApplier;
        this.fixRouteStateRepository = fixRouteStateRepository;
        this.fixOutboundTokenBucket = fixOutboundTokenBucket;
    }

    @Scheduled(fixedDelayString = "${oms.fix.outbound-poll-interval-ms:100}")
    public void drainPendingOutbound() {
        if (!fixSessionRegistry.hasLoggedOnSession()) {
            return;
        }
        if (!isFixRouteSendEnabled()) {
            meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_ROUTE_DISABLED_SKIPS).increment();
            return;
        }
        UUID id = fixRouteDispatcher.pollPendingOrNull();
        if (id == null) {
            return;
        }
        Order order = ordersRepository.findById(id).orElse(null);
        if (order == null || order.status() != OrderStatus.WORKING) {
            log.debug("Skipping FIX outbound for orderId={} (missing or not WORKING)", id);
            return;
        }
        long maxAgeMs = omsConfig.getFix().getMaxOutboundJobAgeMs();
        if (maxAgeMs > 0 && order.acceptedAt() != null) {
            long ageMs = Duration.between(order.acceptedAt(), Instant.now()).toMillis();
            if (ageMs > maxAgeMs) {
                meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_JOB_EXPIRED).increment();
                executionReportApplier.applyOutboundJobExpired(id);
                return;
            }
        }
        if (!fixOutboundTokenBucket.tryAcquire()) {
            meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_THROTTLED_REQUEUES).increment();
            fixRouteDispatcher.enqueueWorkingOrder(id);
            return;
        }
        try {
            NewOrderSingle nos = newOrderSingleBuilder.build(order);
            Session.sendToTarget(nos, fixSessionRegistry.sessionOrNull());
            meterRegistry.counter(FixMetrics.METRIC_NOS_SENT).increment();
        } catch (SessionNotFound e) {
            log.warn("FIX sendToTarget failed (no session) orderId={}", id, e);
        } catch (Exception e) {
            log.error("FIX outbound send failed orderId={}", id, e);
        }
    }

    private boolean isFixRouteSendEnabled() {
        String routeKey = omsConfig.getFix().getRouteKey();
        return fixRouteStateRepository
                .findByRouteKey(routeKey)
                .map(FixRouteStateRow::sendEnabled)
                .orElse(true);
    }
}
