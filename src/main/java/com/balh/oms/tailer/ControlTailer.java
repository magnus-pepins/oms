package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.persistence.ControlDecisionsRepository;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.risk.BuyingPowerAdmission;
import com.balh.oms.risk.ControlRiskEvaluator;
import com.balh.oms.routing.RouteDispatcher;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.UUID;

/**
 * Applies a control-plane event to Postgres after it has been read off the
 * Chronicle journal (slice 1) or — earlier in the lifecycle — directly from a
 * Disruptor handler. Both paths converge here.
 *
 * <p>Idempotent: every update goes through CAS on
 * {@code orders.version}. Re-applying the same event is a no-op.
 */
@Component
public class ControlTailer {

    private static final Logger log = LoggerFactory.getLogger(ControlTailer.class);

    private static final String METRIC_REJECT_EVENTS = "oms_order_rejected_events_published_total";
    private static final String TAG_REJECT_CODE = "reject_code";
    private static final String METRIC_WORKING_EVENTS = "oms_order_working_events_published_total";
    private static final String METRIC_STALE_REJECTS = "oms_control_jobs_rejected_stale_total";

    private final OrdersRepository orders;
    private final StaleJobGuard stale;
    private final OmsConfig config;
    private final BuyingPowerAdmission buyingPower;
    private final ControlRiskEvaluator controlRisk;
    private final ControlDecisionsRepository controlDecisions;
    private final DomainEventOutboxRepository domainEventOutbox;
    private final DomainEventEnvelopeCodec envelopeCodec;
    private final MeterRegistry meterRegistry;
    private final RouteDispatcher routeDispatcher;

    public ControlTailer(
            OrdersRepository orders,
            StaleJobGuard stale,
            OmsConfig config,
            BuyingPowerAdmission buyingPower,
            ControlRiskEvaluator controlRisk,
            ControlDecisionsRepository controlDecisions,
            DomainEventOutboxRepository domainEventOutbox,
            DomainEventEnvelopeCodec envelopeCodec,
            MeterRegistry meterRegistry,
            RouteDispatcher routeDispatcher) {
        this.orders = orders;
        this.stale = stale;
        this.config = config;
        this.buyingPower = buyingPower;
        this.controlRisk = controlRisk;
        this.controlDecisions = controlDecisions;
        this.domainEventOutbox = domainEventOutbox;
        this.envelopeCodec = envelopeCodec;
        this.meterRegistry = meterRegistry;
        this.routeDispatcher = routeDispatcher;
    }

    @Transactional
    public TailResult apply(PendingControlEvent event) {
        if (stale.isStale(event.orderTimestamp())) {
            boolean updated = orders.updateWithCas(
                    event.orderId(),
                    event.orderVersion(),
                    OrderStatus.REJECTED,
                    RejectCode.RISK_STALE_QUEUE,
                    null,
                    Instant.now()
            );
            if (updated) {
                meterRegistry.counter(METRIC_STALE_REJECTS).increment();
                controlDecisions.record(
                        event.orderId(),
                        event.orderVersion(),
                        "REJECT",
                        RejectCode.RISK_STALE_QUEUE,
                        ControlRiskEvaluator.STAGE_CONTROL,
                        null);
                publishRejected(event, RejectCode.RISK_STALE_QUEUE);
            }
            return updated ? TailResult.STALE_REJECTED : TailResult.SKIPPED_VERSION_MISMATCH;
        }

        var row = orders.findById(event.orderId());
        if (row.isEmpty()) {
            log.warn("Control event references unknown orderId={}", event.orderId());
            return TailResult.UNKNOWN_ORDER;
        }
        Order order = row.get();

        var riskReject = controlRisk.evaluate(order);
        if (riskReject.isPresent()) {
            RejectCode code = riskReject.get();
            boolean updated = orders.updateWithCas(
                    event.orderId(),
                    event.orderVersion(),
                    OrderStatus.REJECTED,
                    code,
                    null,
                    Instant.now()
            );
            if (updated) {
                controlDecisions.record(
                        event.orderId(),
                        event.orderVersion(),
                        "REJECT",
                        code,
                        ControlRiskEvaluator.STAGE_CONTROL,
                        null);
                publishRejected(event, code);
            }
            return updated ? TailResult.RISK_PIPELINE_REJECTED : TailResult.SKIPPED_VERSION_MISMATCH;
        }

        if (config.getLedger().isEnabled()) {
            switch (buyingPower.evaluate(order)) {
                case REJECT_INSUFFICIENT -> {
                    boolean updated = orders.updateWithCas(
                            event.orderId(),
                            event.orderVersion(),
                            OrderStatus.REJECTED,
                            RejectCode.RISK_BUYING_POWER,
                            null,
                            Instant.now()
                    );
                    if (updated) {
                        controlDecisions.record(
                                event.orderId(),
                                event.orderVersion(),
                                "REJECT",
                                RejectCode.RISK_BUYING_POWER,
                                ControlRiskEvaluator.STAGE_CONTROL,
                                null);
                        publishRejected(event, RejectCode.RISK_BUYING_POWER);
                    }
                    return updated ? TailResult.BUYING_POWER_REJECTED : TailResult.SKIPPED_VERSION_MISMATCH;
                }
                case REJECT_LEDGER_UNAVAILABLE -> {
                    boolean updated = orders.updateWithCas(
                            event.orderId(),
                            event.orderVersion(),
                            OrderStatus.REJECTED,
                            RejectCode.INTERNAL_ERROR,
                            null,
                            Instant.now()
                    );
                    if (updated) {
                        controlDecisions.record(
                                event.orderId(),
                                event.orderVersion(),
                                "REJECT",
                                RejectCode.INTERNAL_ERROR,
                                ControlRiskEvaluator.STAGE_CONTROL,
                                null);
                        publishRejected(event, RejectCode.INTERNAL_ERROR);
                    }
                    return updated ? TailResult.LEDGER_SERVICE_REJECTED : TailResult.SKIPPED_VERSION_MISMATCH;
                }
                case PROCEED -> { /* fall through */ }
            }
        }

        boolean updated = orders.updateWithCas(
                event.orderId(),
                event.orderVersion(),
                OrderStatus.WORKING,
                null,
                event.orderTimestamp(),
                null
        );
        if (!updated) {
            log.debug("CAS skipped for orderId={} expectedVersion={} (someone else won the race)",
                    event.orderId(), event.orderVersion());
            return TailResult.SKIPPED_VERSION_MISMATCH;
        }
        int newSeq = event.orderVersion() + 1;
        controlDecisions.record(
                event.orderId(),
                event.orderVersion(),
                "PASS",
                null,
                ControlRiskEvaluator.STAGE_CONTROL,
                null);
        orders.findById(event.orderId()).ifPresentOrElse(
                o -> publishWorking(event, o, newSeq),
                () -> log.warn("WORKING CAS succeeded but order {} not found for event publish", event.orderId()));
        registerRouteDispatch(event.orderId());
        return TailResult.APPLIED;
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

    private void publishRejected(PendingControlEvent event, RejectCode reason) {
        int newSeq = event.orderVersion() + 1;
        try {
            domainEventOutbox.insert(event.orderId(), envelopeCodec.orderRejected(event, reason, newSeq));
            meterRegistry.counter(METRIC_REJECT_EVENTS, TAG_REJECT_CODE, reason.name()).increment();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("domain event envelope serialisation failed", e);
        }
    }

    private void publishWorking(PendingControlEvent event, Order order, int newSeq) {
        try {
            domainEventOutbox.insert(order.id(), envelopeCodec.orderWorking(event, order, newSeq));
            meterRegistry.counter(METRIC_WORKING_EVENTS).increment();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("domain event envelope serialisation failed", e);
        }
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
