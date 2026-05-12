package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.observability.otel.IngressToFixNosLatencyLimits;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.persistence.ControlDecisionsRepository;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.risk.BuyingPowerAdmission;
import com.balh.oms.risk.ControlRiskEvaluator;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Shared control-plane admission: risk, buying power, CAS to {@code WORKING} or reject, {@code control_decisions},
 * {@code domain_event_outbox} fanout rows.
 *
 * <p>Used from {@link com.balh.oms.ingress.OrderIngressService} when {@code oms.control.postgres-write-path=ingress},
 * and from {@link ControlTailer} when {@code oms.control.postgres-write-path=tail}. Does not enqueue outbound routing;
 * {@link ControlTailer} does that after a successful admit on the tail path, or Chronicle consumption does on the
 * ingress path.
 */
@Component
public class OrderControlAdmission {

    private static final Logger log = LoggerFactory.getLogger(OrderControlAdmission.class);

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
    private final IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder;

    public OrderControlAdmission(
            OrdersRepository orders,
            StaleJobGuard stale,
            OmsConfig config,
            BuyingPowerAdmission buyingPower,
            ControlRiskEvaluator controlRisk,
            ControlDecisionsRepository controlDecisions,
            DomainEventOutboxRepository domainEventOutbox,
            DomainEventEnvelopeCodec envelopeCodec,
            MeterRegistry meterRegistry,
            IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder) {
        this.orders = orders;
        this.stale = stale;
        this.config = config;
        this.buyingPower = buyingPower;
        this.controlRisk = controlRisk;
        this.controlDecisions = controlDecisions;
        this.domainEventOutbox = domainEventOutbox;
        this.envelopeCodec = envelopeCodec;
        this.meterRegistry = meterRegistry;
        this.ingressToFixNosLatencyRecorder = ingressToFixNosLatencyRecorder;
    }

    /**
     * Applies control-plane admission for a single {@code OrderAccepted} pending event (same semantics as the legacy
     * tail path, without route dispatch).
     */
    public ControlTailer.TailResult persistAdmission(PendingControlEvent event) {
        return persistAdmissionBody(event);
    }

    private ControlTailer.TailResult persistAdmissionBody(PendingControlEvent event) {
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
            return updated ? ControlTailer.TailResult.STALE_REJECTED : ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH;
        }

        var row = orders.findById(event.orderId());
        if (row.isEmpty()) {
            log.warn("Control event references unknown orderId={}", event.orderId());
            return ControlTailer.TailResult.UNKNOWN_ORDER;
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
            return updated ? ControlTailer.TailResult.RISK_PIPELINE_REJECTED : ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH;
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
                    return updated ? ControlTailer.TailResult.BUYING_POWER_REJECTED : ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH;
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
                    return updated ? ControlTailer.TailResult.LEDGER_SERVICE_REJECTED : ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH;
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
            return ControlTailer.TailResult.SKIPPED_VERSION_MISMATCH;
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
        return ControlTailer.TailResult.APPLIED;
    }

    private void publishRejected(PendingControlEvent event, RejectCode reason) {
        ingressToFixNosLatencyRecorder.discard(event.orderId(), IngressToFixNosLatencyLimits.REASON_CONTROL_REJECT);
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
}
