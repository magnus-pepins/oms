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
import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Shared control-plane admission: risk, buying power, CAS to {@code WORKING} or reject, {@code control_decisions},
 * {@code domain_event_outbox} fanout rows.
 *
 * <p>The cluster substrate (Phase 2/3 of the Aeron Cluster substrate plan) calls this from
 * {@link com.balh.oms.projector.OmsPostgresProjector#applyAdmittedEvent} after the cluster admits an order; the
 * projector hands the {@link AdmissionResult} forward to whatever downstream side effect needs the post-admission
 * state (e.g. the simulated route enqueue). Outbound routing is enqueued by the projector itself (next to this call),
 * not here.
 */
@Component
public class OrderControlAdmission {

    /**
     * Outcome of {@link #persistAdmission(PendingControlEvent)}. Lives on this class (not on a tailer) because
     * Phase 3 slice 3g deleted {@code ControlTailer}; the projector path is the only caller.
     */
    public enum AdmissionResult {
        APPLIED,
        SKIPPED_VERSION_MISMATCH,
        STALE_REJECTED,
        UNKNOWN_ORDER,
        BUYING_POWER_REJECTED,
        LEDGER_SERVICE_REJECTED,
        RISK_PIPELINE_REJECTED
    }

    private static final Logger log = LoggerFactory.getLogger(OrderControlAdmission.class);

    private static final String METRIC_REJECT_EVENTS = "oms_order_rejected_events_published_total";
    private static final String TAG_REJECT_CODE = "reject_code";
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

    public OrderControlAdmission(
            OrdersRepository orders,
            StaleJobGuard stale,
            OmsConfig config,
            BuyingPowerAdmission buyingPower,
            ControlRiskEvaluator controlRisk,
            ControlDecisionsRepository controlDecisions,
            DomainEventOutboxRepository domainEventOutbox,
            DomainEventEnvelopeCodec envelopeCodec,
            MeterRegistry meterRegistry) {
        this.orders = orders;
        this.stale = stale;
        this.config = config;
        this.buyingPower = buyingPower;
        this.controlRisk = controlRisk;
        this.controlDecisions = controlDecisions;
        this.domainEventOutbox = domainEventOutbox;
        this.envelopeCodec = envelopeCodec;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Applies control-plane admission for a single {@code OrderAccepted} pending event (same semantics as the legacy
     * tail path, without route dispatch).
     */
    public AdmissionResult persistAdmission(PendingControlEvent event) {
        return persistAdmissionBody(event, null);
    }

    /**
     * Projector hot path: {@code admittedOrder} is the in-memory row just inserted by
     * {@link OrdersRepository#insertFromAdmittedEvent} in the same transaction, avoiding a
     * redundant {@link OrdersRepository#findById(java.util.UUID)} round-trip.
     */
    public AdmissionResult persistAdmission(PendingControlEvent event, Order admittedOrder) {
        return persistAdmissionBody(event, Objects.requireNonNull(admittedOrder, "admittedOrder"));
    }

    private AdmissionResult persistAdmissionBody(PendingControlEvent event, Order preloadedOrder) {
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
            return updated ? AdmissionResult.STALE_REJECTED : AdmissionResult.SKIPPED_VERSION_MISMATCH;
        }

        Order order = preloadedOrder != null
                ? preloadedOrder
                : orders.findById(event.orderId()).orElse(null);
        if (order == null) {
            log.warn("Control event references unknown orderId={}", event.orderId());
            return AdmissionResult.UNKNOWN_ORDER;
        }

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
            return updated ? AdmissionResult.RISK_PIPELINE_REJECTED : AdmissionResult.SKIPPED_VERSION_MISMATCH;
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
                    return updated ? AdmissionResult.BUYING_POWER_REJECTED : AdmissionResult.SKIPPED_VERSION_MISMATCH;
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
                    return updated ? AdmissionResult.LEDGER_SERVICE_REJECTED : AdmissionResult.SKIPPED_VERSION_MISMATCH;
                }
                case PROCEED -> { /* fall through */ }
            }
        }

        // Control-plane admission PASSED. The order is NOT promoted to WORKING here: WORKING means
        // "live at the venue" and is set only when the venue acknowledges the order
        // (EXEC_TYPE_VENUE_NEW → {@link com.balh.oms.projector.OmsPostgresProjector#applyVenueNewProjection},
        // or implicitly on first fill). Until then the order stays PENDING_NEW (admitted at OMS,
        // routed, awaiting venue). This is the same lifecycle for every routing backend — internal
        // venue gRPC and external FIX venues. We still record the control_decisions PASS audit row
        // so the risk decision is captured at admission time; the OrderWorking domain event now
        // fires from the venue-acceptance projection, not here.
        controlDecisions.record(
                event.orderId(),
                event.orderVersion(),
                "PASS",
                null,
                ControlRiskEvaluator.STAGE_CONTROL,
                null);
        return AdmissionResult.APPLIED;
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

}
