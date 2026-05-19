package com.balh.oms.events;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.RejectCode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;

/**
 * Canonical JSON envelope for the transactional domain fanout outbox.
 *
 * <p>Wire shape: {@code schemaVersion}, {@code type}, {@code occurredAt},
 * {@code correlationId} (order id), {@code payload} (event-specific object).
 */
@Component
public class DomainEventEnvelopeCodec {

    private static final int ENVELOPE_SCHEMA_VERSION = 1;

    private final ObjectMapper objectMapper;

    public DomainEventEnvelopeCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String orderAccepted(Order order) throws JsonProcessingException {
        var payload = new OrderAcceptedEvent(
                order.id(),
                order.version(),
                order.shardId(),
                order.accountIdHash(),
                order.side().name(),
                order.instrumentSymbol(),
                order.quantity(),
                order.limitPrice(),
                order.timeInForce(),
                order.acceptedAt());
        return envelope("OrderAccepted", order.id(), payload);
    }

    /**
     * Phase 4 Tier 2.5 phase D-3: build an {@code OrderAccepted} envelope from the cluster's
     * {@link OrderAdmittedEvent} so the projector can emit the envelope after the cluster log
     * is durable, instead of the ingress JVM emitting it inside its own outbox tx. Same shape
     * as {@link #orderAccepted(Order)} (payload type, schema version, type tag) so downstream
     * NATS / desk / drop-copy consumers cannot tell which writer produced the row.
     *
     * <p>Field mapping (mirrors {@link com.balh.oms.persistence.OrdersRepository#insertFromAdmittedEvent}
     * so the {@code orders} row and the {@code OrderAccepted} envelope agree byte-for-byte on
     * side / TIF / qty / limit / acceptedAt):
     * <ul>
     *   <li>{@code side} byte → {@link AcceptOrderCommand#sideName(byte)}</li>
     *   <li>{@code timeInForceCode} byte → {@link AcceptOrderCommand#timeInForceName(byte)}</li>
     *   <li>{@code quantityScaled} (1e9 fixed-point) → {@link BigDecimal} with scale 10,
     *       {@link RoundingMode#UNNECESSARY} (cluster guarantees no precision loss)</li>
     *   <li>{@code limitPriceScaledOrZero} (1e6 fixed-point; 0 = market) → {@code null} for
     *       market orders, otherwise scale 10 {@link BigDecimal}</li>
     *   <li>{@code acceptedAtMillis} → {@link Instant#ofEpochMilli(long)}</li>
     * </ul>
     */
    public String orderAcceptedFromAdmitted(OrderAdmittedEvent ev) throws JsonProcessingException {
        BigDecimal quantity = BigDecimal.valueOf(ev.quantityScaled())
                .divide(BigDecimal.valueOf(AcceptOrderCommand.QUANTITY_SCALE), 10, RoundingMode.UNNECESSARY);
        BigDecimal limitPrice = ev.limitPriceScaledOrZero() == 0L
                ? null
                : BigDecimal.valueOf(ev.limitPriceScaledOrZero())
                        .divide(BigDecimal.valueOf(AcceptOrderCommand.PRICE_SCALE), 10, RoundingMode.UNNECESSARY);
        var payload = new OrderAcceptedEvent(
                ev.orderId(),
                ev.version(),
                ev.shardId(),
                ev.accountIdHash(),
                AcceptOrderCommand.sideName(ev.side()),
                ev.instrumentSymbol(),
                quantity,
                limitPrice,
                AcceptOrderCommand.timeInForceName(ev.timeInForceCode()),
                Instant.ofEpochMilli(ev.acceptedAtMillis()));
        return envelope("OrderAccepted", ev.orderId(), payload);
    }

    public String orderRejected(PendingControlEvent event, RejectCode reason, int newSeq) throws JsonProcessingException {
        var payload = new OrderRejectedEvent(
                event.orderId(),
                newSeq,
                event.shardId(),
                event.accountIdHash(),
                reason.name(),
                Instant.now());
        return envelope("OrderRejected", event.orderId(), payload);
    }

    public String orderWorking(PendingControlEvent event, Order order, int newSeq) throws JsonProcessingException {
        var payload = new OrderWorkingEvent(
                order.id(),
                newSeq,
                event.shardId(),
                event.accountIdHash(),
                order.side().name(),
                order.instrumentSymbol(),
                order.quantity(),
                order.limitPrice(),
                order.timeInForce(),
                Instant.now());
        return envelope("OrderWorking", order.id(), payload);
    }

    public String orderPartiallyFilled(Order order, int newSeq, BigDecimal cumQty, BigDecimal lastQty,
            BigDecimal lastPx, String venueId, String venueExecRef) throws JsonProcessingException {
        var payload = new OrderPartiallyFilledEvent(
                order.id(),
                newSeq,
                order.shardId(),
                order.accountIdHash(),
                cumQty,
                lastQty,
                lastPx,
                venueId,
                venueExecRef,
                Instant.now());
        return envelope("OrderPartiallyFilled", order.id(), payload);
    }

    public String orderFilled(Order order, int newSeq, BigDecimal filledQty, BigDecimal averageFillPrice,
            String venueId, String venueExecRef) throws JsonProcessingException {
        var payload = new OrderFilledEvent(
                order.id(),
                newSeq,
                order.shardId(),
                order.accountIdHash(),
                filledQty,
                averageFillPrice,
                venueId,
                venueExecRef,
                Instant.now());
        return envelope("OrderFilled", order.id(), payload);
    }

    public String orderCancelled(Order order, int newSeq, String venueId, String venueExecRef)
            throws JsonProcessingException {
        var payload = new OrderCancelledEvent(
                order.id(), newSeq, order.shardId(), order.accountIdHash(), venueId, venueExecRef, Instant.now());
        return envelope("OrderCancelled", order.id(), payload);
    }

    /**
     * {@code OrderRejected} after venue/broker reject CAS (same payload shape as control reject).
     */
    public String orderRejectedAfterVenue(Order order, RejectCode reason, int newSeq) throws JsonProcessingException {
        var payload = new OrderRejectedEvent(
                order.id(),
                newSeq,
                order.shardId(),
                order.accountIdHash(),
                reason.name(),
                Instant.now());
        return envelope("OrderRejected", order.id(), payload);
    }

    /**
     * Wed-demo addition. Emitted by the projector after a venue REPLACE ACK (ER 35=5) has been
     * applied to {@code orders.quantity} / {@code orders.limit_price} (CAS via
     * {@code OrdersRepository.updateReplaceWithCas}). The BFF consumer mirrors {@code newQuantity}
     * and {@code newLimitPrice} onto {@code customer_orders.qty} / {@code customer_orders.limit_price}
     * so the customer blotter shows the post-replace values without re-reading OMS.
     *
     * <p>{@code refreshed} is the post-CAS {@code orders} row (already carries the new qty/price).
     * We pass {@code newQuantity} / {@code newLimitPrice} explicitly anyway so the envelope is
     * self-describing — a downstream consumer that doesn't trust the {@code orders} row read
     * (or that sees the envelope before its own read replica catches up) has enough on the wire.
     */
    public String orderReplaced(
            Order refreshed,
            int newSeq,
            BigDecimal newQuantity,
            BigDecimal newLimitPrice,
            String venueId,
            String venueExecRef) throws JsonProcessingException {
        var payload = new OrderReplacedEvent(
                refreshed.id(),
                newSeq,
                refreshed.shardId(),
                refreshed.accountIdHash(),
                newQuantity,
                newLimitPrice,
                refreshed.status().name(),
                venueId,
                venueExecRef,
                Instant.now());
        return envelope("OrderReplaced", refreshed.id(), payload);
    }

    /**
     * Emitted by the projector after a venue 35=9 OrderCancelReject (against a 35=F cancel) has
     * been recorded in {@code executions} ({@code exec_type=CANCEL_REJECT}). The order is
     * unchanged on the venue and in OMS; the envelope is purely informational so downstream BFF
     * surfaces a toast ("cancel rejected by broker") without polling.
     *
     * <p>{@code currentSeq} is the unchanged {@code orders.version}, not {@code version + 1} — the
     * reject did not mutate the row. See {@link OrderCancelRejectedEvent} doc for the sequencing
     * implication for downstream consumers.
     */
    public String orderCancelRejected(
            Order order,
            int currentSeq,
            String venueId,
            String venueExecRef) throws JsonProcessingException {
        var payload = new OrderCancelRejectedEvent(
                order.id(),
                currentSeq,
                order.shardId(),
                order.accountIdHash(),
                order.status().name(),
                venueId,
                venueExecRef,
                Instant.now());
        return envelope("OrderCancelRejected", order.id(), payload);
    }

    /**
     * Mirror of {@link #orderCancelRejected} for a 35=9 against a prior 35=G replace. Same
     * payload shape, distinct {@code type} tag so the BFF can route the toast text correctly
     * ("modify rejected" vs. "cancel rejected").
     */
    public String orderReplaceRejected(
            Order order,
            int currentSeq,
            String venueId,
            String venueExecRef) throws JsonProcessingException {
        var payload = new OrderReplaceRejectedEvent(
                order.id(),
                currentSeq,
                order.shardId(),
                order.accountIdHash(),
                order.status().name(),
                venueId,
                venueExecRef,
                Instant.now());
        return envelope("OrderReplaceRejected", order.id(), payload);
    }

    private String envelope(String type, UUID correlationOrderId, Object payload) throws JsonProcessingException {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("schemaVersion", ENVELOPE_SCHEMA_VERSION);
        root.put("type", type);
        root.put("occurredAt", Instant.now().toString());
        root.put("correlationId", correlationOrderId.toString());
        root.set("payload", objectMapper.valueToTree(payload));
        return objectMapper.writeValueAsString(root);
    }
}
