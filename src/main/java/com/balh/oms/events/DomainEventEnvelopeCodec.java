package com.balh.oms.events;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.RejectCode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
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
