package com.balh.oms.fix;

import com.balh.oms.returnpath.ExecutionCancelCommand;
import com.balh.oms.returnpath.ExecutionTradeCommand;
import com.balh.oms.returnpath.ExecutionVenueRejectCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.CxlRejReason;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrdStatus;
import quickfix.field.OrderID;
import quickfix.field.OrigClOrdID;
import quickfix.field.Text;
import quickfix.field.TransactTime;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.UUID;

/**
 * Maps FIX 4.4 {@code ExecutionReport} and {@code OrderCancelReject} fields to return-path commands (slice 4).
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixExecutionReportMapper {

    private final ObjectMapper objectMapper;

    public FixExecutionReportMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Optional<ExecutionTradeCommand> tryParseTrade(Message msg, String venueId) throws FieldNotFound {
        char execType = msg.getChar(ExecType.FIELD);
        if (execType != ExecType.PARTIAL_FILL && execType != ExecType.FILL) {
            return Optional.empty();
        }
        UUID orderId = parseClOrdId(msg);
        if (orderId == null) {
            return Optional.empty();
        }
        Instant venueTs = parseTransactTime(msg).orElse(Instant.now());
        String venueExecRef = msg.getString(ExecID.FIELD);
        BigDecimal lastQty = new BigDecimal(msg.getString(LastQty.FIELD));
        BigDecimal lastPx = msg.isSetField(LastPx.FIELD)
                ? new BigDecimal(msg.getString(LastPx.FIELD))
                : BigDecimal.ZERO;
        BigDecimal leaves = new BigDecimal(msg.getString(LeavesQty.FIELD));
        BigDecimal cum = new BigDecimal(msg.getString(CumQty.FIELD));
        return Optional.of(new ExecutionTradeCommand(orderId, venueId, venueTs, venueExecRef, lastQty, lastPx, leaves, cum));
    }

    public Optional<ExecutionCancelCommand> tryParseCancel(Message msg, String venueId) throws FieldNotFound {
        char execType = msg.getChar(ExecType.FIELD);
        if (execType != ExecType.CANCELED) {
            return Optional.empty();
        }
        UUID orderId = parseClOrdId(msg);
        if (orderId == null) {
            return Optional.empty();
        }
        Instant venueTs = parseTransactTime(msg).orElse(Instant.now());
        String venueExecRef = msg.getString(ExecID.FIELD);
        return Optional.of(new ExecutionCancelCommand(orderId, venueId, venueTs, venueExecRef));
    }

    /**
     * {@code ExecutionReport} with {@code ExecType=Rejected} (venue declined new order).
     */
    public Optional<ExecutionVenueRejectCommand> tryParseVenueReject(Message msg, String venueId) throws FieldNotFound {
        if (msg.getChar(ExecType.FIELD) != ExecType.REJECTED) {
            return Optional.empty();
        }
        UUID orderId = parseClOrdId(msg);
        if (orderId == null) {
            return Optional.empty();
        }
        Instant venueTs = parseTransactTime(msg).orElse(Instant.now());
        String venueExecRef = msg.getString(ExecID.FIELD);
        String raw = rawRejectedErJson(msg);
        return Optional.of(new ExecutionVenueRejectCommand(orderId, venueId, venueTs, venueExecRef, raw));
    }

    /**
     * FIX 4.4 {@code OrderCancelReject} (9).
     */
    public Optional<ExecutionVenueRejectCommand> tryParseOrderCancelReject(Message msg, String venueId)
            throws FieldNotFound {
        if (!MsgType.ORDER_CANCEL_REJECT.equals(msg.getHeader().getString(MsgType.FIELD))) {
            return Optional.empty();
        }
        UUID orderId = parseClOrdIdPreferOrig(msg);
        if (orderId == null) {
            return Optional.empty();
        }
        Instant venueTs = parseTransactTime(msg).orElse(Instant.now());
        int reason = msg.isSetField(CxlRejReason.FIELD) ? msg.getInt(CxlRejReason.FIELD) : 0;
        String brokerOrderId = msg.isSetField(OrderID.FIELD) ? msg.getString(OrderID.FIELD) : "unknown";
        String venueExecRef = "ocr-" + brokerOrderId + "-" + reason;
        String raw = rawOrderCancelRejectJson(msg);
        return Optional.of(new ExecutionVenueRejectCommand(orderId, venueId, venueTs, venueExecRef, raw));
    }

    private static UUID parseClOrdIdPreferOrig(Message msg) throws FieldNotFound {
        if (msg.isSetField(OrigClOrdID.FIELD)) {
            try {
                UUID u = parseUuid(msg.getString(OrigClOrdID.FIELD).trim());
                if (u != null) {
                    return u;
                }
            } catch (FieldNotFound e) {
                return parseClOrdId(msg);
            }
        }
        return parseClOrdId(msg);
    }

    private static UUID parseClOrdId(Message msg) {
        if (!msg.isSetField(ClOrdID.FIELD)) {
            return null;
        }
        try {
            return parseUuid(msg.getString(ClOrdID.FIELD).trim());
        } catch (IllegalArgumentException | FieldNotFound ex) {
            return null;
        }
    }

    private static UUID parseUuid(String raw) {
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static Optional<Instant> parseTransactTime(Message msg) throws FieldNotFound {
        if (!msg.isSetField(TransactTime.FIELD)) {
            return Optional.empty();
        }
        String raw = msg.getString(TransactTime.FIELD).trim();
        try {
            LocalDateTime ldt;
            int dot = raw.indexOf('.');
            if (dot > 0 && raw.length() > dot) {
                ldt = LocalDateTime.parse(raw, DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS"));
            } else {
                ldt = LocalDateTime.parse(raw, DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss"));
            }
            return Optional.of(ldt.atOffset(ZoneOffset.UTC).toInstant());
        } catch (DateTimeParseException ex) {
            return Optional.empty();
        }
    }

    private String rawRejectedErJson(Message msg) throws FieldNotFound {
        ObjectNode n = objectMapper.createObjectNode();
        n.put("kind", "ExecutionReport");
        n.put("execType", "REJECTED");
        if (msg.isSetField(Text.FIELD)) {
            n.put("text", msg.getString(Text.FIELD));
        }
        if (msg.isSetField(OrdStatus.FIELD)) {
            n.put("ordStatus", String.valueOf(msg.getChar(OrdStatus.FIELD)));
        }
        try {
            return objectMapper.writeValueAsString(n);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private String rawOrderCancelRejectJson(Message msg) throws FieldNotFound {
        ObjectNode n = objectMapper.createObjectNode();
        n.put("kind", "OrderCancelReject");
        n.put("cxlRejReason", msg.isSetField(CxlRejReason.FIELD) ? msg.getInt(CxlRejReason.FIELD) : 0);
        if (msg.isSetField(Text.FIELD)) {
            n.put("text", msg.getString(Text.FIELD));
        }
        try {
            return objectMapper.writeValueAsString(n);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
