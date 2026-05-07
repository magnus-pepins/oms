package com.balh.oms.fix;

import com.balh.oms.returnpath.ExecutionCancelCommand;
import com.balh.oms.returnpath.ExecutionTradeCommand;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
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
 * Maps FIX 4.4 {@code ExecutionReport} fields to return-path commands (slice 4).
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixExecutionReportMapper {

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

    private static UUID parseClOrdId(Message msg) {
        if (!msg.isSetField(ClOrdID.FIELD)) {
            return null;
        }
        try {
            return UUID.fromString(msg.getString(ClOrdID.FIELD).trim());
        } catch (IllegalArgumentException | FieldNotFound ex) {
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
}
