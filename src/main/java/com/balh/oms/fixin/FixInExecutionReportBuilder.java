package com.balh.oms.fixin;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.ExecutionAppliedEvent;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.Order;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LeavesQty;
import quickfix.field.OrdStatus;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrdType;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TimeInForce;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.TransactTime;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.OrderCancelReject;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInExecutionReportBuilder {

    public ExecutionReport buildNew(UUID omsOrderId, FixInParsedNewOrder parsed) {
        ExecutionReport er = baseReport(omsOrderId, parsed, ExecType.NEW, OrdStatus.NEW);
        er.setString(OrderQty.FIELD, plain(parsed.quantity()));
        setOrdTypeAndPrice(er, parsed);
        return er;
    }

    public ExecutionReport buildRejected(UUID omsOrderId, FixInParsedNewOrder parsed, String reason) {
        ExecutionReport er = baseReport(omsOrderId, parsed, ExecType.REJECTED, OrdStatus.REJECTED);
        er.setString(Text.FIELD, reason == null ? "rejected" : reason);
        return er;
    }

    public ExecutionReport buildFromExecutionApplied(
            Order order, String clientClOrdId, ExecutionAppliedEvent ev) {
        char execType;
        char ordStatus;
        switch (ev.execTypeCode()) {
            case ApplyExecutionReportCommand.EXEC_TYPE_TRADE -> {
                boolean full = ev.newCumQtyScaled() >= scaleQty(order.quantity());
                execType = full ? ExecType.FILL : ExecType.PARTIAL_FILL;
                ordStatus = full ? OrdStatus.FILLED : OrdStatus.PARTIALLY_FILLED;
            }
            case ApplyExecutionReportCommand.EXEC_TYPE_CANCEL -> {
                execType = ExecType.CANCELED;
                ordStatus = OrdStatus.CANCELED;
            }
            case ApplyExecutionReportCommand.EXEC_TYPE_REPLACE -> {
                execType = ExecType.REPLACED;
                ordStatus = OrdStatus.REPLACED;
            }
            case ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT -> {
                execType = ExecType.REJECTED;
                ordStatus = OrdStatus.REJECTED;
            }
            default -> throw new IllegalArgumentException("unsupported execTypeCode=" + ev.execTypeCode());
        }
        ExecutionReport er = new ExecutionReport();
        er.set(new ClOrdID(clientClOrdId));
        er.set(new OrderID(order.id().toString()));
        er.set(new ExecID("oms-exec-" + ev.venueExecRef()));
        er.set(new ExecType(execType));
        er.set(new OrdStatus(ordStatus));
        er.set(new Side(mapSideFromOrder(order.side())));
        er.set(new Symbol(order.instrumentSymbol()));
        er.set(new TimeInForce(mapTifFromOrder(order.timeInForce())));
        er.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        BigDecimal cumQty = unscaleQty(ev.newCumQtyScaled());
        er.setString(CumQty.FIELD, plain(cumQty));
        BigDecimal leaves = order.quantity().subtract(cumQty).max(BigDecimal.ZERO);
        er.setString(LeavesQty.FIELD, plain(leaves));
        er.setString(OrderQty.FIELD, plain(order.quantity()));
        if (ev.lastQtyScaled() > 0) {
            er.setString(LastQty.FIELD, plain(unscaleQty(ev.lastQtyScaled())));
        }
        if (ev.lastPxScaled() > 0) {
            er.setString(LastPx.FIELD, plain(unscalePrice(ev.lastPxScaled())));
        }
        return er;
    }

    public OrderCancelReject buildCancelReject(
            UUID omsOrderId, String clientClOrdId, String origClientClOrdId, String reason) {
        OrderCancelReject reject = new OrderCancelReject();
        reject.set(new ClOrdID(clientClOrdId));
        reject.setField(new quickfix.field.OrigClOrdID(origClientClOrdId));
        reject.set(new OrderID(omsOrderId.toString()));
        reject.setString(Text.FIELD, reason == null ? "rejected" : reason);
        return reject;
    }

    private ExecutionReport baseReport(
            UUID omsOrderId, FixInParsedNewOrder parsed, char execType, char ordStatus) {
        ExecutionReport er = new ExecutionReport();
        er.set(new ClOrdID(parsed.clientClOrdId()));
        er.set(new OrderID(omsOrderId.toString()));
        er.set(new ExecID("oms-admit-" + omsOrderId));
        er.set(new ExecType(execType));
        er.set(new OrdStatus(ordStatus));
        er.set(new Side(mapSide(parsed.sideCode())));
        er.set(new Symbol(parsed.instrumentSymbol()));
        er.set(new TimeInForce(mapTif(parsed.timeInForceCode())));
        er.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        er.setString(CumQty.FIELD, "0");
        er.setString(LeavesQty.FIELD, plain(parsed.quantity()));
        return er;
    }

    private static void setOrdTypeAndPrice(ExecutionReport er, FixInParsedNewOrder parsed) {
        if (parsed.ordTypeCode() == AcceptOrderCommand.ORD_TYPE_LIMIT) {
            er.set(new OrdType(OrdType.LIMIT));
            er.setString(Price.FIELD, plain(parsed.limitPriceOrNull()));
        } else {
            er.set(new OrdType(OrdType.MARKET));
            if (parsed.limitPriceOrNull() != null) {
                er.setString(Price.FIELD, plain(parsed.limitPriceOrNull()));
            }
        }
    }

    private static char mapSide(byte sideCode) {
        return sideCode == AcceptOrderCommand.SIDE_SELL ? Side.SELL : Side.BUY;
    }

    private static char mapSideFromOrder(com.balh.oms.domain.Side side) {
        return side == com.balh.oms.domain.Side.SELL ? Side.SELL : Side.BUY;
    }

    private static char mapTifFromOrder(String tif) {
        if (tif == null) {
            return TimeInForce.DAY;
        }
        return switch (tif.trim().toUpperCase()) {
            case "IOC" -> TimeInForce.IMMEDIATE_OR_CANCEL;
            case "FOK" -> TimeInForce.FILL_OR_KILL;
            case "GTC" -> TimeInForce.GOOD_TILL_CANCEL;
            default -> TimeInForce.DAY;
        };
    }

    private static long scaleQty(BigDecimal quantity) {
        return quantity.movePointRight(9).longValueExact();
    }

    private static BigDecimal unscaleQty(long scaled) {
        return BigDecimal.valueOf(scaled, 9);
    }

    private static BigDecimal unscalePrice(long scaled) {
        return BigDecimal.valueOf(scaled, 6);
    }

    private static char mapTif(byte tifCode) {
        return switch (tifCode) {
            case AcceptOrderCommand.TIF_IOC -> TimeInForce.IMMEDIATE_OR_CANCEL;
            case AcceptOrderCommand.TIF_FOK -> TimeInForce.FILL_OR_KILL;
            case AcceptOrderCommand.TIF_GTC -> TimeInForce.GOOD_TILL_CANCEL;
            default -> TimeInForce.DAY;
        };
    }

    private static String plain(BigDecimal value) {
        BigDecimal normalized = value.scale() < 0 ? value.setScale(0) : value;
        return normalized.toPlainString();
    }
}
