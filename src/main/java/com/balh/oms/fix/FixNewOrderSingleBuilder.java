package com.balh.oms.fix;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.field.ClOrdID;
import quickfix.field.HandlInst;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix44.NewOrderSingle;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Builds FIX 4.4 {@link NewOrderSingle} from domain {@link Order} (slice 4).
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixNewOrderSingleBuilder {

    public NewOrderSingle build(Order order) {
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID(order.id().toString()));
        nos.set(new HandlInst(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION));
        nos.set(new Symbol(order.instrumentSymbol()));
        nos.set(new quickfix.field.Side(
                order.side() == Side.BUY ? quickfix.field.Side.BUY : quickfix.field.Side.SELL));
        nos.setString(OrderQty.FIELD, order.quantity().toPlainString());
        if (order.limitPrice() != null) {
            nos.set(new OrdType(OrdType.LIMIT));
            nos.setString(Price.FIELD, order.limitPrice().toPlainString());
        } else {
            nos.set(new OrdType(OrdType.MARKET));
        }
        nos.set(new TimeInForce(mapTimeInForce(order.timeInForce())));
        nos.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        return nos;
    }

    private static char mapTimeInForce(String tif) {
        if (tif == null || tif.isBlank()) {
            return TimeInForce.DAY;
        }
        return switch (tif.trim().toUpperCase()) {
            case "GTC" -> TimeInForce.GOOD_TILL_CANCEL;
            case "IOC" -> TimeInForce.IMMEDIATE_OR_CANCEL;
            case "FOK" -> TimeInForce.FILL_OR_KILL;
            default -> TimeInForce.DAY;
        };
    }
}
