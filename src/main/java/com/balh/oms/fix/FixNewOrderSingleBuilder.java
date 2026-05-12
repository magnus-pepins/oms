package com.balh.oms.fix;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Builds FIX 4.4 {@link NewOrderSingle}.
 *
 * <p>Two entry points:
 *
 * <ul>
 *   <li>{@link #build(Order)} — legacy slice-4 path: takes a domain {@link Order} that came back
 *       from a Postgres lookup (typically inside {@code FixOutboundDispatchWorker}). Used by the
 *       legacy in-memory queue dispatcher.</li>
 *   <li>{@link #build(OrderAdmittedEvent)} — Phase 3 slice 3b-2 path: takes the cluster-emitted
 *       event directly so {@code OmsFixEgressService} never has to round-trip through Postgres on
 *       the hot send path. The {@code OrderAdmittedEvent} carries every NOS-relevant field
 *       ({@code clOrdID}, side, qty/price, TIF, instrument symbol — see codec on
 *       {@link OrderAdmittedEvent}), so the egress JVM hits the FIX wire from
 *       {@code AeronArchive.replay} → {@code Session.sendToTarget} with no SQL on the path.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixNewOrderSingleBuilder {

    private static final BigDecimal QUANTITY_SCALE_BD = BigDecimal.valueOf(AcceptOrderCommand.QUANTITY_SCALE);
    private static final BigDecimal PRICE_SCALE_BD = BigDecimal.valueOf(AcceptOrderCommand.PRICE_SCALE);

    private final FixSymbolMapper fixSymbolMapper;

    public FixNewOrderSingleBuilder(FixSymbolMapper fixSymbolMapper) {
        this.fixSymbolMapper = fixSymbolMapper;
    }

    public NewOrderSingle build(Order order) {
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID(order.id().toString()));
        nos.set(new HandlInst(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION));
        nos.set(new Symbol(fixSymbolMapper.toVenueSymbol(order.instrumentSymbol())));
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

    /**
     * Builds a NOS straight from a cluster-emitted {@link OrderAdmittedEvent}, without consulting
     * Postgres. This is the path used by {@code OmsFixEgressService} once Phase 3 slice 3b-2
     * lands; the legacy {@link #build(Order)} stays for the in-memory queue dispatcher until
     * {@code oms-fix-worker} is retired in slice 3g.
     *
     * <p>Field mapping mirrors {@link com.balh.oms.persistence.OrdersRepository}'s projector
     * inserts so a NOS built from {@code OrderAdmittedEvent} carries the same numeric values an
     * order built from the projected Postgres row would carry: quantity from the 1e9 fixed-point,
     * price from the 1e6 fixed-point (zero ⇒ market order, no {@code Price} field), side from the
     * byte code, TIF from the byte code. We use {@link RoundingMode#UNNECESSARY} (same as the
     * projector) so a value that does not divide cleanly throws — the codec validates fixed-point
     * scale so this is a "should never happen" bug-trip, not a runtime data-shape concern.
     */
    public NewOrderSingle build(OrderAdmittedEvent event) {
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID(event.orderId().toString()));
        nos.set(new HandlInst(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION));
        nos.set(new Symbol(fixSymbolMapper.toVenueSymbol(event.instrumentSymbol())));
        nos.set(new quickfix.field.Side(mapSide(event.side())));
        BigDecimal quantity = BigDecimal.valueOf(event.quantityScaled())
                .divide(QUANTITY_SCALE_BD, /* scale = */ 10, RoundingMode.UNNECESSARY)
                .stripTrailingZeros();
        nos.setString(OrderQty.FIELD, plainString(quantity));
        if (event.limitPriceScaledOrZero() == 0L) {
            nos.set(new OrdType(OrdType.MARKET));
        } else {
            nos.set(new OrdType(OrdType.LIMIT));
            BigDecimal limitPrice = BigDecimal.valueOf(event.limitPriceScaledOrZero())
                    .divide(PRICE_SCALE_BD, /* scale = */ 10, RoundingMode.UNNECESSARY)
                    .stripTrailingZeros();
            nos.setString(Price.FIELD, plainString(limitPrice));
        }
        nos.set(new TimeInForce(mapTimeInForceCode(event.timeInForceCode())));
        nos.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        return nos;
    }

    private static char mapSide(byte sideCode) {
        return switch (sideCode) {
            case AcceptOrderCommand.SIDE_BUY -> quickfix.field.Side.BUY;
            case AcceptOrderCommand.SIDE_SELL -> quickfix.field.Side.SELL;
            default -> throw new IllegalArgumentException(
                    "unknown OrderAdmittedEvent.side code: " + sideCode);
        };
    }

    private static char mapTimeInForceCode(byte tifCode) {
        return switch (tifCode) {
            case AcceptOrderCommand.TIF_DAY -> TimeInForce.DAY;
            case AcceptOrderCommand.TIF_IOC -> TimeInForce.IMMEDIATE_OR_CANCEL;
            case AcceptOrderCommand.TIF_FOK -> TimeInForce.FILL_OR_KILL;
            case AcceptOrderCommand.TIF_GTC -> TimeInForce.GOOD_TILL_CANCEL;
            default -> throw new IllegalArgumentException(
                    "unknown OrderAdmittedEvent.timeInForceCode: " + tifCode);
        };
    }

    /**
     * {@link BigDecimal#toPlainString()} on a value that has been
     * {@link BigDecimal#stripTrailingZeros() stripped} can return scientific notation for whole
     * numbers (e.g. {@code "1E+1"} for {@code "10"}). FIX expects fixed-point. Setting the scale
     * back to a non-negative value forces plain notation while keeping the trailing-zero strip on
     * fractional values.
     */
    private static String plainString(BigDecimal value) {
        BigDecimal normalized = value.scale() < 0 ? value.setScale(0, RoundingMode.UNNECESSARY) : value;
        return normalized.toPlainString();
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
