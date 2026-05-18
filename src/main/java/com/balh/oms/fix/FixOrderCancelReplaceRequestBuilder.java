package com.balh.oms.fix;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderReplaceRequestedEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.field.ClOrdID;
import quickfix.field.HandlInst;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.OrigClOrdID;
import quickfix.field.Price;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix44.OrderCancelReplaceRequest;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Builds FIX 4.4 {@link OrderCancelReplaceRequest} (35=G) from a cluster-emitted
 * {@link OrderReplaceRequestedEvent}, mirroring {@link FixOrderCancelRequestBuilder} for
 * structure and dedupe shape.
 *
 * <h2>Field choices</h2>
 *
 * <ul>
 *   <li>{@code OrigClOrdID(41)} = original NOS's ClOrdID = {@code orderId.toString()} — see
 *       {@link FixOrderCancelRequestBuilder} for the "we don't rotate ClOrdIDs across modify
 *       cycles" caveat.</li>
 *   <li>{@code ClOrdID(11)} = derived from {@code orderId + ":r:" + clientRequestKey}.
 *       Deterministic across replays so duplicate emissions hit {@code DupClOrdID} at the broker
 *       rather than producing two distinct replaces.</li>
 *   <li>{@code HandlInst(21)}: required in FIX 4.4 (gone in 5.0SP2). We set
 *       {@code AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION} to match the NOS we
 *       sent in the first place.</li>
 *   <li>{@code OrdType(40)}: {@code MARKET} when the resolved new price is zero, else
 *       {@code LIMIT}. The cluster's apply-replace contract already forbids requesting a price
 *       that does not match the existing OrdType shape — but the egress derives the OrdType
 *       from whatever price field ends up in the outbound message rather than the order's
 *       prior state, since the spec requires consistency on the wire.</li>
 *   <li>{@code OrderQty(38)}: new total quantity (FIX semantics — not a delta).</li>
 *   <li>{@code Price(44)}: when {@code newLimitPriceScaledOrZero != 0}, we send the new value.
 *       When the user requested "qty-only modify" by passing zero, we copy
 *       {@code originalLimitPriceScaledOrZero} so the price field on the wire equals what the
 *       order currently has — a 35=G must always carry the full new order shape, including
 *       price for limit orders. Zero on both fields ⇒ {@code OrdType=MARKET} (price tag
 *       omitted).</li>
 *   <li>{@code TimeInForce(59)}: mirror of the original order's TIF.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixOrderCancelReplaceRequestBuilder {

    public static final String CLORDID_REPLACE_SEPARATOR = ":r:";

    private static final BigDecimal QUANTITY_SCALE_BD = BigDecimal.valueOf(AcceptOrderCommand.QUANTITY_SCALE);
    private static final BigDecimal PRICE_SCALE_BD = BigDecimal.valueOf(AcceptOrderCommand.PRICE_SCALE);

    private final FixSymbolMapper fixSymbolMapper;

    public FixOrderCancelReplaceRequestBuilder(FixSymbolMapper fixSymbolMapper) {
        this.fixSymbolMapper = fixSymbolMapper;
    }

    public OrderCancelReplaceRequest build(OrderReplaceRequestedEvent event) {
        OrderCancelReplaceRequest msg = new OrderCancelReplaceRequest();
        msg.set(new OrigClOrdID(event.orderId().toString()));
        msg.set(new ClOrdID(deriveReplaceClOrdID(event)));
        msg.set(new HandlInst(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION));
        msg.set(new Symbol(fixSymbolMapper.toVenueSymbol(event.instrumentSymbol())));
        msg.set(new quickfix.field.Side(mapSide(event.sideCode())));
        BigDecimal quantity = BigDecimal.valueOf(event.newQuantityScaled())
                .divide(QUANTITY_SCALE_BD, /* scale = */ 10, RoundingMode.UNNECESSARY)
                .stripTrailingZeros();
        msg.setString(OrderQty.FIELD, plainString(quantity));

        long resolvedPriceScaled = event.newLimitPriceScaledOrZero() != 0L
                ? event.newLimitPriceScaledOrZero()
                : event.originalLimitPriceScaledOrZero();
        if (resolvedPriceScaled == 0L) {
            msg.set(new OrdType(OrdType.MARKET));
        } else {
            msg.set(new OrdType(OrdType.LIMIT));
            BigDecimal limitPrice = BigDecimal.valueOf(resolvedPriceScaled)
                    .divide(PRICE_SCALE_BD, /* scale = */ 10, RoundingMode.UNNECESSARY)
                    .stripTrailingZeros();
            msg.setString(Price.FIELD, plainString(limitPrice));
        }

        msg.set(new TimeInForce(mapTimeInForceCode(event.timeInForceCode())));
        msg.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        return msg;
    }

    /**
     * See {@link FixOrderCancelRequestBuilder#deriveCancelClOrdID} for the dedupe semantics —
     * this is the modify-side mirror: deterministic across replays so a re-emit collides at the
     * broker via {@code DupClOrdID} instead of producing two distinct 35=G messages.
     */
    public static String deriveReplaceClOrdID(OrderReplaceRequestedEvent event) {
        String suffix = event.clientRequestKey().isEmpty() ? "" : event.clientRequestKey();
        return event.orderId() + CLORDID_REPLACE_SEPARATOR + suffix;
    }

    private static char mapSide(byte sideCode) {
        return switch (sideCode) {
            case AcceptOrderCommand.SIDE_BUY -> quickfix.field.Side.BUY;
            case AcceptOrderCommand.SIDE_SELL -> quickfix.field.Side.SELL;
            default -> throw new IllegalArgumentException(
                    "unknown OrderReplaceRequestedEvent.sideCode: " + sideCode);
        };
    }

    private static char mapTimeInForceCode(byte tifCode) {
        return switch (tifCode) {
            case AcceptOrderCommand.TIF_DAY -> TimeInForce.DAY;
            case AcceptOrderCommand.TIF_IOC -> TimeInForce.IMMEDIATE_OR_CANCEL;
            case AcceptOrderCommand.TIF_FOK -> TimeInForce.FILL_OR_KILL;
            case AcceptOrderCommand.TIF_GTC -> TimeInForce.GOOD_TILL_CANCEL;
            default -> throw new IllegalArgumentException(
                    "unknown OrderReplaceRequestedEvent.timeInForceCode: " + tifCode);
        };
    }

    private static String plainString(BigDecimal value) {
        BigDecimal normalized = value.scale() < 0 ? value.setScale(0, RoundingMode.UNNECESSARY) : value;
        return normalized.toPlainString();
    }
}
