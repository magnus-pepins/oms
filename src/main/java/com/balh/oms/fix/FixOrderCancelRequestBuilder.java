package com.balh.oms.fix;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderCancelRequestedEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.field.ClOrdID;
import quickfix.field.OrderQty;
import quickfix.field.OrigClOrdID;
import quickfix.field.Symbol;
import quickfix.field.TransactTime;
import quickfix.fix44.OrderCancelRequest;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Builds FIX 4.4 {@link OrderCancelRequest} (35=F) straight from a cluster-emitted
 * {@link OrderCancelRequestedEvent}, mirroring the shape of {@link FixNewOrderSingleBuilder}.
 *
 * <h2>Field choices</h2>
 *
 * <ul>
 *   <li>{@code OrigClOrdID(41)} = original NOS's ClOrdID = {@code orderId.toString()}. OMS sends
 *       exactly one NOS per order and never rotates ClOrdIDs across modify cycles in the demo —
 *       so OrigClOrdID is always the order UUID. This matches what
 *       {@link FixNewOrderSingleBuilder#build} put on the wire, which is what the broker has on
 *       file under its OrderID(37).</li>
 *   <li>{@code ClOrdID(11)} = a fresh, deterministic ID derived from
 *       {@code orderId + ":c:" + clientRequestKey}. Deterministic so a cluster log replay
 *       re-emits the exact same ClOrdID and the broker dedupes via {@code DupClOrdID}; mirrors
 *       option 1 dedupe in {@code OmsFixEgressService}.</li>
 *   <li>{@code Side(54)} = mapped from the event's {@code sideCode}.</li>
 *   <li>{@code Symbol(55)} = venue symbol (after {@link FixSymbolMapper#toVenueSymbol}).</li>
 *   <li>{@code OrderQty(38)} = original NOS quantity (FIX 4.4 requires this on 35=F). FIX 4.4
 *       does not have a separate LeavesQty here; brokers typically ignore this field on the
 *       cancel side and look it up by OrigClOrdID — but the spec requires the tag, so we set it.
 *       We use the original order qty rather than leaves so the value matches what was on the
 *       NOS (least surprising for desks tailing FIX logs).</li>
 *   <li>{@code TransactTime(60)} = wall clock at build time.</li>
 * </ul>
 *
 * <p>If a broker requires {@code LeavesQty(151)} on the cancel side we can compute it as
 * {@code originalQuantityScaled - cumQtyScaled} — both are carried on
 * {@link OrderCancelRequestedEvent}.
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixOrderCancelRequestBuilder {

    public static final String CLORDID_CANCEL_SEPARATOR = ":c:";

    private static final BigDecimal QUANTITY_SCALE_BD = BigDecimal.valueOf(AcceptOrderCommand.QUANTITY_SCALE);

    private final FixSymbolMapper fixSymbolMapper;

    public FixOrderCancelRequestBuilder(FixSymbolMapper fixSymbolMapper) {
        this.fixSymbolMapper = fixSymbolMapper;
    }

    public OrderCancelRequest build(OrderCancelRequestedEvent event) {
        OrderCancelRequest msg = new OrderCancelRequest();
        msg.set(new OrigClOrdID(event.orderId().toString()));
        msg.set(new ClOrdID(deriveCancelClOrdID(event)));
        msg.set(new Symbol(fixSymbolMapper.toVenueSymbol(event.instrumentSymbol())));
        msg.set(new quickfix.field.Side(mapSide(event.sideCode())));
        BigDecimal quantity = BigDecimal.valueOf(event.originalQuantityScaled())
                .divide(QUANTITY_SCALE_BD, /* scale = */ 10, RoundingMode.UNNECESSARY)
                .stripTrailingZeros();
        msg.setString(OrderQty.FIELD, plainString(quantity));
        msg.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        return msg;
    }

    /**
     * Builds the cancel-side {@code ClOrdID(11)} from {@code (orderId, clientRequestKey)}.
     * Deterministic across replays so a re-emitted event from cluster log replay produces the
     * same ClOrdID; brokers reject duplicates via {@code DupClOrdID} (matches option 1 dedupe).
     *
     * <p>If {@code clientRequestKey} is empty (the cluster admits the request anyway, just
     * skipping the dedupe-index step), we fall back to {@code orderId + ":c"} — re-emission then
     * collides at the broker rather than producing two distinct cancel ClOrdIDs.
     */
    public static String deriveCancelClOrdID(OrderCancelRequestedEvent event) {
        String suffix = event.clientRequestKey().isEmpty() ? "" : event.clientRequestKey();
        return event.orderId() + CLORDID_CANCEL_SEPARATOR + suffix;
    }

    private static char mapSide(byte sideCode) {
        return switch (sideCode) {
            case AcceptOrderCommand.SIDE_BUY -> quickfix.field.Side.BUY;
            case AcceptOrderCommand.SIDE_SELL -> quickfix.field.Side.SELL;
            default -> throw new IllegalArgumentException(
                    "unknown OrderCancelRequestedEvent.sideCode: " + sideCode);
        };
    }

    private static String plainString(BigDecimal value) {
        BigDecimal normalized = value.scale() < 0 ? value.setScale(0, RoundingMode.UNNECESSARY) : value;
        return normalized.toPlainString();
    }
}
