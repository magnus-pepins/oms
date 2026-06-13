package com.balh.oms.fix;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import quickfix.field.Account;
import quickfix.field.ClOrdID;
import quickfix.field.ClientID;
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
 *   <li>{@link #build(Order)} — takes a domain {@link Order} loaded from Postgres. Retained for
 *       {@code FixManualMassCancelService} and any future Postgres-driven repair flows.</li>
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
    private final boolean emitPortfolioIdTag;
    private final int portfolioIdTag;
    private final boolean emitPortfolioAccountTag;
    private final boolean emitPortfolioClientIdTag;

    public FixNewOrderSingleBuilder(FixSymbolMapper fixSymbolMapper, OmsConfig omsConfig) {
        this.fixSymbolMapper = fixSymbolMapper;
        OmsConfig.Fix fix = omsConfig.getFix();
        this.emitPortfolioIdTag = fix.isEmitPortfolioIdTag();
        this.portfolioIdTag = fix.getPortfolioIdTag();
        this.emitPortfolioAccountTag = fix.isEmitPortfolioAccountTag();
        this.emitPortfolioClientIdTag = fix.isEmitPortfolioClientIdTag();
    }

    /**
     * Config-gated generic portfolio attribution. A counterparty that requires a trader id and/or
     * account on inbound orders is served by mirroring the OMS portfolio id into the standard
     * {@code Account(1)} / {@code ClientID(109)} fields and/or a custom UDF tag. No-op when no flag
     * is enabled or the order carries no portfolio id, so default wire output is unchanged.
     */
    private void applyPortfolioAttribution(NewOrderSingle nos, String portfolioIdOrNull) {
        if (portfolioIdOrNull == null || portfolioIdOrNull.isBlank()) {
            return;
        }
        String portfolioId = portfolioIdOrNull.trim();
        if (emitPortfolioAccountTag) {
            nos.set(new Account(portfolioId));
        }
        if (emitPortfolioClientIdTag) {
            // ClientID(109) is deprecated in FIX 4.4, so NewOrderSingle has no typed set(ClientID)
            // overload — set it by tag. Counterparties that still key trader identity off 109 accept this.
            nos.setString(ClientID.FIELD, portfolioId);
        }
        if (emitPortfolioIdTag) {
            nos.setString(portfolioIdTag, portfolioId);
        }
    }

    public NewOrderSingle build(Order order) {
        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID(order.id().toString()));
        nos.set(new HandlInst(HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION));
        nos.set(new Symbol(fixSymbolMapper.toVenueSymbol(order.instrumentSymbol())));
        nos.set(new quickfix.field.Side(
                order.side() == Side.BUY ? quickfix.field.Side.BUY : quickfix.field.Side.SELL));
        nos.setString(OrderQty.FIELD, order.quantity().toPlainString());
        // OrdType from the explicit order.ordType() string (canonical "MARKET"/"LIMIT"); Price
        // tag is set whenever a limit_price is present, so MARKET orders with a reference cap
        // carry a Price tag without being re-classified as LIMITs. See
        // build(OrderAdmittedEvent) for the Wed-demo rationale.
        nos.set(new OrdType(mapOrdTypeName(order.ordType())));
        if (order.limitPrice() != null) {
            nos.setString(Price.FIELD, order.limitPrice().toPlainString());
        }
        nos.set(new TimeInForce(mapTimeInForce(order.timeInForce())));
        nos.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        applyPortfolioAttribution(nos, order.portfolioId());
        return nos;
    }

    private static char mapOrdTypeName(String ordType) {
        String n = ordType == null ? "MARKET" : ordType.trim().toUpperCase(java.util.Locale.ROOT);
        return switch (n) {
            case "MARKET" -> OrdType.MARKET;
            case "LIMIT" -> OrdType.LIMIT;
            default -> throw new IllegalArgumentException("unsupported order.ordType: " + ordType);
        };
    }

    /**
     * Builds a NOS straight from a cluster-emitted {@link OrderAdmittedEvent}, without consulting
     * Postgres. This is the only NOS path on {@code OmsFixEgressService}; the legacy
     * {@link #build(Order)} overload stays for {@code FixManualMassCancelService} and any future
     * Postgres-driven repair flows.
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
        // Wed-demo: OrdType is now driven by the explicit ordTypeCode field on the event, NOT
        // by limitPrice presence. The Price tag is set independently whenever limit_price > 0
        // so that a MARKET order can carry a reference / cap price the venue (or simulator)
        // uses for the fill — without re-classifying the order as LIMIT (the simulator's LIMIT
        // branch rests in book and never fills until cancel/replace; the MARKET branch
        // instant-fills at the inbound Price tag if present, else falls back to "1").
        nos.set(new OrdType(mapOrdType(event.ordTypeCode())));
        if (event.limitPriceScaledOrZero() != 0L) {
            BigDecimal limitPrice = BigDecimal.valueOf(event.limitPriceScaledOrZero())
                    .divide(PRICE_SCALE_BD, /* scale = */ 10, RoundingMode.UNNECESSARY)
                    .stripTrailingZeros();
            nos.setString(Price.FIELD, plainString(limitPrice));
        }
        nos.set(new TimeInForce(mapTimeInForceCode(event.timeInForceCode())));
        nos.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        applyPortfolioAttribution(nos, event.portfolioIdOrNull());
        return nos;
    }

    private static char mapOrdType(byte ordTypeCode) {
        return switch (ordTypeCode) {
            case AcceptOrderCommand.ORD_TYPE_MARKET -> OrdType.MARKET;
            case AcceptOrderCommand.ORD_TYPE_LIMIT -> OrdType.LIMIT;
            default -> throw new IllegalArgumentException(
                    "unknown OrderAdmittedEvent.ordTypeCode: " + ordTypeCode);
        };
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
