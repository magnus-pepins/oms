package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
import com.balh.oms.routing.VenueRoutingSymbols;
import com.balh.oms.settlement.StockCommissionCalculator;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * Sizes BUY cash requirements (buying-power gate + Ledger inflight hold): notional at the
 * reference / limit price plus commission using the same default schedule as settlement when
 * no BFF fee snapshot exists yet.
 */
public final class BuyFundsRequirement {

    private BuyFundsRequirement() {}

    /**
     * Price used to size BUY funds: {@link Order#limitPrice()} (strict limit or MARKET reference cap).
     */
    public static Optional<BigDecimal> buyReferencePrice(Order order) {
        if (order == null || order.side() != Side.BUY) {
            return Optional.empty();
        }
        BigDecimal px = order.limitPrice();
        if (px == null || px.signum() <= 0) {
            return Optional.empty();
        }
        return Optional.of(px);
    }

    public static boolean hasBuyFundingPrice(Order order) {
        return buyReferencePrice(order).isPresent();
    }

    /**
     * @return notional + estimated fee, or empty when BUY funding price is missing
     */
    public static Optional<BigDecimal> requiredBuyFunds(Order order, OmsConfig config) {
        Optional<BigDecimal> price = buyReferencePrice(order);
        if (price.isEmpty() || order.quantity() == null) {
            return Optional.empty();
        }
        BigDecimal notional = StockCommissionCalculator.notional(order.quantity(), price.get());
        if (notional.signum() <= 0) {
            return Optional.empty();
        }
        // Venue-routed prediction markets: collateral is contract notional at the limit price,
        // not the equities min-fee schedule (€1 min on a €0.03 PREDMKT tick inflated soak holds
        // 33× and exhausted bench balance after failed ledger-on runs on pop).
        if (VenueRoutingSymbols.matchesVenuePrefix(
                VenueRoutingSymbols.venueSymbolPrefix(config), order.instrumentSymbol())) {
            return Optional.of(notional);
        }
        String market = config.getSettlement().getDefaultInstrumentMarket();
        StockCommissionCalculator.Schedule schedule = StockCommissionCalculator.defaultScheduleFor(market);
        BigDecimal fee = StockCommissionCalculator.feeFor(schedule, notional);
        return Optional.of(notional.add(fee));
    }

    public static Optional<BigDecimal> estimatedFee(Order order, OmsConfig config) {
        return requiredBuyFunds(order, config)
                .flatMap(total -> buyReferencePrice(order).map(px -> {
                    BigDecimal notional = StockCommissionCalculator.notional(order.quantity(), px);
                    return total.subtract(notional);
                }));
    }
}
