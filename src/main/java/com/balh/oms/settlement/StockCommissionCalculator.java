package com.balh.oms.settlement;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Stock brokerage commission calculator used by the settlement outbox to compute
 * the {@code fee} leg posted to Ledger.
 *
 * <p>The schedule constants intentionally mirror customer-frontend's
 * {@code STOCK_<MARKET>}/{@code default}-tier rows in {@code fee_schedules}
 * (see {@code customer-frontend/supabase/migrations/20260519140000_fees_stock_tiers.sql}
 * and {@code customer-frontend/lib/server/resolveStockFee.ts}). For the demo the
 * customer is quoted at order placement and pinned to
 * {@code customer_orders.estimated_fee}; OMS recomputes here using the same
 * default-tier numbers so the settlement leg matches the customer-visible
 * estimate. The follow-up is to thread {@code estimated_fee} into the OMS
 * order row at placement time and read it back here (single source of truth).
 *
 * <p>Formula matches resolveStockFee:
 * {@code clamp(min, notional * fee_percent/100 + flat, max)} rounded to cents.
 */
public final class StockCommissionCalculator {

    /** Markets we have a default schedule for; matches STOCK_FEE_ALLOWED_MARKETS. */
    private static final String DEFAULT_MARKET = "US";

    private static final int CENTS_SCALE = 2;

    public record Schedule(
            String market,
            String currency,
            BigDecimal feePercent,
            BigDecimal flatFee,
            BigDecimal minFee,
            BigDecimal maxFee,
            String feeBalanceIndicator) {}

    /** Demo defaults — kept in sync with customer-frontend fee_schedules `default` tier. */
    private static final Schedule US_DEFAULT = new Schedule(
            "US",
            "USD",
            new BigDecimal("0.25"),     // 0.25 %
            BigDecimal.ZERO,
            new BigDecimal("1.00"),     // min $1
            new BigDecimal("50.00"),    // cap $50
            "@Fees-USD");

    private static final Schedule EU_DEFAULT = new Schedule(
            "EU",
            "EUR",
            new BigDecimal("0.25"),
            BigDecimal.ZERO,
            new BigDecimal("1.00"),
            new BigDecimal("50.00"),
            "@Fees-EUR");

    private static final Schedule UK_DEFAULT = new Schedule(
            "UK",
            "GBP",
            new BigDecimal("0.25"),
            BigDecimal.ZERO,
            new BigDecimal("1.00"),
            new BigDecimal("50.00"),
            "@Fees-GBP");

    private StockCommissionCalculator() {}

    /**
     * Resolve the default-tier schedule for a {@code market} code (US / EU / UK).
     * Falls back to US when the market is unknown.
     */
    public static Schedule defaultScheduleFor(String market) {
        String m = market == null ? DEFAULT_MARKET : market.trim().toUpperCase();
        return switch (m) {
            case "EU" -> EU_DEFAULT;
            case "UK" -> UK_DEFAULT;
            default -> US_DEFAULT;
        };
    }

    /** notional = quantity × price (positive). */
    public static BigDecimal notional(BigDecimal quantity, BigDecimal price) {
        if (quantity == null || price == null) return BigDecimal.ZERO;
        BigDecimal n = quantity.multiply(price);
        return n.signum() < 0 ? n.negate() : n;
    }

    public static BigDecimal feeFor(Schedule s, BigDecimal notional) {
        if (s == null || notional == null || notional.signum() == 0) return BigDecimal.ZERO;
        BigDecimal raw = notional
                .multiply(s.feePercent().divide(new BigDecimal("100"), 10, RoundingMode.HALF_UP))
                .add(s.flatFee());
        BigDecimal fee = raw.max(s.minFee());
        if (s.maxFee() != null) fee = fee.min(s.maxFee());
        return fee.setScale(CENTS_SCALE, RoundingMode.HALF_UP);
    }
}
