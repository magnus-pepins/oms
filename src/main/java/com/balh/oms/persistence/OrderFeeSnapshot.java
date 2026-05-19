package com.balh.oms.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * BFF-pinned per-order settlement metadata for one accepted order, persisted by
 * the customer-frontend proxy at OMS accept time. See V40 + V41 migration headers
 * and {@link OrderFeeSnapshotRepository} for the design rationale (resolveStockFee
 * lookup priority + replay-safe pinning + cross-currency cash plumbing).
 *
 * <p>{@code feeSource} mirrors the customer-frontend {@code StockFeeSource} string
 * (one of {@code override}, {@code tier}, {@code default}, {@code no-match}) so
 * operators investigating a fee can trace why the customer was quoted that number.
 *
 * <p>{@code cashCurrency} / {@code cashAmount} / {@code fxRate} are nullable
 * (V41). When {@code cashCurrency != null} and differs from the trade currency,
 * SettlementConfirmProcessor routes the cash leg as base/quote via
 * {@code @FX-Suspense-<ccy>} mirroring {@link com.balh.oms.fx.FxHedgeService}.
 */
public record OrderFeeSnapshot(
        UUID orderId,
        BigDecimal feeAmount,
        String feeCurrency,
        String feeBalanceIndicator,
        String feeTier,
        String feeSource,
        UUID feeScheduleId,
        UUID userFeeOverrideId,
        String cashCurrency,
        BigDecimal cashAmount,
        BigDecimal fxRate,
        Instant createdAt) {}
