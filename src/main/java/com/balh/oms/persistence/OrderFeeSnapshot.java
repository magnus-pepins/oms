package com.balh.oms.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * BFF-pinned commission for one accepted order, persisted by the customer-frontend
 * proxy at OMS accept time. See V40 migration and {@link OrderFeeSnapshotRepository}
 * for the design rationale (resolveStockFee lookup priority + replay-safe pinning).
 *
 * <p>{@code feeSource} mirrors the customer-frontend {@code StockFeeSource} string
 * (one of {@code override}, {@code tier}, {@code default}, {@code no-match}) so
 * operators investigating a fee can trace why the customer was quoted that number.
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
        Instant createdAt) {}
