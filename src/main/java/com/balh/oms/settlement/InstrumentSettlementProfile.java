package com.balh.oms.settlement;

import java.time.LocalDate;

/**
 * One effective-dated row of the per-instrument settlement profile (Flyway V61, gap plan
 * §5.3 Slice 2b-1).
 *
 * <p>The {@link SettlementDateCalculator} only reads {@link #settlementCycle()} at trade
 * projection time today; the rest of the fields are carried for forward use:
 *
 * <ul>
 *   <li>{@link #settlementCurrency()} — used by the ledger-booking outbox in §5.4 once
 *       per-instrument currency overrides the order's account currency.</li>
 *   <li>{@link #iskEligible()} — gates ISK-account flows in §5.10 (Swedish ISK enforcement).</li>
 *   <li>{@link #primaryMic()} — drives the trade-date timezone fix that replaces the
 *       UTC-projection placeholder in {@link SettlementDateCalculator#computeTradeDate}.</li>
 *   <li>{@link #settlementCalendarId()} — used by the holiday-aware compute path landing
 *       with §5.3 Slice 2b-3.</li>
 * </ul>
 *
 * <p>The composite natural key is {@code (instrumentId, effectiveFrom)} (see V61 UNIQUE
 * constraint); the {@code id} surrogate is exposed so future ops tables (audit, calendar
 * overrides) can FK to a profile without re-deriving the composite key.
 */
public record InstrumentSettlementProfile(
        long id,
        String instrumentId,
        String symbol,
        String isin,
        String primaryMic,
        String settlementCalendarId,
        String settlementCycle,
        String settlementCurrency,
        boolean iskEligible,
        LocalDate effectiveFrom,
        LocalDate effectiveTo) {}
