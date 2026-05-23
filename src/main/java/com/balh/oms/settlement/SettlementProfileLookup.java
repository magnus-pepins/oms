package com.balh.oms.settlement;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Read-only lookup over {@code instrument_settlement_profile} (Flyway V61, gap plan
 * §5.3 Slice 2b-1).
 *
 * <p>Exists so {@link SettlementDateCalculator} can depend on a narrow, easily-faked
 * interface rather than the concrete JDBC repository. The Spring wiring binds the
 * production implementation ({@link InstrumentSettlementProfileRepository}); unit tests
 * pass a lambda or hand-rolled in-memory fake.
 *
 * <p>Implementations <strong>must</strong> only return rows whose effective-dated window
 * contains {@code asOf}: {@code effective_from <= asOf AND (effective_to IS NULL OR
 * asOf < effective_to)}. The half-open upper bound matches the V61 CHECK constraint
 * ({@code effective_to > effective_from}) and lets the EU T+1 migration on 2027-10-11 be
 * represented as one closing row with {@code effective_to = 2027-10-11} and one opening
 * row with {@code effective_from = 2027-10-11}.
 */
@FunctionalInterface
public interface SettlementProfileLookup {
    Optional<InstrumentSettlementProfile> findActiveBySymbol(String symbol, LocalDate asOf);
}
