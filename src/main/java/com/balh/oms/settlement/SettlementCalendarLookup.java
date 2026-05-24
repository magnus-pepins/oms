package com.balh.oms.settlement;

import java.time.LocalDate;
import java.util.Set;

/**
 * Read-only lookup over {@code settlement_calendar} (Flyway V63, gap plan §5.3 Slice 2b-3).
 *
 * <p>Mirrors the shape of {@link SettlementProfileLookup}: a narrow interface so
 * {@link SettlementDateCalculator} can depend on a fakable abstraction rather than the JDBC
 * repository. Production binds {@link SettlementCalendarRepository}; unit tests pass a
 * lambda or hand-rolled fake.
 *
 * <p>The bulk-range method is the only entry point because the calculator advances at most
 * a small window (T+N + weekend bumps + holiday bumps ≈ 2N + 7 days for N ≤ 3) and pulling
 * a {@link Set} once is cheaper than N point-lookups against the index. The
 * {@code (calendar_id, holiday_date)} primary key + the auxiliary range index make the
 * range scan effectively constant-time for these windows.
 */
public interface SettlementCalendarLookup {
    /**
     * Returns the set of non-business dates for {@code calendarId} that fall in
     * {@code [from, to]} inclusive. Returns an empty set when {@code calendarId} is null,
     * blank, or has no rows in the range.
     */
    Set<LocalDate> findHolidaysInRange(String calendarId, LocalDate from, LocalDate to);
}
