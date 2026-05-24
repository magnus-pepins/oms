package com.balh.oms.settlement;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SettlementDateCalculator}. Stock-settlement gap plan §5.3 Slice 1.
 *
 * <p>Tests are deliberately exhaustive on the small surface: parser, weekday adders, weekend
 * rollovers, and the explicit T+0-on-weekend behaviour. The matcher integration with broker
 * confirms lives in {@code BrokerTradeConfirmMatcherIntegrationTest} (added in Slice 2 of
 * §5.3) and is intentionally not duplicated here.
 */
class SettlementDateCalculatorTest {

    @Test
    void defaultCycleFallbackIsTplus2() {
        var calc = new SettlementDateCalculator(SettlementDateCalculator.DEFAULT_CYCLE_FALLBACK);
        assertThat(calc.defaultCycleDays()).isEqualTo(2);
    }

    @Test
    void parsesValidCycleStrings() {
        assertThat(new SettlementDateCalculator("T+0").defaultCycleDays()).isZero();
        assertThat(new SettlementDateCalculator("T+1").defaultCycleDays()).isEqualTo(1);
        assertThat(new SettlementDateCalculator("T+2").defaultCycleDays()).isEqualTo(2);
        assertThat(new SettlementDateCalculator("T+3").defaultCycleDays()).isEqualTo(3);
        assertThat(new SettlementDateCalculator(" t+1 ").defaultCycleDays()).isEqualTo(1);
    }

    @Test
    void rejectsMalformedCycleStrings() {
        for (String bad : new String[] {"1", "T1", "T+", "T+a", "T+-1", "T+99", null}) {
            assertThatThrownBy(() -> new SettlementDateCalculator(bad))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void computeTradeDateProjectsUtcCalendarDate() {
        var calc = new SettlementDateCalculator("T+2");
        Instant venueTs = Instant.parse("2026-05-20T13:45:30Z");
        assertThat(calc.computeTradeDate(venueTs)).isEqualTo(LocalDate.of(2026, 5, 20));
    }

    @Test
    void computeTradeDateRejectsNull() {
        var calc = new SettlementDateCalculator("T+2");
        assertThatThrownBy(() -> calc.computeTradeDate(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void computeTradeDateAtUtcMidnightCrossover() {
        var calc = new SettlementDateCalculator("T+2");
        Instant justBeforeMidnight = LocalDate.of(2026, 5, 19)
                .atTime(23, 59, 59).toInstant(ZoneOffset.UTC);
        Instant justAfterMidnight = LocalDate.of(2026, 5, 20)
                .atStartOfDay().toInstant(ZoneOffset.UTC);
        assertThat(calc.computeTradeDate(justBeforeMidnight)).isEqualTo(LocalDate.of(2026, 5, 19));
        assertThat(calc.computeTradeDate(justAfterMidnight)).isEqualTo(LocalDate.of(2026, 5, 20));
    }

    @Test
    void expectedSettlementDateAddsTwoBusinessDaysForMidweekTrade() {
        var calc = new SettlementDateCalculator("T+2");
        // 2026-05-20 was a Wednesday → T+2 is Friday 2026-05-22.
        assertThat(calc.computeExpectedSettlementDate(LocalDate.of(2026, 5, 20)))
                .isEqualTo(LocalDate.of(2026, 5, 22));
    }

    @Test
    void expectedSettlementDateRollsOverWeekendForThursdayTrade() {
        var calc = new SettlementDateCalculator("T+2");
        // 2026-05-21 Thursday → T+1 Friday → T+2 skips Sat/Sun → Monday 2026-05-25.
        assertThat(calc.computeExpectedSettlementDate(LocalDate.of(2026, 5, 21)))
                .isEqualTo(LocalDate.of(2026, 5, 25));
    }

    @Test
    void expectedSettlementDateForFridayTradeIsTuesdayOnTplus2() {
        var calc = new SettlementDateCalculator("T+2");
        // 2026-05-22 Friday → T+1 Monday → T+2 Tuesday 2026-05-26.
        assertThat(calc.computeExpectedSettlementDate(LocalDate.of(2026, 5, 22)))
                .isEqualTo(LocalDate.of(2026, 5, 26));
    }

    @Test
    void expectedSettlementDateForUsTplus1ConventionMatchesNextBusinessDay() {
        var calc = new SettlementDateCalculator("T+1");
        assertThat(calc.computeExpectedSettlementDate(LocalDate.of(2026, 5, 20))) // Wed → Thu
                .isEqualTo(LocalDate.of(2026, 5, 21));
        assertThat(calc.computeExpectedSettlementDate(LocalDate.of(2026, 5, 22))) // Fri → Mon
                .isEqualTo(LocalDate.of(2026, 5, 25));
    }

    @Test
    void tplusZeroOnBusinessDayReturnsSameDate() {
        var calc = new SettlementDateCalculator("T+0");
        assertThat(calc.computeExpectedSettlementDate(LocalDate.of(2026, 5, 20))) // Wed
                .isEqualTo(LocalDate.of(2026, 5, 20));
    }

    @Test
    void tplusZeroOnWeekendRollsToNextBusinessDay() {
        var calc = new SettlementDateCalculator("T+0");
        // Trade-date 2026-05-23 was Saturday — bumped to Monday 2026-05-25.
        assertThat(calc.computeExpectedSettlementDate(LocalDate.of(2026, 5, 23)))
                .isEqualTo(LocalDate.of(2026, 5, 25));
        assertThat(calc.computeExpectedSettlementDate(LocalDate.of(2026, 5, 24)))
                .isEqualTo(LocalDate.of(2026, 5, 25));
    }

    @Test
    void computeExpectedSettlementDateRejectsNull() {
        var calc = new SettlementDateCalculator("T+2");
        assertThatThrownBy(() -> calc.computeExpectedSettlementDate(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------------------
    // Slice 2b-1: resolveExpectedSettlementDate(tradeDate, instrumentSymbol)
    // ---------------------------------------------------------------------------

    @Test
    void resolve_withoutLookup_fallsBackToDefaultCycle() {
        // No SettlementProfileLookup wired (true in unit tests by default). The
        // resolver MUST behave identically to Slice 1's computeExpectedSettlementDate.
        var calc = new SettlementDateCalculator("T+2");
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "AAPL"))
                .isEqualTo(LocalDate.of(2026, 5, 22));
    }

    @Test
    void resolve_withNullSymbol_skipsLookupAndUsesDefault() {
        var calc = new SettlementDateCalculator("T+2");
        AtomicInteger lookupCalls = new AtomicInteger(0);
        calc.setProfileLookup((symbol, asOf) -> {
            lookupCalls.incrementAndGet();
            return Optional.empty();
        });
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), null))
                .isEqualTo(LocalDate.of(2026, 5, 22));
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "  "))
                .isEqualTo(LocalDate.of(2026, 5, 22));
        // Null/blank symbol short-circuits before hitting the lookup — avoids a wasted DB
        // round-trip on every projector trade insert where symbol is somehow blank.
        assertThat(lookupCalls.get()).isZero();
    }

    @Test
    void resolve_profileMiss_fallsBackToDefaultCycle() {
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.empty());
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "AAPL"))
                .isEqualTo(LocalDate.of(2026, 5, 22)); // T+2 default
    }

    @Test
    void resolve_profileHitTplus1OverridesDefaultTplus2() {
        // Default cycle is T+2 (EU/UK), but the AAPL profile carries T+1 (US-listed
        // equities post-2024). The resolver must honour the profile, not the default.
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> {
            if ("AAPL".equals(symbol)) {
                return Optional.of(stubProfile("AAPL", "T+1", LocalDate.of(2024, 5, 28), null));
            }
            return Optional.empty();
        });
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "AAPL"))
                .isEqualTo(LocalDate.of(2026, 5, 21)); // T+1
    }

    @Test
    void resolve_profileHitTplus2WithDefaultTplus1StillUsesProfile() {
        // Mirror of the above: even when the default is T+1, a T+2 profile (e.g. XSTO)
        // must win.
        var calc = new SettlementDateCalculator("T+1");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("ABB", "T+2", LocalDate.of(2024, 1, 1), null)));
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "ABB"))
                .isEqualTo(LocalDate.of(2026, 5, 22));
    }

    @Test
    void resolve_profileHitTplus0SettlesOnTradeDateWhenWeekday() {
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("USDT", "T+0", LocalDate.of(2024, 1, 1), null)));
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "USDT"))
                .isEqualTo(LocalDate.of(2026, 5, 20));
    }

    @Test
    void resolve_profileLookupThrows_logsAndFallsBackToDefault() {
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> {
            throw new RuntimeException("simulated JDBC failure");
        });
        // Failure inside the lookup must not propagate — the projector's trade-projection
        // writer would otherwise crash on an unrelated read path. Falling back to default
        // keeps inserts moving; a real persistence problem will surface at the executions
        // insert itself.
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "AAPL"))
                .isEqualTo(LocalDate.of(2026, 5, 22));
    }

    @Test
    void resolve_profileWithMalformedCycle_logsAndFallsBackToDefault() {
        // The V61 CHECK constraint enforces a 4-value allowlist, but if a future
        // migration widens it without updating the parser, the resolver must degrade
        // gracefully (default cycle) rather than crash the projector.
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("BOND", "T+99", LocalDate.of(2024, 1, 1), null)));
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "BOND"))
                .isEqualTo(LocalDate.of(2026, 5, 22));
    }

    @Test
    void resolve_passesTradeDateAsAsOfToLookup() {
        // Verifies the calculator queries the profile for the trade date, not for "now".
        // This matters for the EU T+1 migration on 2027-10-11: a trade-date of 2027-10-10
        // must still see the T+2 row, not the post-migration T+1 row.
        var calc = new SettlementDateCalculator("T+2");
        AtomicInteger capturedAsOfYear = new AtomicInteger(-1);
        calc.setProfileLookup((symbol, asOf) -> {
            capturedAsOfYear.set(asOf.getYear());
            return Optional.empty();
        });
        calc.resolveExpectedSettlementDate(LocalDate.of(2027, 10, 10), "ABB");
        assertThat(capturedAsOfYear.get()).isEqualTo(2027);
    }

    @Test
    void resolve_rejectsNullTradeDate() {
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.empty());
        assertThatThrownBy(() -> calc.resolveExpectedSettlementDate(null, "AAPL"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static InstrumentSettlementProfile stubProfile(
            String symbol, String cycle, LocalDate effFrom, LocalDate effTo) {
        return new InstrumentSettlementProfile(
                /* id = */ 1L,
                /* instrumentId = */ symbol + "-INST",
                symbol,
                /* isin = */ null,
                /* primaryMic = */ "XSTO",
                /* settlementCalendarId = */ "XSTO-CAL",
                cycle,
                /* settlementCurrency = */ "USD",
                /* iskEligible = */ false,
                effFrom,
                effTo);
    }

    // ---------------------------------------------------------------------------
    // Slice 2b-3: holiday-aware advance via SettlementCalendarLookup
    // ---------------------------------------------------------------------------

    @Test
    void resolve_withoutCalendarLookup_skipsOnlyWeekends() {
        // No SettlementCalendarLookup wired → calculator falls back to weekend-only
        // skipping even if the profile carries a calendar_id. Same as Slice 1.
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("AAPL", "T+2", LocalDate.of(2024, 1, 1), null)));
        // 2026-04-02 Thursday → T+1 Fri 2026-04-03 (Good Friday in XSTO/XNAS) → T+2 Mon 2026-04-06.
        // Without calendar awareness Good Friday counts as a business day, so T+2 = Mon 2026-04-06
        // anyway (Sun rolls forward). Pick a window that proves the calendar is NOT consulted:
        // 2026-12-23 Wed → T+1 Thu 2026-12-24 → T+2 Fri 2026-12-25. The matcher will later open
        // a settlement_date_mismatch break if the broker disagreed, which is the intended
        // safety net for empty-calendar deployments.
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 12, 23), "AAPL"))
                .isEqualTo(LocalDate.of(2026, 12, 25));
    }

    @Test
    void resolve_withCalendarLookup_skipsHolidaysInWindow() {
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("AAPL", "T+2", LocalDate.of(2024, 1, 1), null)));
        calc.setCalendarLookup((calendarId, from, to) -> {
            assertThat(calendarId).isEqualTo("XSTO-CAL");
            Set<LocalDate> hols = new HashSet<>();
            hols.add(LocalDate.of(2026, 12, 24)); // Christmas Eve (settlement closed)
            hols.add(LocalDate.of(2026, 12, 25)); // Christmas Day
            return hols;
        });
        // 2026-12-23 Wed trade → T+2 must skip Thu 12-24 AND Fri 12-25 (both holidays) and
        // weekend Sat/Sun 26/27, landing on Mon 2026-12-28. Then T+2 means we still need
        // one more business day → Tue 2026-12-29.
        // Walk: start 12-23 Wed, +1=12-24 (holiday, remaining=2), +1=12-25 (holiday, remaining=2),
        //   +1=12-26 (Sat, remaining=2), +1=12-27 (Sun, remaining=2),
        //   +1=12-28 (Mon, business → remaining=1), +1=12-29 (Tue, business → remaining=0).
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 12, 23), "AAPL"))
                .isEqualTo(LocalDate.of(2026, 12, 29));
    }

    @Test
    void resolve_calendarLookupOnTplus0_rollsHolidayForwardToNextBusinessDay() {
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("USDT", "T+0", LocalDate.of(2024, 1, 1), null)));
        calc.setCalendarLookup((calendarId, from, to) ->
                Set.of(LocalDate.of(2026, 12, 25)));
        // T+0 on Christmas Day → must roll forward to Mon 2026-12-28 (Sat 12-26 + Sun 12-27 skipped).
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 12, 25), "USDT"))
                .isEqualTo(LocalDate.of(2026, 12, 28));
    }

    @Test
    void resolve_calendarLookupHonoursProfileCalendarId_notADifferentOne() {
        // Profile says XSTO-CAL; calculator must query the lookup with exactly that id,
        // not the symbol or any other heuristic.
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("ABB", "T+2", LocalDate.of(2024, 1, 1), null)));
        AtomicInteger calls = new AtomicInteger(0);
        calc.setCalendarLookup((calendarId, from, to) -> {
            calls.incrementAndGet();
            assertThat(calendarId).isEqualTo("XSTO-CAL");
            return Set.of();
        });
        calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "ABB");
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void resolve_calendarLookupQueryRangeStartsAtTradeDate() {
        // The range query must start at tradeDate (not "today" — that would miss
        // back-projection of historical trades) and end well past tradeDate + cycleDays.
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("AAPL", "T+2", LocalDate.of(2024, 1, 1), null)));
        AtomicInteger lookupSeenFromYear = new AtomicInteger(-1);
        calc.setCalendarLookup((calendarId, from, to) -> {
            lookupSeenFromYear.set(from.getYear());
            assertThat(to).isAfter(from);
            return Set.of();
        });
        calc.resolveExpectedSettlementDate(LocalDate.of(2027, 10, 8), "AAPL");
        assertThat(lookupSeenFromYear.get()).isEqualTo(2027);
    }

    @Test
    void resolve_calendarLookupThrows_fallsBackToWeekendOnlySkipping() {
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.of(stubProfile("AAPL", "T+2", LocalDate.of(2024, 1, 1), null)));
        calc.setCalendarLookup((calendarId, from, to) -> {
            throw new RuntimeException("simulated JDBC failure on calendar read");
        });
        // 2026-05-20 Wed → T+2 = Fri 2026-05-22, no holidays in range. Failure must
        // degrade gracefully.
        assertThat(calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "AAPL"))
                .isEqualTo(LocalDate.of(2026, 5, 22));
    }

    @Test
    void resolve_calendarLookupNotConsultedWhenProfileHasNoCalendarId() {
        // A profile with blank calendar id means "no calendar awareness" — calculator
        // must not query the lookup.
        var calc = new SettlementDateCalculator("T+2");
        var noCalProfile = new InstrumentSettlementProfile(
                1L, "AAPL-INST", "AAPL", null, "XNAS",
                /* settlementCalendarId = */ "",
                "T+2", "USD", false,
                LocalDate.of(2024, 1, 1), null);
        calc.setProfileLookup((symbol, asOf) -> Optional.of(noCalProfile));
        AtomicInteger calls = new AtomicInteger(0);
        calc.setCalendarLookup((calendarId, from, to) -> {
            calls.incrementAndGet();
            return Set.of();
        });
        calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "AAPL");
        assertThat(calls.get()).isZero();
    }

    @Test
    void resolve_calendarLookupNotConsultedWhenProfileMisses() {
        // Profile miss → default-cycle fallback path → no calendar id → no calendar query.
        var calc = new SettlementDateCalculator("T+2");
        calc.setProfileLookup((symbol, asOf) -> Optional.empty());
        AtomicInteger calls = new AtomicInteger(0);
        calc.setCalendarLookup((calendarId, from, to) -> {
            calls.incrementAndGet();
            return Set.of();
        });
        calc.resolveExpectedSettlementDate(LocalDate.of(2026, 5, 20), "AAPL");
        assertThat(calls.get()).isZero();
    }
}
