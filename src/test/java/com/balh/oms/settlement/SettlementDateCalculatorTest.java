package com.balh.oms.settlement;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;
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
}
