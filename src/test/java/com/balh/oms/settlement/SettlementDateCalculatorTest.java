package com.balh.oms.settlement;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

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
}
