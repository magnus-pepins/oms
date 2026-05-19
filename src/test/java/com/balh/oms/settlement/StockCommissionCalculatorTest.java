package com.balh.oms.settlement;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class StockCommissionCalculatorTest {

    @Test
    void usDefaultScheduleMatchesCustomerFrontendDefaults() {
        var s = StockCommissionCalculator.defaultScheduleFor("US");
        assertThat(s.market()).isEqualTo("US");
        assertThat(s.currency()).isEqualTo("USD");
        assertThat(s.feePercent()).isEqualByComparingTo("0.25");
        assertThat(s.minFee()).isEqualByComparingTo("1.00");
        assertThat(s.maxFee()).isEqualByComparingTo("50.00");
        assertThat(s.feeBalanceIndicator()).isEqualTo("@Fees-USD");
    }

    @Test
    void unknownMarketFallsBackToUs() {
        assertThat(StockCommissionCalculator.defaultScheduleFor("MX").market()).isEqualTo("US");
        assertThat(StockCommissionCalculator.defaultScheduleFor(null).market()).isEqualTo("US");
        assertThat(StockCommissionCalculator.defaultScheduleFor("").market()).isEqualTo("US");
    }

    @Test
    void notionalIsAlwaysPositive() {
        assertThat(StockCommissionCalculator.notional(new BigDecimal("10"), new BigDecimal("5.50")))
                .isEqualByComparingTo("55.00");
        // Defensive: negative quantity (should not happen for executions) folds to positive.
        assertThat(StockCommissionCalculator.notional(new BigDecimal("-3"), new BigDecimal("2")))
                .isEqualByComparingTo("6");
        assertThat(StockCommissionCalculator.notional(null, new BigDecimal("5"))).isEqualByComparingTo("0");
    }

    @Test
    void feeHitsMinFloorForSmallTrades() {
        var us = StockCommissionCalculator.defaultScheduleFor("US");
        // 0.25% of $10 = $0.025 → clamped up to $1 minimum
        assertThat(StockCommissionCalculator.feeFor(us, new BigDecimal("10")))
                .isEqualByComparingTo("1.00");
    }

    @Test
    void feeHitsMaxCeilingForLargeTrades() {
        var us = StockCommissionCalculator.defaultScheduleFor("US");
        // 0.25% of $1,000,000 = $2,500 → clamped down to $50 maximum
        assertThat(StockCommissionCalculator.feeFor(us, new BigDecimal("1000000")))
                .isEqualByComparingTo("50.00");
    }

    @Test
    void feeRoundsToCents() {
        var us = StockCommissionCalculator.defaultScheduleFor("US");
        // 0.25% of $1234.567 = $3.0864... rounded to $3.09
        assertThat(StockCommissionCalculator.feeFor(us, new BigDecimal("1234.567")))
                .isEqualByComparingTo("3.09");
    }

    @Test
    void zeroNotionalReturnsZero() {
        var us = StockCommissionCalculator.defaultScheduleFor("US");
        assertThat(StockCommissionCalculator.feeFor(us, BigDecimal.ZERO)).isEqualByComparingTo("0");
    }
}
