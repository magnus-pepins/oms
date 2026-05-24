package com.balh.oms.settlement;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerTradeConfirmMatcherTest {

    @Test
    void grossAmountWithinTolerance_acceptsPennyDrift() {
        assertThat(BrokerTradeConfirmMatcher.grossAmountWithinTolerance(
                        new BigDecimal("55.01"), new BigDecimal("55.00"), new BigDecimal("0.01")))
                .isTrue();
    }

    @Test
    void grossAmountWithinTolerance_rejectsLargeDrift() {
        assertThat(BrokerTradeConfirmMatcher.grossAmountWithinTolerance(
                        new BigDecimal("56.00"), new BigDecimal("55.00"), new BigDecimal("0.01")))
                .isFalse();
    }
}
