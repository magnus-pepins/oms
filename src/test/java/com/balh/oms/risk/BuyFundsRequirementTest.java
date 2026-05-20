package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BuyFundsRequirementTest {

    @Test
    void requiredBuyFundsIncludesFee() {
        OmsConfig cfg = new OmsConfig();
        Order order = buyOrder(new BigDecimal("10"), new BigDecimal("100"));
        var required = BuyFundsRequirement.requiredBuyFunds(order, cfg);
        assertThat(required).isPresent();
        // US default: 0.25% of 1000 = 2.50, min $1 → fee $2.50; total 1002.50
        assertThat(required.get()).isEqualByComparingTo("1002.50");
    }

    @Test
    void marketBuyWithoutReferencePriceIsEmpty() {
        OmsConfig cfg = new OmsConfig();
        Order order = buyOrder(new BigDecimal("1"), null);
        assertThat(BuyFundsRequirement.requiredBuyFunds(order, cfg)).isEmpty();
        assertThat(BuyFundsRequirement.hasBuyFundingPrice(order)).isFalse();
    }

    private static Order buyOrder(BigDecimal qty, BigDecimal limit) {
        Instant now = Instant.now();
        return new Order(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "k",
                0,
                0,
                OrderStatus.NEW,
                null,
                Side.BUY,
                "AAPL",
                qty,
                limit,
                "DAY",
                now,
                now,
                null,
                "hash",
                "balance_x",
                BigDecimal.ZERO,
                "MARKET");
    }
}
