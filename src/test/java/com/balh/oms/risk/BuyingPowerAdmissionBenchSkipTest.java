package com.balh.oms.risk;

import static org.assertj.core.api.Assertions.assertThat;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BuyingPowerAdmissionBenchSkipTest {

    @AfterEach
    void tearDown() {
        BuyingPowerAdmission.setSkipVenueControlBuyingPowerEvalForTesting(null);
    }

    @Test
    void shouldSkip_whenBenchFlagLedgerBalanceAndPassAuditSkip() {
        BuyingPowerAdmission.setSkipVenueControlBuyingPowerEvalForTesting(true);
        Order order = predmktOrder("bal-1");

        assertThat(BuyingPowerAdmission.shouldSkipVenueBenchBuyingPowerEval(true, order)).isTrue();
    }

    @Test
    void shouldNotSkip_withoutLedgerBalanceId() {
        BuyingPowerAdmission.setSkipVenueControlBuyingPowerEvalForTesting(true);
        Order order = predmktOrder(null);

        assertThat(BuyingPowerAdmission.shouldSkipVenueBenchBuyingPowerEval(true, order)).isFalse();
    }

    @Test
    void shouldNotSkip_whenPassAuditSkipDisabled() {
        BuyingPowerAdmission.setSkipVenueControlBuyingPowerEvalForTesting(true);
        Order order = predmktOrder("bal-2");

        assertThat(BuyingPowerAdmission.shouldSkipVenueBenchBuyingPowerEval(false, order)).isFalse();
    }

    @Test
    void shouldNotSkip_whenBuyingPowerEvalGateDisabled() {
        BuyingPowerAdmission.setSkipVenueControlBuyingPowerEvalForTesting(false);
        Order order = predmktOrder("bal-3");

        assertThat(BuyingPowerAdmission.shouldSkipVenueBenchBuyingPowerEval(true, order)).isFalse();
    }

    private static Order predmktOrder(String ledgerBalanceId) {
        Instant now = Instant.parse("2026-06-06T12:00:00Z");
        return new Order(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "idem",
                0,
                0,
                OrderStatus.PENDING_NEW,
                null,
                Side.BUY,
                "PREDMKT-TEST-1",
                new BigDecimal("1"),
                new BigDecimal("0.50"),
                "DAY",
                now,
                now,
                null,
                "acct-hash",
                ledgerBalanceId,
                BigDecimal.ZERO);
    }
}
