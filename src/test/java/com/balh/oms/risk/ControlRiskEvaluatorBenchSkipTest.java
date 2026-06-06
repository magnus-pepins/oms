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

class ControlRiskEvaluatorBenchSkipTest {

    @AfterEach
    void tearDown() {
        ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(null);
    }

    @Test
    void shouldSkip_whenBenchFlagLedgerBalanceAndPassAuditSkip() {
        ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(true);
        Order order = predmktOrder("bal-1");

        assertThat(ControlRiskEvaluator.shouldSkipVenueBenchRiskEval(true, order)).isTrue();
    }

    @Test
    void shouldNotSkip_withoutLedgerBalanceId() {
        ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(true);
        Order order = predmktOrder(null);

        assertThat(ControlRiskEvaluator.shouldSkipVenueBenchRiskEval(true, order)).isFalse();
    }

    @Test
    void shouldNotSkip_whenPassAuditSkipDisabled() {
        ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(true);
        Order order = predmktOrder("bal-2");

        assertThat(ControlRiskEvaluator.shouldSkipVenueBenchRiskEval(false, order)).isFalse();
    }

    @Test
    void shouldNotSkip_whenRiskEvalGateDisabled() {
        ControlRiskEvaluator.setSkipVenueControlRiskEvalForTesting(false);
        Order order = predmktOrder("bal-3");

        assertThat(ControlRiskEvaluator.shouldSkipVenueBenchRiskEval(true, order)).isFalse();
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
