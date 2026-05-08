package com.balh.oms.settlement;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderAggregateSettlementStatusTest {

    @Test
    void emptyOrNull_returnsNull() {
        assertThat(OrderAggregateSettlementStatus.summarize(null)).isNull();
        assertThat(OrderAggregateSettlementStatus.summarize(List.of())).isNull();
    }

    @Test
    void failedDominates() {
        assertThat(OrderAggregateSettlementStatus.summarize(List.of("settled", "FAILED", "executed")))
                .isEqualTo("failed");
    }

    @Test
    void allSettled() {
        assertThat(OrderAggregateSettlementStatus.summarize(List.of("settled", "settled"))).isEqualTo("settled");
    }

    @Test
    void picksHighestNonSettled() {
        assertThat(OrderAggregateSettlementStatus.summarize(List.of("executed", "settling"))).isEqualTo("settling");
        assertThat(OrderAggregateSettlementStatus.summarize(List.of("matched", "confirmed"))).isEqualTo("confirmed");
    }

    @Test
    void settledAndExecuted_returnsExecuted() {
        assertThat(OrderAggregateSettlementStatus.summarize(List.of("settled", "executed"))).isEqualTo("executed");
    }
}
