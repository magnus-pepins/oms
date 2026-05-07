package com.balh.oms.routing;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class SimulatedFillEngineSplitTest {

    @Test
    void splitInThirdsSumsToOriginalQuantity() {
        BigDecimal q = new BigDecimal("100");
        var parts = SimulatedFillEngine.splitInThirds(q);
        assertThat(parts).hasSize(3);
        BigDecimal sum = parts.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        assertThat(sum).isEqualByComparingTo(q);
    }
}
