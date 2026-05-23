package com.balh.oms.fx;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class FxPairCatalogTest {

    @Test
    void inversePair_swapsSixLetterPair() {
        assertThat(FxPairCatalog.inversePair("USDAUD")).isEqualTo("AUDUSD");
        assertThat(FxPairCatalog.inversePair("AUDUSD")).isEqualTo("USDAUD");
    }

    @Test
    void invertVendorBbo_swapsReciprocalLegs() {
        FxBboMarkup.VendorBbo inverted = FxPairCatalog.invertVendorBbo(
                new BigDecimal("0.71236"), new BigDecimal("0.71243"));
        assertThat(inverted.bid()).isGreaterThan(new BigDecimal("1.40"));
        assertThat(inverted.offer()).isGreaterThan(inverted.bid());
    }
}
