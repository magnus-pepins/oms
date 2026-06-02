package com.balh.oms.fx;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pure-function coverage for the exposure-book routing helpers on
 * {@link FxHedgeService}. The full leg-construction path is exercised by
 * {@link FxHedgeServiceAtomicIntegrationTest}; these tests pin the
 * indicator each book resolves to so a future refactor can't silently
 * point a retail hedge at the suspense book (or vice versa).
 */
class FxHedgeServiceExposureTest {

    @Test
    void normalizeExposure_defaultsToSuspense() {
        assertThat(FxHedgeService.normalizeExposure(null)).isEqualTo("suspense");
        assertThat(FxHedgeService.normalizeExposure("")).isEqualTo("suspense");
        assertThat(FxHedgeService.normalizeExposure("  ")).isEqualTo("suspense");
        assertThat(FxHedgeService.normalizeExposure("SUSPENSE")).isEqualTo("suspense");
    }

    @Test
    void normalizeExposure_acceptsRetail() {
        assertThat(FxHedgeService.normalizeExposure("retail")).isEqualTo("retail");
        assertThat(FxHedgeService.normalizeExposure(" Retail ")).isEqualTo("retail");
    }

    @Test
    void normalizeExposure_rejectsUnknown() {
        assertThatThrownBy(() -> FxHedgeService.normalizeExposure("bank"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exposure");
    }

    @Test
    void exposureIndicatorPrefix_mapsBooksToDistinctIndicators() {
        assertThat(FxHedgeService.exposureIndicatorPrefix("suspense")).isEqualTo("@FX-Suspense-");
        assertThat(FxHedgeService.exposureIndicatorPrefix("retail")).isEqualTo("@Nostro-");
        // The retail prefix must never collide with the -Bank inventory book.
        assertThat(FxHedgeService.exposureIndicatorPrefix("retail") + "GBP").isEqualTo("@Nostro-GBP");
    }
}
