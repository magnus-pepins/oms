package com.balh.oms.corporateaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CorporateActionCostBasisAllocatorTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void merger_proportionalShares_splitsCostByQuantityRatio() throws Exception {
        var payload = JSON.readTree("{\"costBasisAllocationMethod\":\"PROPORTIONAL_SHARES\"}");
        var split =
                CorporateActionCostBasisAllocator.merger(
                        new BigDecimal("1000.00"),
                        new BigDecimal("100"),
                        new BigDecimal("50"),
                        payload);
        assertThat(split.childCostAfter()).isEqualByComparingTo("500.00");
        assertThat(split.parentCostAfter()).isEqualByComparingTo("500.00");
    }

    @Test
    void spinOff_allocatesChildFraction() throws Exception {
        var payload = JSON.readTree("{\"spinOffCostBasisFraction\":\"0.25\"}");
        var split =
                CorporateActionCostBasisAllocator.spinOff(
                        new BigDecimal("800.00"),
                        new BigDecimal("0.75"),
                        new BigDecimal("0.25"),
                        payload);
        assertThat(split.childCostAfter()).isEqualByComparingTo("200.00");
        assertThat(split.parentCostAfter()).isEqualByComparingTo("600.00");
    }
}
