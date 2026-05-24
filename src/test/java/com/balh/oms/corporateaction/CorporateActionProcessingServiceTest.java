package com.balh.oms.corporateaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class CorporateActionProcessingServiceTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void reverseSplitAlias_normalizesToStockSplitRatio() throws Exception {
        var payload = JSON.readTree("{\"newShares\":\"1\",\"oldShares\":\"3\",\"cashInLieuPerShare\":\"2.50\"}");
        assertThat(CorporateActionProcessingService.splitRatio(payload))
                .isEqualByComparingTo("0.3333333333");
    }

    @Test
    void stockDividendRatio_addsSharesPerShare() throws Exception {
        var payload = JSON.readTree("{\"sharesPerShare\":\"0.10\"}");
        assertThat(CorporateActionProcessingService.stockDividendRatio(payload))
                .isEqualByComparingTo("1.10");
    }
}
