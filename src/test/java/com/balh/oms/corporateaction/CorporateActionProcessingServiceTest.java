package com.balh.oms.corporateaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CorporateActionProcessingServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void splitRatio_fromNewAndOldShares() throws Exception {
        var payload = objectMapper.readTree("{\"newShares\": \"2\", \"oldShares\": \"1\"}");
        assertThat(CorporateActionProcessingService.splitRatio(payload))
                .isEqualByComparingTo("2");
    }

    @Test
    void parseBrokerDates_readsPayableDate() {
        var dates =
                CorporateActionProcessorJob.parseBrokerDates(
                        "{\"payableDate\":\"2026-06-15\",\"recordDate\":\"2026-06-01\"}", objectMapper);
        assertThat(dates.payableDate()).hasToString("2026-06-15");
        assertThat(dates.recordDate()).hasToString("2026-06-01");
    }

    @Test
    void withholdingAmount_appliesRate() throws Exception {
        var payload = objectMapper.readTree("{\"withholdingRate\": \"0.15\"}");
        assertThat(CorporateActionProcessingService.withholdingAmount(new java.math.BigDecimal("100.00"), payload))
                .isEqualByComparingTo("15.00");
    }

    @Test
    void stockDividendRatio_fromSharesPerShare() throws Exception {
        var payload = objectMapper.readTree("{\"sharesPerShare\": \"0.1\"}");
        assertThat(CorporateActionProcessingService.stockDividendRatio(payload))
                .isEqualByComparingTo("1.1");
    }
}
