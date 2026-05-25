package com.balh.oms.corporateaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CorporateActionWithholdingResolverTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final UUID ACCOUNT = UUID.fromString("ee69e1be-c1f1-4dfa-b3e8-2ecd9fb90970");

    @Mock AccountTaxResidencyRepository taxResidency;

    @Test
    void resolve_usesTreatyRateWhenCountryMatches() throws Exception {
        when(taxResidency.findTaxCountry(ACCOUNT)).thenReturn(Optional.of("SE"));
        var payload =
                JSON.readTree(
                        "{\"withholdingRates\":{\"SE\":\"0.15\",\"US\":\"0.30\"},\"withholdingRate\":\"0.30\"}");
        var resolver = new CorporateActionWithholdingResolver(taxResidency);
        assertThat(resolver.resolve(new BigDecimal("100.00"), payload, ACCOUNT))
                .isEqualByComparingTo("15.00");
    }

    @Test
    void resolve_fallsBackToFlatWithholdingRate() throws Exception {
        var payload = JSON.readTree("{\"withholdingRate\":\"0.25\"}");
        var resolver = new CorporateActionWithholdingResolver(taxResidency);
        assertThat(resolver.resolve(new BigDecimal("10.00"), payload, ACCOUNT))
                .isEqualByComparingTo("2.50");
    }
}
