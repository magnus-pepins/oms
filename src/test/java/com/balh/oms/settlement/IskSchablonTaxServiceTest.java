package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IskSchablonTaxServiceTest {

    @Test
    void schablonRate_usesFloorWhenStatslanerantaLow() {
        var jdbc = org.mockito.Mockito.mock(org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate.class);
        var repo = new IskTaxParametersRepository(jdbc);
        var config = new OmsConfig();
        var service = new IskSchablonTaxService(repo, config);

        assertThat(service.schablonRateForYear(2099)).isEqualByComparingTo("0.0288");
    }

    @Test
    void schablonintakt_multipliesKapitalunderlagByRate() {
        var jdbc = org.mockito.Mockito.mock(org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate.class);
        var repo = new IskTaxParametersRepository(jdbc);
        var config = new OmsConfig();
        var service = new IskSchablonTaxService(repo, config);

        assertThat(service.schablonintakt(new BigDecimal("100000.00"), 2099))
                .isEqualByComparingTo("2880.00");
    }

    @Test
    void deriveSchablonRate_matchesIlFormula() {
        assertThat(IskTaxParametersRepository.deriveSchablonRate(new BigDecimal("0.0188")))
                .isEqualByComparingTo("0.0288");
        assertThat(IskTaxParametersRepository.deriveSchablonRate(new BigDecimal("0.0001")))
                .isEqualByComparingTo("0.0125");
    }
}
