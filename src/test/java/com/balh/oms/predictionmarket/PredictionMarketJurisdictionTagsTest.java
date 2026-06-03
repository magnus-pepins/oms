package com.balh.oms.predictionmarket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class PredictionMarketJurisdictionTagsTest {

    @Test
    void normalize_acceptsIso2AndDedupes() {
        assertThat(PredictionMarketJurisdictionTags.normalize(List.of("se", "SE", "lv")))
                .containsExactly("SE", "LV");
    }

    @Test
    void normalize_rejectsInvalidCode() {
        assertThatThrownBy(() -> PredictionMarketJurisdictionTags.normalize(List.of("SWE")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
