package com.balh.oms.predictionmarket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PredictionMarketContractServiceTest {

    @Test
    void normalizeCurrency_defaultsToUsd() {
        assertEquals("USD", PredictionMarketContractService.normalizeCurrency(null));
        assertEquals("EUR", PredictionMarketContractService.normalizeCurrency("eur"));
    }

    @Test
    void normalizeCurrency_rejectsInvalid() {
        assertThrows(
                IllegalArgumentException.class,
                () -> PredictionMarketContractService.normalizeCurrency("USDD"));
    }
}
