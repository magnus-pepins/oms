package com.balh.oms.predictionmarket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class PredictionMarketCatalogPresentationTest {

    @Test
    void normalizeCategory_allowsNull() {
        assertNull(PredictionMarketCatalogPresentation.normalizeCategory(null));
        assertNull(PredictionMarketCatalogPresentation.normalizeCategory("  "));
    }

    @Test
    void normalizeCardImageUrl_requiresHttps() {
        assertEquals(
                "https://cdn.example.com/x.png",
                PredictionMarketCatalogPresentation.normalizeCardImageUrl(
                        "https://cdn.example.com/x.png"));
        assertThrows(
                IllegalArgumentException.class,
                () -> PredictionMarketCatalogPresentation.normalizeCardImageUrl("http://x.com/a.png"));
    }

    @Test
    void normalizeTags_dedupesAndLowercases() {
        assertEquals(
                List.of("election", "eu"),
                PredictionMarketCatalogTags.normalize(List.of("Election", "eu", "election")));
    }
}
