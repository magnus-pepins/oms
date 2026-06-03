package com.balh.oms.predictionmarket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class PredictionMarketReferenceLinksTest {

    @Test
    void normalize_acceptsHttpsLinks() {
        var links =
                PredictionMarketReferenceLinks.normalize(
                        List.of(new PredictionMarketReferenceLinks.Link("ZeroHedge", "https://zerohedge.com/x")));
        assertThat(links).hasSize(1);
        assertThat(links.getFirst().label()).isEqualTo("ZeroHedge");
    }

    @Test
    void normalize_rejectsHttp() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        PredictionMarketReferenceLinks.normalize(
                                List.of(
                                        new PredictionMarketReferenceLinks.Link(
                                                "Bad", "http://example.com"))));
    }

    @Test
    void toJson_roundTrips() {
        var links = List.of(new PredictionMarketReferenceLinks.Link("A", "https://a.test"));
        String json = PredictionMarketReferenceLinks.toJson(links);
        assertThat(PredictionMarketReferenceLinks.fromJson(json)).isEqualTo(links);
    }
}
