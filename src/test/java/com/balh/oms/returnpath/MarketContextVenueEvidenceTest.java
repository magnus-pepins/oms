package com.balh.oms.returnpath;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MarketContextVenueEvidenceTest {

    @Test
    void toJsonPatch_containsVenueFields() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        UUID id = UUID.randomUUID();
        UUID account = UUID.randomUUID();
        Instant received = Instant.parse("2026-05-07T10:00:00Z");
        Instant accepted = Instant.parse("2026-05-07T10:01:00Z");
        Order order = new Order(
                id,
                account,
                "k",
                0,
                1,
                OrderStatus.WORKING,
                null,
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                new BigDecimal("5.25"),
                "DAY",
                received,
                accepted,
                null,
                "h",
                null,
                BigDecimal.ZERO);
        Instant ts = Instant.parse("2026-05-07T12:00:00Z");
        var cmd = new ExecutionTradeCommand(id, "FIX", ts, "exec-1", new BigDecimal("4"), new BigDecimal("5.25"),
                new BigDecimal("6"), new BigDecimal("4"));
        String json = MarketContextVenueEvidence.toJsonPatch(mapper, order, cmd);
        var tree = mapper.readTree(json);
        assertThat(tree.path("schemaVersion").asInt()).isEqualTo(MarketContextVenueEvidence.SCHEMA_VERSION);
        assertThat(tree.path("evidenceSource").asText()).isEqualTo("venue_execution_report");
        assertThat(tree.path("instrumentSymbol").asText()).isEqualTo("AAPL");
        assertThat(tree.path("venueExecRef").asText()).isEqualTo("exec-1");
    }

    @Test
    void toJsonPatch_withNbboOptional_addsNbboClassReference() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        UUID id = UUID.randomUUID();
        UUID account = UUID.randomUUID();
        Instant received = Instant.parse("2026-05-07T10:00:00Z");
        Instant accepted = Instant.parse("2026-05-07T10:01:00Z");
        Order order = new Order(
                id,
                account,
                "k",
                0,
                1,
                OrderStatus.WORKING,
                null,
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                new BigDecimal("5.25"),
                "DAY",
                received,
                accepted,
                null,
                "h",
                null,
                BigDecimal.ZERO);
        Instant ts = Instant.parse("2026-05-07T12:00:00Z");
        var cmd = new ExecutionTradeCommand(id, "FIX", ts, "exec-1", new BigDecimal("4"), new BigDecimal("5.25"),
                new BigDecimal("6"), new BigDecimal("4"));
        var nbbo = new MarketContextVenueEvidence.NbboQuoteRef(new BigDecimal("5.24"), new BigDecimal("5.26"), ts);
        String json = MarketContextVenueEvidence.toJsonPatch(mapper, order, cmd, Optional.of(nbbo));
        var tree = mapper.readTree(json);
        assertThat(tree.path("nbboClassReference").path("bid").asText()).isEqualTo("5.2400000000");
        assertThat(tree.path("nbboClassReference").path("ask").asText()).isEqualTo("5.2600000000");
        assertThat(tree.path("nbboClassReference").path("quoteClass").asText()).isEqualTo("NBBO_STUB");
    }
}
