package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementBrokerFileFormatTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void detect_v0Fixture_byExecutionId() {
        byte[] bytes = """
                {"rows":[{"executionId":42}]}
                """
                .getBytes(StandardCharsets.UTF_8);
        assertThat(SettlementBrokerFileFormat.detect(objectMapper, bytes))
                .isEqualTo(SettlementBrokerFileFormat.Kind.V0_FIXTURE);
    }

    @Test
    void detect_v0Fixture_byAccountVenueRef() {
        byte[] bytes = """
                {"rows":[{"accountId":"%s","venueExecRef":"EX-1"}]}
                """
                .formatted(UUID.randomUUID())
                .getBytes(StandardCharsets.UTF_8);
        assertThat(SettlementBrokerFileFormat.detect(objectMapper, bytes))
                .isEqualTo(SettlementBrokerFileFormat.Kind.V0_FIXTURE);
    }

    @Test
    void detect_v2Economic_byEnvelopeAndRowShape() {
        byte[] bytes = """
                {
                  "schemaVersion": 1,
                  "brokerId": "broker_x",
                  "fileId": "F-1",
                  "businessDate": "2026-05-23",
                  "rows": [
                    {
                      "brokerTradeId": "BT-1",
                      "venueExecRef": "EX-1",
                      "instrument": { "symbol": "AAPL", "currency": "USD" },
                      "side": "BUY",
                      "quantity": "10",
                      "price": "5"
                    }
                  ]
                }
                """
                .getBytes(StandardCharsets.UTF_8);
        assertThat(SettlementBrokerFileFormat.detect(objectMapper, bytes))
                .isEqualTo(SettlementBrokerFileFormat.Kind.V2_ECONOMIC);
    }

    @Test
    void detect_invalid_whenEmptyOrUnrecognized() {
        assertThat(SettlementBrokerFileFormat.detect(objectMapper, new byte[0]))
                .isEqualTo(SettlementBrokerFileFormat.Kind.INVALID);
        assertThat(SettlementBrokerFileFormat.detect(objectMapper, "{\"rows\":[]}".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo(SettlementBrokerFileFormat.Kind.INVALID);
        assertThat(SettlementBrokerFileFormat.detect(
                        objectMapper, "{\"rows\":[{\"foo\":\"bar\"}]}".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo(SettlementBrokerFileFormat.Kind.INVALID);
    }
}
