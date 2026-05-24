package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class SettlementBrokerFileFormatTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void detect_cashStatement() throws Exception {
        byte[] bytes =
                """
                        {"schemaVersion":1,"brokerId":"b","fileId":"f","currency":"SEK",
                         "movements":[{"brokerMovementId":"m1","amount":"1"}]}
                        """
                        .getBytes(StandardCharsets.UTF_8);
        assertThat(SettlementBrokerFileFormat.detect(JSON, bytes))
                .isEqualTo(SettlementBrokerFileFormat.Kind.CASH_STATEMENT);
    }

    @Test
    void detect_positionSnapshot() throws Exception {
        byte[] bytes =
                """
                        {"schemaVersion":1,"brokerId":"b","fileId":"f","rows":[{"quantityTotal":"10"}]}
                        """
                        .getBytes(StandardCharsets.UTF_8);
        assertThat(SettlementBrokerFileFormat.detect(JSON, bytes))
                .isEqualTo(SettlementBrokerFileFormat.Kind.POSITION_SNAPSHOT);
    }

    @Test
    void detect_corporateAction() throws Exception {
        byte[] bytes =
                """
                        {"schemaVersion":1,"brokerId":"b","fileId":"f","events":[{"brokerEventId":"e1"}]}
                        """
                        .getBytes(StandardCharsets.UTF_8);
        assertThat(SettlementBrokerFileFormat.detect(JSON, bytes))
                .isEqualTo(SettlementBrokerFileFormat.Kind.CORPORATE_ACTION);
    }
}
