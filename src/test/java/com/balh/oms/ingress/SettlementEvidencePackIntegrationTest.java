package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/** Phase C tail (gap plan §5.17): execution evidence pack. */
class SettlementEvidencePackIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void getEvidencePack_returnsExecutionTimelineAndV1Confirm() {
        long executionId = seedTradeExecution();
        jdbc.update("INSERT INTO broker_settlement_confirm (execution_id) VALUES (?)", executionId);

        ResponseEntity<SettlementEvidencePackResponse> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/settlement/executions/" + executionId + "/evidence-pack",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                SettlementEvidencePackResponse.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().executionId()).isEqualTo(executionId);
        assertThat(res.getBody().execution().rawEnvelopeJson()).contains("evidence");
        assertThat(res.getBody().timeline().executionId()).isEqualTo(executionId);
        assertThat(res.getBody().timeline().phases()).isNotEmpty();
        assertThat(res.getBody().v1BrokerConfirm()).isNotNull();
        assertThat(res.getBody().brokerTradeConfirms()).isEmpty();
    }

    @Test
    void getEvidencePack_unknownExecution_returns404() {
        ResponseEntity<SettlementEvidencePackResponse> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/settlement/executions/999999999/evidence-pack",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                SettlementEvidencePackResponse.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private long seedTradeExecution() {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', 'BUY', 'AAPL', 10, 5, 'DAY',
                          NOW(), NOW(), 'h', NULL, 10
                        )
                        """,
                orderId,
                accountId,
                "evidence-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, settlement_status
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), 'EX-EVIDENCE-1',
                          10, 5, 0, 10,
                          CAST('TRADE' AS execution_exec_type), CAST('{"evidence":true}' AS JSONB),
                          CAST('executed' AS execution_settlement_status)
                        )
                        """,
                orderId,
                accountId);
        return jdbc.queryForObject("SELECT id FROM executions WHERE order_id = ?", Long.class, orderId);
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }
}
