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

/** Gap plan §10.A MCP read endpoints. */
class SettlementMcpReadIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void correlateExecution_returnsEvidencePackAndProfile() {
        long executionId = seedTradeExecution();

        ResponseEntity<SettlementCorrelateExecutionResponse> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/settlement/executions/" + executionId + "/correlate",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                SettlementCorrelateExecutionResponse.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().executionId()).isEqualTo(executionId);
        assertThat(res.getBody().evidencePack().execution().id()).isEqualTo(executionId);
    }

    @Test
    void getBreakNarrative_returnsSummaryForCashMismatch() {
        long executionId = seedTradeExecution();
        jdbc.update(
                """
                        INSERT INTO reconciliation_breaks (
                          break_type, severity, source_system, execution_id, account_id,
                          diff_json, status, opened_by
                        ) VALUES (
                          'cash_mismatch', 'high', 'broker', ?, ?, CAST(? AS JSONB), 'open', 'it'
                        )
                        """,
                executionId,
                UUID.randomUUID(),
                "{\"reason\":\"currency mismatch\",\"brokerCurrency\":\"USD\",\"omsCurrency\":\"SEK\"}");

        long breakId =
                jdbc.queryForObject("SELECT id FROM reconciliation_breaks ORDER BY id DESC LIMIT 1", Long.class);

        ResponseEntity<SettlementBreakNarrativeResponse> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/settlement/reconciliation-breaks/" + breakId + "/narrative",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                SettlementBreakNarrativeResponse.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().summary()).containsIgnoringCase("currency");
        assertThat(res.getBody().recommendedAction()).isNotBlank();
    }

    @Test
    void listReconciliationBreaks_filtersByAccountId() {
        UUID accountA = UUID.randomUUID();
        UUID accountB = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO reconciliation_breaks (
                          break_type, severity, source_system, account_id,
                          diff_json, status, opened_by
                        ) VALUES (
                          'cash_mismatch', 'high', 'broker', ?, '{}'::jsonb, 'open', 'it'
                        )
                        """,
                accountA);
        jdbc.update(
                """
                        INSERT INTO reconciliation_breaks (
                          break_type, severity, source_system, account_id,
                          diff_json, status, opened_by
                        ) VALUES (
                          'cash_mismatch', 'high', 'broker', ?, '{}'::jsonb, 'open', 'it'
                        )
                        """,
                accountB);

        ResponseEntity<SettlementController.ReconciliationBreaksListResponse> res = http.exchange(
                "http://localhost:"
                        + port
                        + "/internal/v1/settlement/reconciliation-breaks?accountId="
                        + accountA,
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                SettlementController.ReconciliationBreaksListResponse.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).hasSize(1);
        assertThat(res.getBody().items().getFirst().accountId()).isEqualTo(accountA);
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
                "it-key-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, settlement_status, trade_date, expected_settlement_date
                        ) VALUES (
                          ?, ?, 'loopback', NOW(), 'it-venue-ref', 10, 5, 0, 10,
                          'TRADE', 'executed', CURRENT_DATE, CURRENT_DATE + 2
                        )
                        """,
                orderId,
                accountId);
        return jdbc.queryForObject("SELECT id FROM executions ORDER BY id DESC LIMIT 1", Long.class);
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }
}
