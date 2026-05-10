package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementFileImportIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void fileImport_registersBrokerConfirms() {
        long exId = seedTradeExecution();
        String json =
                """
                        {"rows":[{"executionId":%d,"accountId":null,"venueExecRef":null}]}
                        """
                        .formatted(exId);
        ResponseEntity<SettlementController.SettlementFileImportResponse> res = postFile(json);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().duplicate()).isFalse();
        assertThat(res.getBody().status()).isEqualTo("applied");
        assertThat(res.getBody().insertedConfirms()).isEqualTo(1);
        assertThat(res.getBody().skippedInvalidRows()).isZero();
        assertThat(res.getBody().skippedUnresolvedRows()).isZero();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ?",
                        Integer.class,
                        exId))
                .isEqualTo(1);
    }

    @Test
    void fileImport_duplicateSameBytes_isIdempotent() {
        long exId = seedTradeExecution();
        String json =
                """
                        {"rows":[{"executionId":%d,"accountId":null,"venueExecRef":null}]}
                        """
                        .formatted(exId);
        ResponseEntity<SettlementController.SettlementFileImportResponse> first = postFile(json);
        assertThat(first.getBody()).isNotNull();
        assertThat(first.getBody().duplicate()).isFalse();
        long batchId = first.getBody().batchId();

        ResponseEntity<SettlementController.SettlementFileImportResponse> second = postFile(json);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().duplicate()).isTrue();
        assertThat(second.getBody().batchId()).isEqualTo(batchId);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ?",
                        Integer.class,
                        exId))
                .isEqualTo(1);
    }

    @Test
    void fileImport_invalidJson_marksBatchFailed() {
        ResponseEntity<SettlementController.SettlementFileImportResponse> res = postFile("not json {");
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().duplicate()).isFalse();
        assertThat(res.getBody().status()).isEqualTo("failed");
        assertThat(res.getBody().errorSummary()).isNotBlank();
        assertThat(jdbc.queryForObject(
                        "SELECT status FROM settlement_file_import_batch WHERE id = ?",
                        String.class,
                        res.getBody().batchId()))
                .isEqualTo("failed");
    }

    @Test
    void fileImport_unknownExecutionId_appliesBatchWithSkippedUnresolved_noConfirmInserted() {
        long bogusExecutionId = 9_999_999_999L;
        String json =
                """
                        {"rows":[{"executionId":%d,"accountId":null,"venueExecRef":null}]}
                        """
                        .formatted(bogusExecutionId);
        ResponseEntity<SettlementController.SettlementFileImportResponse> res = postFile(json);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().duplicate()).isFalse();
        assertThat(res.getBody().status()).isEqualTo("applied");
        assertThat(res.getBody().insertedConfirms()).isZero();
        assertThat(res.getBody().skippedUnresolvedRows()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ?",
                        Integer.class,
                        bogusExecutionId))
                .isZero();
    }

    @Test
    void fileImport_listBatches_returnsRows() {
        long exId = seedTradeExecution();
        String json =
                """
                        {"rows":[{"executionId":%d,"accountId":null,"venueExecRef":null}]}
                        """
                        .formatted(exId);
        assertThat(postFile(json).getStatusCode()).isEqualTo(HttpStatus.OK);

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<SettlementController.SettlementFileImportBatchesResponse> list =
                http.exchange(
                        base() + "/file-import-batches?limit=10&offset=0",
                        HttpMethod.GET,
                        new HttpEntity<>(h),
                        new ParameterizedTypeReference<>() {});
        assertThat(list.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(list.getBody()).isNotNull();
        assertThat(list.getBody().items()).hasSize(1);
        assertThat(list.getBody().items().getFirst().status()).isEqualTo("applied");
    }

    private ResponseEntity<SettlementController.SettlementFileImportResponse> postFile(String jsonBytes) {
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("source", "it-eod");
        body.add(
                "file",
                new ByteArrayResource(jsonBytes.getBytes(StandardCharsets.UTF_8)) {
                    @Override
                    public String getFilename() {
                        return "broker.json";
                    }
                });
        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        return http.exchange(
                base() + "/file-import",
                HttpMethod.POST,
                new HttpEntity<>(body, h),
                new ParameterizedTypeReference<>() {});
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
                "file-import-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          10, 5, 0, 10,
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB)
                        )
                        """,
                orderId,
                accountId,
                "vref-fi-" + orderId);
        long exId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', CAST(? AS UUID), 10, 0, 10, 0)
                        """,
                accountId,
                "a0000001-0000-4000-8000-000000000001");
        return exId;
    }

    private String base() {
        return "http://localhost:" + port + "/internal/v1/settlement";
    }
}
