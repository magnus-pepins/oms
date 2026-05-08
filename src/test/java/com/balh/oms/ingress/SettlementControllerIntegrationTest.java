package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementControllerIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void rejectsWithoutInternalApiKey() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> res = http.postForEntity(
                base() + "/broker-confirms",
                new HttpEntity<>(new SettlementController.BrokerConfirmIngestRequest(List.of(1L)), h),
                String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void advanceOneStep_notFound_returns404() {
        HttpHeaders h = headers();
        ResponseEntity<SettlementController.SettlementStepResponse> res = http.exchange(
                base() + "/executions/999999999/advance-one-step",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void brokerConfirmsThenProcessPending_settlesSeededExecution() {
        long exId = seedTradeExecution();
        HttpHeaders h = headers();
        ResponseEntity<SettlementController.BrokerConfirmIngestResponse> ingest = http.exchange(
                base() + "/broker-confirms",
                HttpMethod.POST,
                new HttpEntity<>(new SettlementController.BrokerConfirmIngestRequest(List.of(exId)), h),
                new ParameterizedTypeReference<>() {});
        assertThat(ingest.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ingest.getBody()).isNotNull();
        assertThat(ingest.getBody().insertedRows()).isEqualTo(1);

        ResponseEntity<SettlementController.BrokerConfirmProcessResponse> proc = http.exchange(
                base() + "/process-pending",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(proc.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(proc.getBody()).isNotNull();
        assertThat(proc.getBody().processedRows()).isEqualTo(1);

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        exId))
                .isEqualTo("settled");
    }

    @Test
    void advanceOneStepRepeatedly_reachesSettled() {
        long exId = seedTradeExecution();
        HttpHeaders h = headers();
        String[] chain = {"matched", "confirmed", "settling", "settled"};
        for (String expected : chain) {
            ResponseEntity<SettlementController.SettlementStepResponse> res = http.exchange(
                    base() + "/executions/" + exId + "/advance-one-step",
                    HttpMethod.POST,
                    new HttpEntity<>(h),
                    new ParameterizedTypeReference<>() {});
            assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(res.getBody()).isNotNull();
            assertThat(res.getBody().settlementStatus()).isEqualTo(expected);
        }
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
                "ctl-it-" + orderId);
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
                "vref-" + orderId);
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

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }
}
