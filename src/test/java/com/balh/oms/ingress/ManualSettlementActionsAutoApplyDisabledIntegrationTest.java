package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.settlement.ManualSettlementActionTypes;
import java.util.UUID;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When {@code oms.settlement.manual-action-auto-apply-enabled=false}, approve records four-eyes only
 * and does not run {@link ManualSettlementActionTypes} handlers.
 */
class ManualSettlementActionsAutoApplyDisabledIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @DynamicPropertySource
    static void disableManualActionAutoApply(DynamicPropertyRegistry registry) {
        registry.add("oms.settlement.manual-action-auto-apply-enabled", () -> "false");
    }

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void approve_advanceSettlement_doesNotAdvanceWhenAutoApplyDisabled() {
        long exId = seedTradeExecution();
        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        exId))
                .isEqualTo("executed");

        HttpHeaders h = headers();
        var createBody =
                new ManualSettlementActionsController.CreateManualSettlementActionRequest(
                        exId,
                        ManualSettlementActionTypes.ADVANCE_SETTLEMENT_ONE_STEP,
                        "alice@example.com",
                        "{}");
        ResponseEntity<ManualSettlementActionResponse> created =
                http.exchange(
                        urlBase(),
                        HttpMethod.POST,
                        new HttpEntity<>(createBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody()).isNotNull();
        long actionId = created.getBody().id();

        var approveBody = new ManualSettlementActionsController.ApproveManualSettlementActionRequest("bob@example.com");
        ResponseEntity<ManualSettlementActionResponse> approved =
                http.exchange(
                        urlBase() + "/" + actionId + "/approve",
                        HttpMethod.POST,
                        new HttpEntity<>(approveBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(approved.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(approved.getBody()).isNotNull();
        assertThat(approved.getBody().approvedBy()).isEqualTo("bob@example.com");

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        exId))
                .isEqualTo("executed");
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
                "msa-auto-off-" + orderId);
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
                "vref-msa-auto-off-" + orderId);
        return jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
    }

    private String urlBase() {
        return "http://localhost:" + port + "/internal/v1/settlement/manual-actions";
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }
}
