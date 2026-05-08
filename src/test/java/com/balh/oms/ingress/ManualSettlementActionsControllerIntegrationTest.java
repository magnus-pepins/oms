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

import static org.assertj.core.api.Assertions.assertThat;

class ManualSettlementActionsControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final UUID DEFAULT_CUSTODY =
            UUID.fromString("a0000001-0000-4000-8000-000000000001");

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
    void createThenListThenApprove() {
        long exId = seedTradeExecution();
        HttpHeaders h = headers();
        var createBody =
                new ManualSettlementActionsController.CreateManualSettlementActionRequest(
                        exId, "ADJUST_NOTE", "alice@example.com", "{\"note\":\"x\"}");
        ResponseEntity<ManualSettlementActionResponse> created =
                http.exchange(
                        base(),
                        HttpMethod.POST,
                        new HttpEntity<>(createBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody()).isNotNull();
        assertThat(created.getBody().executionId()).isEqualTo(exId);
        assertThat(created.getBody().requestedBy()).isEqualTo("alice@example.com");
        assertThat(created.getBody().approvedBy()).isNull();
        long actionId = created.getBody().id();

        ResponseEntity<ManualSettlementActionsPageResponse> listed =
                http.exchange(
                        base() + "?executionId=" + exId,
                        HttpMethod.GET,
                        new HttpEntity<>(h),
                        new ParameterizedTypeReference<>() {});
        assertThat(listed.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listed.getBody()).isNotNull();
        assertThat(listed.getBody().items()).hasSize(1);

        var approveBody = new ManualSettlementActionsController.ApproveManualSettlementActionRequest("bob@example.com");
        ResponseEntity<ManualSettlementActionResponse> approved =
                http.exchange(
                        base() + "/" + actionId + "/approve",
                        HttpMethod.POST,
                        new HttpEntity<>(approveBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(approved.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(approved.getBody()).isNotNull();
        assertThat(approved.getBody().approvedBy()).isEqualTo("bob@example.com");
    }

    @Test
    void approve_sameActor_returns400() {
        long exId = seedTradeExecution();
        HttpHeaders h = headers();
        var createBody =
                new ManualSettlementActionsController.CreateManualSettlementActionRequest(
                        exId, "X", "same@example.com", null);
        ResponseEntity<ManualSettlementActionResponse> created =
                http.exchange(
                        base(),
                        HttpMethod.POST,
                        new HttpEntity<>(createBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(created.getBody()).isNotNull();
        long actionId = created.getBody().id();
        var approveBody = new ManualSettlementActionsController.ApproveManualSettlementActionRequest("same@example.com");
        ResponseEntity<String> res =
                http.exchange(
                        base() + "/" + actionId + "/approve",
                        HttpMethod.POST,
                        new HttpEntity<>(approveBody, h),
                        String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void getById_returnsRow() {
        long exId = seedTradeExecution();
        HttpHeaders h = headers();
        var createBody =
                new ManualSettlementActionsController.CreateManualSettlementActionRequest(
                        exId, "GET_BY_ID", "viewer@example.com", "{}");
        ResponseEntity<ManualSettlementActionResponse> created =
                http.exchange(
                        base(),
                        HttpMethod.POST,
                        new HttpEntity<>(createBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(created.getBody()).isNotNull();
        long actionId = created.getBody().id();
        ResponseEntity<ManualSettlementActionResponse> got =
                http.exchange(
                        base() + "/" + actionId,
                        HttpMethod.GET,
                        new HttpEntity<>(h),
                        new ParameterizedTypeReference<>() {});
        assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(got.getBody()).isNotNull();
        assertThat(got.getBody().actionType()).isEqualTo("GET_BY_ID");
    }

    @Test
    void getById_notFound_returns404() {
        HttpHeaders h = headers();
        ResponseEntity<String> res =
                http.exchange(base() + "/999999999", HttpMethod.GET, new HttpEntity<>(h), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void create_unknownExecution_returns404() {
        HttpHeaders h = headers();
        var createBody =
                new ManualSettlementActionsController.CreateManualSettlementActionRequest(
                        999_999_999L, "X", "a@b.com", "{}");
        ResponseEntity<String> res =
                http.exchange(base(), HttpMethod.POST, new HttpEntity<>(createBody, h), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void approve_markTradeFailed_unwindsBuyPositionAndFailsExecution() {
        BuySeed seed = seedFilledBuyOrderWithExecution();
        HttpHeaders h = headers();
        var createBody =
                new ManualSettlementActionsController.CreateManualSettlementActionRequest(
                        seed.executionId(),
                        ManualSettlementActionTypes.MARK_TRADE_FAILED,
                        "alice@example.com",
                        "{}");
        ResponseEntity<ManualSettlementActionResponse> created =
                http.exchange(
                        base(),
                        HttpMethod.POST,
                        new HttpEntity<>(createBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody()).isNotNull();
        long actionId = created.getBody().id();

        var approveBody = new ManualSettlementActionsController.ApproveManualSettlementActionRequest("bob@example.com");
        ResponseEntity<ManualSettlementActionResponse> approved =
                http.exchange(
                        base() + "/" + actionId + "/approve",
                        HttpMethod.POST,
                        new HttpEntity<>(approveBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(approved.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(approved.getBody()).isNotNull();
        assertThat(approved.getBody().approvedBy()).isEqualTo("bob@example.com");

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        seed.executionId()))
                .isEqualTo("failed");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_total FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_pending_buy_settle FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM position_history WHERE account_id = ? AND event_type = 'MARK_FAILED_UNWIND_BUY'",
                        Integer.class,
                        seed.accountId()))
                .isEqualTo(1);
    }

    @Test
    void approve_advanceSettlementOneStep_movesExecutedToMatched() {
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
                        base(),
                        HttpMethod.POST,
                        new HttpEntity<>(createBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody()).isNotNull();
        long actionId = created.getBody().id();

        var approveBody = new ManualSettlementActionsController.ApproveManualSettlementActionRequest("bob@example.com");
        ResponseEntity<ManualSettlementActionResponse> approved =
                http.exchange(
                        base() + "/" + actionId + "/approve",
                        HttpMethod.POST,
                        new HttpEntity<>(approveBody, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(approved.getStatusCode()).isEqualTo(HttpStatus.OK);

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        exId))
                .isEqualTo("matched");
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
                "msa-it-" + orderId);
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
                "vref-msa-" + orderId);
        return jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
    }

    private BuySeed seedFilledBuyOrderWithExecution() {
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
                "msa-buy-" + orderId);
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
                "vref-msa-buy-" + orderId);
        long exId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', ?, 10, 0, 10, 0)
                        """,
                accountId,
                DEFAULT_CUSTODY);
        return new BuySeed(accountId, exId);
    }

    private record BuySeed(UUID accountId, long executionId) {}

    private String base() {
        return "http://localhost:" + port + "/internal/v1/settlement/manual-actions";
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }
}
