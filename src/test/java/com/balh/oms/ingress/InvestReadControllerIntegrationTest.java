package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
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
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/** Gap plan §5.4 / §5.13 — customer invest read models on internal HTTP. */
class InvestReadControllerIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void cashAvailability_sumsOpenTradeBuyAndSellNotionals() {
        UUID accountId = UUID.randomUUID();
        seedOpenBuyExecution(accountId, "10", "5.00");
        seedOpenSellExecution(accountId, "2", "100.00");

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<InvestReadController.CashAvailabilityResponse> res = http.exchange(
                base() + "/cash-availability?accountId=" + accountId + "&currency=USD",
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().currency()).isEqualTo("USD");
        assertThat(new java.math.BigDecimal(res.getBody().pendingSettlementDebits()))
                .isEqualByComparingTo("50.00");
        assertThat(new java.math.BigDecimal(res.getBody().pendingSettlementCredits()))
                .isEqualByComparingTo("200.00");
        assertThat(new java.math.BigDecimal(res.getBody().unsettledSaleProceeds()))
                .isEqualByComparingTo("200.00");
    }

    @Test
    void settlementEvents_returnsRecentTradeExecutions() {
        UUID accountId = UUID.randomUUID();
        seedOpenBuyExecution(accountId, "10", "5.00");

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<InvestReadController.SettlementEventsListResponse> res = http.exchange(
                base() + "/settlement-events?accountId=" + accountId + "&limit=5",
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).hasSize(1);
        assertThat(res.getBody().items().getFirst().instrumentSymbol()).isEqualTo("AAPL");
        assertThat(res.getBody().items().getFirst().settlementStatus()).isEqualTo("executed");
    }

    @Test
    void executions_returnsTradeFillHistory() {
        UUID accountId = UUID.randomUUID();
        seedOpenBuyExecution(accountId, "10", "5.00");
        seedOpenSellExecution(accountId, "2", "100.00");

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<InvestReadController.ExecutionsListResponse> res = http.exchange(
                base() + "/executions?accountId=" + accountId + "&limit=100",
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).hasSize(2);
        assertThat(res.getBody().items())
                .allSatisfy(item -> assertThat(item.instrumentSymbol()).isEqualTo("AAPL"));
    }

    @Test
    void executions_honoursFromToWindow() {
        UUID accountId = UUID.randomUUID();
        seedOpenBuyExecution(accountId, "10", "5.00");

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        // A future-only window excludes the NOW()-stamped fill above.
        ResponseEntity<InvestReadController.ExecutionsListResponse> res = http.exchange(
                base() + "/executions?accountId=" + accountId + "&from=2999-01-01T00:00:00Z",
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).isEmpty();
    }

    @Test
    void investPositions_exposesPendingQuantityColumns() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.fromString("a0000001-0000-4000-8000-000000000001");
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id, currency,
                          quantity_total, quantity_settled, quantity_pending_buy_settle,
                          quantity_pending_sell_settle, updated_at
                        ) VALUES (?, 'AAPL', ?, 'USD', 10, 5, 3, 2, NOW())
                        """,
                accountId,
                custodyId);

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<InvestReadController.InvestPositionsListResponse> res = http.exchange(
                base() + "/positions?accountId=" + accountId,
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody().items()).hasSize(1);
        var row = res.getBody().items().getFirst();
        assertThat(new java.math.BigDecimal(row.quantitySettled())).isEqualByComparingTo("5");
        assertThat(new java.math.BigDecimal(row.quantityPendingBuySettle())).isEqualByComparingTo("3");
        assertThat(new java.math.BigDecimal(row.quantityPendingSellSettle())).isEqualByComparingTo("2");
    }

    private void seedOpenBuyExecution(UUID accountId, String qty, String price) {
        UUID orderId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', CAST('BUY' AS order_side), 'AAPL',
                          CAST(? AS NUMERIC), CAST(? AS NUMERIC), 'DAY', NOW(), NOW(), 'h',
                          CAST(? AS NUMERIC)
                        )
                        """,
                orderId,
                accountId,
                "inv-read-" + orderId,
                qty,
                price,
                qty);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, settlement_status,
                          trade_date, expected_settlement_date
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          CAST(? AS NUMERIC), CAST(? AS NUMERIC), 0, CAST(? AS NUMERIC),
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          CAST('executed' AS execution_settlement_status),
                          DATE '2026-05-23', DATE '2026-05-27'
                        )
                        """,
                orderId,
                accountId,
                "vref-buy-" + orderId,
                qty,
                price,
                qty);
    }

    private void seedOpenSellExecution(UUID accountId, String qty, String price) {
        UUID orderId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', CAST('SELL' AS order_side), 'AAPL',
                          CAST(? AS NUMERIC), CAST(? AS NUMERIC), 'DAY', NOW(), NOW(), 'h',
                          CAST(? AS NUMERIC)
                        )
                        """,
                orderId,
                accountId,
                "inv-read-sell-" + orderId,
                qty,
                price,
                qty);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, settlement_status
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          CAST(? AS NUMERIC), CAST(? AS NUMERIC), 0, CAST(? AS NUMERIC),
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          CAST('matched' AS execution_settlement_status)
                        )
                        """,
                orderId,
                accountId,
                "vref-sell-" + orderId,
                qty,
                price,
                qty);
    }

    private String base() {
        return "http://localhost:" + port + "/internal/v1/invest";
    }
}
