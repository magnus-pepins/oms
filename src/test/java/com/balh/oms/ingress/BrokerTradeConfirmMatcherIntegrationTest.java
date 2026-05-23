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

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 3 of gap plan §5.2: economic broker confirm matcher.
 *
 * <p>Three match outcomes covered:
 * <ul>
 *   <li>matched — economic fields agree; matcher enqueues a
 *       {@code broker_settlement_confirm} row so the existing v1 pipeline advances state.</li>
 *   <li>mismatch — at least one field disagrees; matcher opens a
 *       {@code reconciliation_breaks} row of type {@code trade_mismatch} and does
 *       <strong>not</strong> enqueue the v1 confirm.</li>
 *   <li>unresolved — no execution found for {@code (accountId, venueExecRef)}; matcher opens
 *       a {@code reconciliation_breaks} row of type {@code unresolved_confirm}.</li>
 * </ul>
 *
 * <p>Also asserts the {@code GET /reconciliation-breaks} endpoint returns the rows the
 * matcher wrote (gap plan §5.13).
 */
class BrokerTradeConfirmMatcherIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void processPendingMatches_match_enqueuesBrokerSettlementConfirm() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-MATCH-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");

        long confirmId = ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-MATCH-1");

        var matchRes = postProcessPending();
        assertThat(matchRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(matchRes.getBody()).isNotNull();
        assertThat(matchRes.getBody().items()).hasSize(1);
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("matched");
        assertThat(matchRes.getBody().items().getFirst().confirmId()).isEqualTo(confirmId);

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE id = ?",
                        String.class,
                        confirmId))
                .isEqualTo("matched");
        Long resolvedExecutionId = jdbc.queryForObject(
                "SELECT resolved_execution_id FROM broker_trade_confirm WHERE id = ?",
                Long.class,
                confirmId);
        assertThat(resolvedExecutionId).isNotNull();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ?",
                        Integer.class,
                        resolvedExecutionId))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks", Integer.class))
                .isZero();
    }

    @Test
    void processPendingMatches_quantityMismatch_opensTradeMismatchBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-MM-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");

        long confirmId = ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "9", "5.00", "BT-MM-1");
        var matchRes = postProcessPending();
        assertThat(matchRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(matchRes.getBody()).isNotNull();
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("mismatch");

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE id = ?",
                        String.class,
                        confirmId))
                .isEqualTo("mismatch");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'trade_mismatch'",
                        Integer.class))
                .isEqualTo(1);
        String diffJson = jdbc.queryForObject(
                "SELECT diff_json::text FROM reconciliation_breaks WHERE confirm_id = ?",
                String.class,
                confirmId);
        assertThat(diffJson)
                .contains("\"quantity\"")
                .contains("\"broker\": \"9.")
                .contains("\"oms\": \"10.");
    }

    @Test
    void processPendingMatches_noExecution_opensUnresolvedBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-NX-" + UUID.randomUUID();

        long confirmId =
                ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-NX-1");
        var matchRes = postProcessPending();
        assertThat(matchRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(matchRes.getBody()).isNotNull();
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("unresolved");

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE id = ?",
                        String.class,
                        confirmId))
                .isEqualTo("unresolved");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'unresolved_confirm'",
                        Integer.class))
                .isEqualTo(1);
    }

    @Test
    void matchById_appliesOnceThenReturns404ForAlreadyDecided() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-ONCE-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");
        long confirmId = ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-ONCE-1");

        ResponseEntity<SettlementController.BrokerTradeMatchResultResponse> first = postMatchOne(confirmId);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).isNotNull();
        assertThat(first.getBody().outcome()).isEqualTo("matched");

        ResponseEntity<SettlementController.BrokerTradeMatchResultResponse> second = postMatchOne(confirmId);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void listReconciliationBreaks_returnsOpenBreaksByDefault() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-LIST-" + UUID.randomUUID();
        ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-LIST-1");
        postProcessPending();

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<SettlementController.ReconciliationBreaksListResponse> list = http.exchange(
                base() + "/reconciliation-breaks?limit=10&offset=0",
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(list.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(list.getBody()).isNotNull();
        assertThat(list.getBody().status()).isEqualTo("open");
        assertThat(list.getBody().items()).hasSize(1);
        assertThat(list.getBody().items().getFirst().breakType()).isEqualTo("unresolved_confirm");
    }

    private ResponseEntity<SettlementController.BrokerTradeMatchBatchResponse> postProcessPending() {
        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        return http.exchange(
                base() + "/broker-trade-confirms/process-pending-matches",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
    }

    private ResponseEntity<SettlementController.BrokerTradeMatchResultResponse> postMatchOne(long confirmId) {
        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        return http.exchange(
                base() + "/broker-trade-confirms/" + confirmId + "/match",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
    }

    private long ingestSingleConfirm(
            UUID accountId,
            String venueExecRef,
            String symbol,
            String side,
            String quantity,
            String price,
            String brokerTradeId) {
        String json = ("""
                {
                  "schemaVersion": 1,
                  "brokerId": "broker_x",
                  "fileId": "%FILE%",
                  "businessDate": "2026-05-23",
                  "rows": [
                    {
                      "brokerTradeId": "%TRADE%",
                      "venueExecRef": "%VREF%",
                      "accountId": "%ACCT%",
                      "instrument": {
                        "symbol": "%SYMBOL%",
                        "currency": "USD"
                      },
                      "side": "%SIDE%",
                      "quantity": "%QTY%",
                      "price": "%PRICE%",
                      "tradeDate": "2026-05-23",
                      "settlementDate": "2026-05-27",
                      "settlementCurrency": "USD",
                      "correctionType": "new"
                    }
                  ]
                }
                """)
                .replace("%FILE%", "F-" + UUID.randomUUID())
                .replace("%TRADE%", brokerTradeId)
                .replace("%VREF%", venueExecRef)
                .replace("%ACCT%", accountId.toString())
                .replace("%SYMBOL%", symbol)
                .replace("%SIDE%", side)
                .replace("%QTY%", quantity)
                .replace("%PRICE%", price);

        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> ingestRes = http.exchange(
                base() + "/broker-trade-confirms/import-json?source=it-matcher",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(StandardCharsets.UTF_8), h),
                new ParameterizedTypeReference<>() {});
        assertThat(ingestRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ingestRes.getBody()).isNotNull();
        assertThat(ingestRes.getBody().status()).isEqualTo("parsed");
        assertThat(ingestRes.getBody().insertedRows()).isEqualTo(1);

        return jdbc.queryForObject(
                "SELECT id FROM broker_trade_confirm WHERE broker_trade_id = ?",
                Long.class,
                brokerTradeId);
    }

    /**
     * Default expected settlement date matches the broker confirm template's hardcoded
     * {@code settlementDate: 2026-05-27} (see {@link #ingestSingleConfirm}), so the
     * existing match-path cases assert "no break" cleanly. Tests that want to drive the
     * settlement-date axis pass an explicit value (or {@code null}) via the overload.
     */
    private static final LocalDate DEFAULT_EXPECTED_SETTLEMENT_DATE = LocalDate.of(2026, 5, 27);

    private void seedTradeExecution(
            UUID accountId, String venueExecRef, String symbol, String side, String quantity, String price) {
        seedTradeExecution(accountId, venueExecRef, symbol, side, quantity, price, DEFAULT_EXPECTED_SETTLEMENT_DATE);
    }

    private void seedTradeExecution(
            UUID accountId,
            String venueExecRef,
            String symbol,
            String side,
            String quantity,
            String price,
            LocalDate expectedSettlementDate) {
        UUID orderId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', CAST(? AS order_side), ?, CAST(? AS NUMERIC), CAST(? AS NUMERIC), 'DAY',
                          NOW(), NOW(), 'h', NULL, CAST(? AS NUMERIC)
                        )
                        """,
                orderId,
                accountId,
                "matcher-" + orderId,
                side,
                symbol,
                quantity,
                price,
                quantity);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, trade_date, expected_settlement_date
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          CAST(? AS NUMERIC), CAST(? AS NUMERIC), 0, CAST(? AS NUMERIC),
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          ?, ?
                        )
                        """,
                orderId,
                accountId,
                venueExecRef,
                quantity,
                price,
                quantity,
                expectedSettlementDate == null
                        ? null
                        : java.sql.Date.valueOf(expectedSettlementDate.minusDays(4)),
                expectedSettlementDate == null ? null : java.sql.Date.valueOf(expectedSettlementDate));
    }

    private String base() {
        return "http://localhost:" + port + "/internal/v1/settlement";
    }
}
