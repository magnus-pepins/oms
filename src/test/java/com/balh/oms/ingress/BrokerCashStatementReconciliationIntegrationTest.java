package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
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

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Phase C Slice 8b (gap plan §5.7): broker cash statement ingest + OMS reconcile. */
class BrokerCashStatementReconciliationIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void importJson_persistsBatchAndMovements() {
        String json = envelope("CASH-F1", "-784.20", "EX-SETTLE-1");
        ResponseEntity<SettlementController.BrokerCashStatementImportResponse> res = postIngest(json);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().status()).isEqualTo("parsed");
        assertThat(res.getBody().insertedMovements()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_cash_statement_batch", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void reconcile_whenAmountMatches_opensNoBreaks() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.randomUUID();
        String venueRef = "EX-SETTLE-MATCH";
        seedCustody(custodyId);
        seedBuyExecution(accountId, custodyId, venueRef, "10", "78.42", "2026-05-27");

        long batchId = ingestBatch("-784.20", venueRef);
        ResponseEntity<SettlementController.CashReconciliationResponse> recon = postReconcile(batchId);

        assertThat(recon.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recon.getBody()).isNotNull();
        assertThat(recon.getBody().matchedCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'cash_mismatch'",
                        Integer.class))
                .isZero();
    }

    /**
     * Regression guard for the 2026-05-24 cash recon currency bug. The lookup SQL
     * used to hardcode {@code 'SEK' AS currency}, so a USD broker movement against
     * a USD execution would mismatch on currency even when the amounts agreed and
     * the OMS account's underlying instrument was USD-settled. The fix derives
     * currency per-execution from {@code instrument_settlement_profile}; this test
     * proves a USD-settled execution matches a USD broker movement instead of
     * raising a "currency mismatch" break with delta 0.
     */
    @Test
    void reconcile_usdInstrument_matchesUsdMovement_noCurrencyMismatch() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.randomUUID();
        String venueRef = "EX-SETTLE-USD";
        seedCustody(custodyId);
        seedInstrumentProfile("AAPL", "USD");
        seedBuyExecution(accountId, custodyId, venueRef, "AAPL", "1", "100.50", "2026-05-22");

        long batchId = ingestUsdBatch("-100.50", venueRef);
        ResponseEntity<SettlementController.CashReconciliationResponse> recon = postReconcile(batchId);

        assertThat(recon.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recon.getBody()).isNotNull();
        assertThat(recon.getBody().matchedCount()).as("USD movement against USD execution must match").isEqualTo(1);
        assertThat(recon.getBody().mismatchCount()).isZero();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM cash_reconciliation_report_row "
                                + "WHERE outcome = 'mismatch' "
                                + "AND diff_json ->> 'reason' = 'currency mismatch'",
                        Integer.class))
                .as("no rows must be flagged 'currency mismatch' for a non-SEK instrument")
                .isZero();
    }

    @Test
    void reconcile_whenAmountMismatch_opensCashBreak() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.randomUUID();
        String venueRef = "EX-SETTLE-MIS";
        seedCustody(custodyId);
        seedBuyExecution(accountId, custodyId, venueRef, "10", "78.42", "2026-05-27");

        long batchId = ingestBatch("-700.00", venueRef);
        ResponseEntity<SettlementController.CashReconciliationResponse> recon = postReconcile(batchId);

        assertThat(recon.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recon.getBody()).isNotNull();
        assertThat(recon.getBody().mismatchCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'cash_mismatch'",
                        Integer.class))
                .isEqualTo(1);
    }

    private long ingestBatch(String amount, String executionRef) {
        ResponseEntity<SettlementController.BrokerCashStatementImportResponse> res =
                postIngest(envelope("CASH-" + UUID.randomUUID(), amount, executionRef));
        assertThat(res.getBody()).isNotNull();
        return res.getBody().batchId();
    }

    private long ingestUsdBatch(String amount, String executionRef) {
        ResponseEntity<SettlementController.BrokerCashStatementImportResponse> res =
                postIngest(usdEnvelope("CASH-" + UUID.randomUUID(), amount, executionRef));
        assertThat(res.getBody()).isNotNull();
        return res.getBody().batchId();
    }

    private ResponseEntity<SettlementController.BrokerCashStatementImportResponse> postIngest(String json) {
        return http.exchange(
                base() + "/broker-cash-statements/import-json",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(java.nio.charset.StandardCharsets.UTF_8), jsonHeaders()),
                SettlementController.BrokerCashStatementImportResponse.class);
    }

    private ResponseEntity<SettlementController.CashReconciliationResponse> postReconcile(long batchId) {
        return http.exchange(
                base() + "/broker-cash-statements/batches/" + batchId + "/reconcile",
                HttpMethod.POST,
                new HttpEntity<>(jsonHeaders()),
                SettlementController.CashReconciliationResponse.class);
    }

    private HttpHeaders jsonHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }

    private String base() {
        return "http://localhost:" + port + "/internal/v1/settlement";
    }

    private static String envelope(String fileId, String amount, String executionRef) {
        return """
                {
                  "schemaVersion": 1,
                  "brokerId": "broker_x",
                  "businessDate": "2026-05-27",
                  "fileId": "%s",
                  "currency": "SEK",
                  "openingBalance": "100000.00",
                  "closingBalance": "99215.80",
                  "movements": [
                    {
                      "brokerMovementId": "CM1",
                      "type": "buy_settlement",
                      "executionRef": "%s",
                      "amount": "%s",
                      "currency": "SEK",
                      "valueDate": "2026-05-27"
                    }
                  ]
                }
                """
                .formatted(fileId, executionRef, amount);
    }

    private static String usdEnvelope(String fileId, String amount, String executionRef) {
        return """
                {
                  "schemaVersion": 1,
                  "brokerId": "broker_x",
                  "businessDate": "2026-05-22",
                  "fileId": "%s",
                  "currency": "USD",
                  "openingBalance": "100.50",
                  "closingBalance": "0.00",
                  "movements": [
                    {
                      "brokerMovementId": "CM-USD-1",
                      "type": "buy_settlement",
                      "executionRef": "%s",
                      "amount": "%s",
                      "currency": "USD",
                      "valueDate": "2026-05-22"
                    }
                  ]
                }
                """
                .formatted(fileId, executionRef, amount);
    }

    private void seedInstrumentProfile(String symbol, String settlementCurrency) {
        jdbc.update(
                """
                        INSERT INTO instrument_settlement_profile (
                          instrument_id, symbol, isin, primary_mic, settlement_calendar_id,
                          settlement_cycle, settlement_currency, isk_eligible, effective_from
                        ) VALUES (
                          ?, ?, NULL, 'XNAS', 'XNAS', 'T+2', ?, FALSE, DATE '2000-01-01'
                        )
                        """,
                symbol,
                symbol,
                settlementCurrency);
    }

    private void seedCustody(UUID custodyId) {
        jdbc.update(
                """
                        INSERT INTO custody_accounts (id, broker_id, account_type, csd_or_book_ref, currency_class)
                        VALUES (?, 'broker_x', 'omnibus', '', 'MULTI')
                        """,
                custodyId);
    }

    private void seedBuyExecution(
            UUID accountId,
            UUID custodyId,
            String venueRef,
            String qty,
            String price,
            String expectedSettlementDate) {
        seedBuyExecution(accountId, custodyId, venueRef, "ERIC-B.ST", qty, price, expectedSettlementDate);
    }

    private void seedBuyExecution(
            UUID accountId,
            UUID custodyId,
            String venueRef,
            String instrumentSymbol,
            String qty,
            String price,
            String expectedSettlementDate) {
        UUID orderId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', 'BUY', ?, ?, ?, 'DAY',
                          NOW(), NOW(), 'h', ?
                        )
                        """,
                orderId,
                accountId,
                "cash-recon-" + orderId,
                instrumentSymbol,
                new java.math.BigDecimal(qty),
                new java.math.BigDecimal(price),
                new java.math.BigDecimal(qty));
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, settlement_status, expected_settlement_date,
                          trade_date
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          ?, ?, 0, ?,
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          CAST('settled' AS execution_settlement_status), ?, ?
                        )
                        """,
                orderId,
                accountId,
                venueRef,
                new java.math.BigDecimal(qty),
                new java.math.BigDecimal(price),
                new java.math.BigDecimal(qty),
                java.sql.Date.valueOf(expectedSettlementDate),
                java.sql.Date.valueOf(expectedSettlementDate));
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id, currency,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, ?, ?, 'SEK', ?, ?, 0, 0)
                        """,
                accountId,
                instrumentSymbol,
                custodyId,
                new java.math.BigDecimal(qty),
                new java.math.BigDecimal(qty));
    }
}
