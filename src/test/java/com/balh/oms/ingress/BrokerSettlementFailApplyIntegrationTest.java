package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.math.BigDecimal;
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
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/** Phase D Slice 10b/14a (gap plan §5.8): fail file apply → settlement lots + mark-failed + penalty outbox. */
@TestPropertySource(properties = "oms.ledger.settlement-outbox-enabled=true")
class BrokerSettlementFailApplyIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void apply_fullFail_marksExecutionFailedAndCreatesLot() {
        String venueRef = "EX-FAIL-FULL";
        long executionId = seedBuyTrade(venueRef, "10");

        long batchId = ingestFail(venueRef, "10");
        ResponseEntity<SettlementController.BrokerSettlementFailApplyResponse> res = postApply(batchId);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().fullFailCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?", String.class, executionId))
                .isEqualTo("failed");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM execution_settlement_lot WHERE execution_id = ? AND status = 'failed'",
                        Integer.class,
                        executionId))
                .isEqualTo(1);
    }

    @Test
    void apply_partialFail_createsFailedAndPendingLotsWithoutMarkFailed() {
        String venueRef = "EX-FAIL-PARTIAL";
        long executionId = seedBuyTrade(venueRef, "10");

        long batchId = ingestFail(venueRef, "3");
        ResponseEntity<SettlementController.BrokerSettlementFailApplyResponse> res = postApply(batchId);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().partialFailCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?", String.class, executionId))
                .isNotEqualTo("failed");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM execution_settlement_lot WHERE execution_id = ? AND status = 'failed'",
                        Integer.class,
                        executionId))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM execution_settlement_lot WHERE execution_id = ? AND status = 'pending'",
                        Integer.class,
                        executionId))
                .isEqualTo(1);
    }

    @Test
    void apply_unmatchedExecution_opensBreak() {
        long batchId = ingestFail("EX-MISSING", "10");
        ResponseEntity<SettlementController.BrokerSettlementFailApplyResponse> res = postApply(batchId);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().unmatchedCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'settlement_fail_unmatched'",
                        Integer.class))
                .isEqualTo(1);
    }

    @Test
    void apply_withPenalty_enqueuesLedgerOutboxLeg() {
        String venueRef = "EX-FAIL-PENALTY";
        long executionId = seedBuyTrade(venueRef, "10");

        long batchId = ingestFailWithPenalty(venueRef, "10", "25.00", "USD");
        ResponseEntity<SettlementController.BrokerSettlementFailApplyResponse> res = postApply(batchId);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM ledger_settlement_outbox "
                                + "WHERE execution_id = ? AND to_settlement_status = 'fail_penalty'",
                        Integer.class,
                        executionId))
                .isEqualTo(1);
        assertThat(new BigDecimal(jdbc.queryForObject(
                        "SELECT payload_json->>'penaltyAmount' FROM ledger_settlement_outbox "
                                + "WHERE execution_id = ? AND leg_kind LIKE 'penalty-%'",
                        String.class,
                        executionId)))
                .isEqualByComparingTo("25.00");
    }

    private long ingestFailWithPenalty(
            String executionRef, String failedQty, String penaltyAmount, String penaltyCurrency) {
        String json = envelopeWithPenalty("FAIL-PEN-" + executionRef, executionRef, failedQty, penaltyAmount, penaltyCurrency);
        ResponseEntity<SettlementController.BrokerSettlementFailImportResponse> res = http.exchange(
                base() + "/broker-settlement-fails/import-json?source=it",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(), headers()),
                SettlementController.BrokerSettlementFailImportResponse.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        return res.getBody().batchId();
    }

    private static String envelopeWithPenalty(
            String fileId, String executionRef, String failedQty, String penaltyAmount, String penaltyCurrency) {
        return """
                {
                  "schemaVersion": 1,
                  "brokerId": "DEFAULT",
                  "fileId": "%s",
                  "businessDate": "2026-05-27",
                  "fails": [
                    {
                      "brokerFailId": "BF-PEN-1",
                      "executionRef": "%s",
                      "instrumentSymbol": "AAPL",
                      "side": "BUY",
                      "failedQuantity": "%s",
                      "intendedSettlementDate": "2026-05-27",
                      "failReason": "SEC_NOT_DELIVERED",
                      "penaltyAmount": "%s",
                      "penaltyCurrency": "%s"
                    }
                  ]
                }
                """
                .formatted(fileId, executionRef, failedQty, penaltyAmount, penaltyCurrency);
    }

    private long ingestFail(String executionRef, String failedQty) {
        String json = envelope("FAIL-APPLY-" + executionRef, executionRef, failedQty);
        ResponseEntity<SettlementController.BrokerSettlementFailImportResponse> res = http.exchange(
                base() + "/broker-settlement-fails/import-json?source=it",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(), headers()),
                SettlementController.BrokerSettlementFailImportResponse.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        return res.getBody().batchId();
    }

    private ResponseEntity<SettlementController.BrokerSettlementFailApplyResponse> postApply(long batchId) {
        return http.exchange(
                base() + "/broker-settlement-fails/batches/" + batchId + "/apply",
                HttpMethod.POST,
                new HttpEntity<>(headers()),
                SettlementController.BrokerSettlementFailApplyResponse.class);
    }

    private long seedBuyTrade(String venueRef, String qty) {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', 'BUY', 'AAPL', ?, 5, 'DAY',
                          NOW(), NOW(), 'h', ?
                        )
                        """,
                orderId,
                accountId,
                "fail-apply-" + orderId,
                new java.math.BigDecimal(qty),
                new java.math.BigDecimal(qty));
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, expected_settlement_date
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          ?, 5, 0, ?,
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          DATE '2026-05-27'
                        )
                        """,
                orderId,
                accountId,
                venueRef,
                new java.math.BigDecimal(qty),
                new java.math.BigDecimal(qty));
        long executionId =
                jdbc.queryForObject("SELECT id FROM executions WHERE order_id = ?", Long.class, orderId);
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', ?, ?, 0, ?, 0)
                        """,
                accountId,
                DEFAULT_CUSTODY,
                new java.math.BigDecimal(qty),
                new java.math.BigDecimal(qty));
        return executionId;
    }

    private static String envelope(String fileId, String executionRef, String failedQty) {
        return """
                {
                  "schemaVersion": 1,
                  "brokerId": "DEFAULT",
                  "fileId": "%s",
                  "businessDate": "2026-05-27",
                  "fails": [
                    {
                      "brokerFailId": "bf-apply-1",
                      "executionRef": "%s",
                      "failedQuantity": "%s",
                      "intendedSettlementDate": "2026-05-27",
                      "failReason": "sec_not_delivered",
                      "resolutionStatus": "open"
                    }
                  ]
                }
                """
                .formatted(fileId, executionRef, failedQty);
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
