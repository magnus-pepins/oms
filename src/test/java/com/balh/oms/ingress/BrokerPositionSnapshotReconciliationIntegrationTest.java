package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.ingress.ApiKeyFilter;
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

/**
 * Phase C Slice 8a (gap plan §5.6): broker position snapshot ingest + OMS reconcile.
 */
class BrokerPositionSnapshotReconciliationIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void importJson_persistsBatchAndRows() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.randomUUID();
        seedCustody(custodyId);
        String json = envelope("POS-F1", accountId, custodyId, "10", "10", "0", "0");

        ResponseEntity<SettlementController.BrokerPositionSnapshotImportResponse> res = postIngest(json);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().duplicate()).isFalse();
        assertThat(res.getBody().status()).isEqualTo("parsed");
        assertThat(res.getBody().insertedRows()).isEqualTo(1);

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_position_snapshot_batch", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_position_snapshot_row", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void reconcile_whenOmsMatches_opensNoBreaks() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.randomUUID();
        seedCustody(custodyId);
        seedPosition(accountId, custodyId, "ERIC-B.ST", "10", "10", "0", "0");

        long batchId = ingestBatch(accountId, custodyId, "10", "10", "0", "0");
        ResponseEntity<SettlementController.PositionReconciliationResponse> recon = postReconcile(batchId);

        assertThat(recon.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recon.getBody()).isNotNull();
        assertThat(recon.getBody().matchedCount()).isEqualTo(1);
        assertThat(recon.getBody().mismatchCount()).isZero();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'position_mismatch'",
                        Integer.class))
                .isZero();
    }

    @Test
    void reconcile_whenQuantityMismatch_opensPositionBreak() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.randomUUID();
        seedCustody(custodyId);
        seedPosition(accountId, custodyId, "ERIC-B.ST", "10", "10", "0", "0");

        long batchId = ingestBatch(accountId, custodyId, "9", "9", "0", "0");
        ResponseEntity<SettlementController.PositionReconciliationResponse> recon = postReconcile(batchId);

        assertThat(recon.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recon.getBody()).isNotNull();
        assertThat(recon.getBody().mismatchCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'position_mismatch'",
                        Integer.class))
                .isEqualTo(1);
    }

    @Test
    void reconcile_whenMissingInOms_opensBreak() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.randomUUID();
        seedCustody(custodyId);

        long batchId = ingestBatch(accountId, custodyId, "5", "5", "0", "0");
        ResponseEntity<SettlementController.PositionReconciliationResponse> recon = postReconcile(batchId);

        assertThat(recon.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recon.getBody()).isNotNull();
        assertThat(recon.getBody().missingInOmsCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'position_mismatch'",
                        Integer.class))
                .isEqualTo(1);
    }

    private long ingestBatch(
            UUID accountId,
            UUID custodyId,
            String total,
            String settled,
            String pendingBuy,
            String pendingSell) {
        String json = envelope("POS-" + UUID.randomUUID(), accountId, custodyId, total, settled, pendingBuy, pendingSell);
        ResponseEntity<SettlementController.BrokerPositionSnapshotImportResponse> res = postIngest(json);
        assertThat(res.getBody()).isNotNull();
        return res.getBody().batchId();
    }

    private ResponseEntity<SettlementController.BrokerPositionSnapshotImportResponse> postIngest(String json) {
        return http.exchange(
                base() + "/broker-position-snapshots/import-json",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(java.nio.charset.StandardCharsets.UTF_8), jsonHeaders()),
                SettlementController.BrokerPositionSnapshotImportResponse.class);
    }

    private ResponseEntity<SettlementController.PositionReconciliationResponse> postReconcile(long batchId) {
        return http.exchange(
                base() + "/broker-position-snapshots/batches/" + batchId + "/reconcile",
                HttpMethod.POST,
                new HttpEntity<>(jsonHeaders()),
                SettlementController.PositionReconciliationResponse.class);
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

    private static String envelope(
            String fileId,
            UUID accountId,
            UUID custodyId,
            String total,
            String settled,
            String pendingBuy,
            String pendingSell) {
        return """
                {
                  "schemaVersion": 1,
                  "brokerId": "broker_x",
                  "businessDate": "2026-05-23",
                  "fileId": "%s",
                  "rows": [
                    {
                      "brokerAccount": "BALH-OMNI-001",
                      "accountId": "%s",
                      "custodyAccountId": "%s",
                      "instrument": { "symbol": "ERIC-B.ST", "isin": "SE0000108656", "currency": "SEK" },
                      "quantityTotal": "%s",
                      "quantitySettled": "%s",
                      "quantityPendingBuySettle": "%s",
                      "quantityPendingSellSettle": "%s",
                      "asOf": "2026-05-23T21:00:00Z"
                    }
                  ]
                }
                """
                .formatted(fileId, accountId, custodyId, total, settled, pendingBuy, pendingSell);
    }

    private void seedCustody(UUID custodyId) {
        jdbc.update(
                """
                        INSERT INTO custody_accounts (id, broker_id, account_type, csd_or_book_ref, currency_class)
                        VALUES (?, 'broker_x', 'omnibus', '', 'MULTI')
                        """,
                custodyId);
    }

    private void seedPosition(
            UUID accountId,
            UUID custodyId,
            String symbol,
            String total,
            String settled,
            String pendingBuy,
            String pendingSell) {
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id, currency,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, ?, ?, 'SEK', ?, ?, ?, ?)
                        """,
                accountId,
                symbol,
                custodyId,
                new java.math.BigDecimal(total),
                new java.math.BigDecimal(settled),
                new java.math.BigDecimal(pendingBuy),
                new java.math.BigDecimal(pendingSell));
    }
}
