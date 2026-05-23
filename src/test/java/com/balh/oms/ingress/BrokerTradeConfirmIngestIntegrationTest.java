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
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 2 of gap plan §5.1: economic broker trade confirm ingest path.
 *
 * <p>Asserts the ingest endpoint persists batch / confirm / fee rows, is idempotent
 * on identical bytes <strong>and</strong> on {@code (brokerId, fileId)} collision,
 * rejects unsupported {@code schemaVersion}, and marks the batch {@code failed} on
 * empty {@code rows}.
 *
 * <p>Matching is out of scope for this slice: rows are expected to land with
 * {@code resolved_execution_id = NULL} and {@code match_status = 'pending'}.
 */
class BrokerTradeConfirmIngestIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void importJson_singleRow_persistsBatchConfirmAndFees() {
        String json = envelope("F1", "BT1");
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> res = postJson(json);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().duplicate()).isFalse();
        assertThat(res.getBody().status()).isEqualTo("parsed");
        assertThat(res.getBody().rowCount()).isEqualTo(1);
        assertThat(res.getBody().insertedRows()).isEqualTo(1);
        assertThat(res.getBody().insertedFees()).isEqualTo(1);
        assertThat(res.getBody().skippedDuplicateRows()).isZero();

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_confirm_batch", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        Integer.class,
                        "BT1"))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        String.class,
                        "BT1"))
                .isEqualTo("pending");
        assertThat(jdbc.queryForObject(
                        "SELECT resolved_execution_id FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        Long.class,
                        "BT1"))
                .isNull();
        assertThat(jdbc.queryForObject(
                        "SELECT raw_row_json::text FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        String.class,
                        "BT1"))
                .contains("\"brokerTradeId\": \"BT1\"");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_trade_confirm_fee", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void importJson_sameBytes_isIdempotent() {
        String json = envelope("F1", "BT1");
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> first = postJson(json);
        assertThat(first.getBody()).isNotNull();
        long batchId = first.getBody().batchId();

        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> second = postJson(json);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().duplicate()).isTrue();
        assertThat(second.getBody().batchId()).isEqualTo(batchId);

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_confirm_batch", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_trade_confirm", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void importJson_sameBrokerFileIdDifferentBytes_isDuplicate() {
        String first = envelope("F1", "BT1");
        String second = envelope("F1", "BT2");
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> firstRes = postJson(first);
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> secondRes = postJson(second);

        assertThat(firstRes.getBody()).isNotNull();
        assertThat(secondRes.getBody()).isNotNull();
        assertThat(secondRes.getBody().duplicate()).isTrue();
        assertThat(secondRes.getBody().batchId()).isEqualTo(firstRes.getBody().batchId());
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_confirm_batch", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        Integer.class,
                        "BT2"))
                .isZero();
    }

    @Test
    void importJson_sameBrokerTradeIdAcrossBatches_isSkippedAsDuplicateRow() {
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> first =
                postJson(envelope("F1", "BT1"));
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> second =
                postJson(envelope("F2", "BT1"));
        assertThat(first.getBody()).isNotNull();
        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().duplicate()).isFalse();
        assertThat(second.getBody().insertedRows()).isZero();
        assertThat(second.getBody().skippedDuplicateRows()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        Integer.class,
                        "BT1"))
                .isEqualTo(1);
    }

    @Test
    void importJson_unsupportedSchemaVersion_rejected() {
        String json =
                """
                        {
                          "schemaVersion": 99,
                          "brokerId": "broker_x",
                          "fileId": "F-99",
                          "businessDate": "2026-05-23",
                          "rows": []
                        }
                        """;
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> res = postJson(json);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().status()).isEqualTo("rejected");
        assertThat(res.getBody().errorSummary()).contains("unsupported schemaVersion");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_confirm_batch", Integer.class))
                .isZero();
    }

    @Test
    void importJson_emptyRows_marksBatchFailed() {
        String json =
                """
                        {
                          "schemaVersion": 1,
                          "brokerId": "broker_x",
                          "fileId": "F-empty",
                          "businessDate": "2026-05-23",
                          "rows": []
                        }
                        """;
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> res = postJson(json);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().duplicate()).isFalse();
        assertThat(res.getBody().status()).isEqualTo("failed");
        assertThat(res.getBody().errorSummary()).isNotBlank();
        assertThat(jdbc.queryForObject(
                        "SELECT status FROM broker_confirm_batch WHERE id = ?",
                        String.class,
                        res.getBody().batchId()))
                .isEqualTo("failed");
    }

    @Test
    void fileImport_multipart_persistsRows() {
        String json = envelope("FMP1", "BTMP1");
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> res = postMultipart(json);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().duplicate()).isFalse();
        assertThat(res.getBody().status()).isEqualTo("parsed");
        assertThat(res.getBody().insertedRows()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT broker_id FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        String.class,
                        "BTMP1"))
                .isEqualTo("broker_x");
    }

    private ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> postJson(String json) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return http.exchange(
                base() + "/broker-trade-confirms/import-json?source=it",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(StandardCharsets.UTF_8), h),
                new ParameterizedTypeReference<>() {});
    }

    private ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> postMultipart(String json) {
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("source", "it-mp");
        body.add(
                "file",
                new ByteArrayResource(json.getBytes(StandardCharsets.UTF_8)) {
                    @Override
                    public String getFilename() {
                        return "broker-trade.json";
                    }
                });
        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        return http.exchange(
                base() + "/broker-trade-confirms/file-import",
                HttpMethod.POST,
                new HttpEntity<>(body, h),
                new ParameterizedTypeReference<>() {});
    }

    private static String envelope(String fileId, String brokerTradeId) {
        return ("""
                {
                  "schemaVersion": 1,
                  "brokerId": "broker_x",
                  "fileId": "%FILE_ID%",
                  "businessDate": "2026-05-23",
                  "generatedAt": "2026-05-23T21:15:00Z",
                  "rows": [
                    {
                      "brokerTradeId": "%TRADE_ID%",
                      "venueExecRef": "EX-%TRADE_ID%",
                      "instrument": {
                        "symbol": "ERIC-B.ST",
                        "isin": "SE0000108656",
                        "mic": "XSTO",
                        "currency": "SEK"
                      },
                      "side": "BUY",
                      "quantity": "10",
                      "price": "78.42",
                      "grossAmount": "784.20",
                      "fees": [
                        { "type": "commission", "amount": "1.00", "currency": "SEK", "chargedTo": "customer" }
                      ],
                      "tradeDate": "2026-05-23",
                      "settlementDate": "2026-05-27",
                      "settlementCurrency": "SEK",
                      "status": "confirmed",
                      "correctionType": "new"
                    }
                  ]
                }
                """)
                .replace("%FILE_ID%", fileId)
                .replace("%TRADE_ID%", brokerTradeId);
    }

    private String base() {
        return "http://localhost:" + port + "/internal/v1/settlement";
    }
}
