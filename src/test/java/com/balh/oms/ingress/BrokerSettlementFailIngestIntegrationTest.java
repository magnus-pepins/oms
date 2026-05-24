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

import static org.assertj.core.api.Assertions.assertThat;

/** Phase D Slice 10a (gap plan §5.8): broker settlement fail file ingest skeleton. */
class BrokerSettlementFailIngestIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void importJson_persistsBatchAndFailRows() {
        String json = envelope("FAIL-F1", "EX-FAIL-1");
        ResponseEntity<SettlementController.BrokerSettlementFailImportResponse> res = postIngest(json);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().status()).isEqualTo("parsed");
        assertThat(res.getBody().insertedFails()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_fail_batch", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_fail_row", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT execution_ref FROM broker_settlement_fail_row LIMIT 1", String.class))
                .isEqualTo("EX-FAIL-1");
    }

    @Test
    void importJson_duplicateBytes_returnsDuplicate() {
        String json = envelope("FAIL-F2", "EX-FAIL-2");
        ResponseEntity<SettlementController.BrokerSettlementFailImportResponse> first = postIngest(json);
        ResponseEntity<SettlementController.BrokerSettlementFailImportResponse> second = postIngest(json);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().duplicate()).isTrue();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_fail_batch", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void listBatches_returnsRecentIngest() {
        postIngest(envelope("FAIL-F3", "EX-FAIL-3"));
        ResponseEntity<SettlementController.BrokerSettlementFailBatchListResponse> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/settlement/broker-settlement-fails/batches?limit=10",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                SettlementController.BrokerSettlementFailBatchListResponse.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).hasSize(1);
        assertThat(res.getBody().items().getFirst().brokerFileId()).isEqualTo("FAIL-F3");
    }

    private ResponseEntity<SettlementController.BrokerSettlementFailImportResponse> postIngest(String json) {
        return http.exchange(
                "http://localhost:" + port + "/internal/v1/settlement/broker-settlement-fails/import-json?source=it",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(), headers()),
                SettlementController.BrokerSettlementFailImportResponse.class);
    }

    private static String envelope(String fileId, String executionRef) {
        return """
                {
                  "schemaVersion": 1,
                  "brokerId": "DEFAULT",
                  "fileId": "%s",
                  "businessDate": "2026-05-27",
                  "fails": [
                    {
                      "brokerFailId": "bf-1",
                      "brokerTradeId": "bt-1",
                      "executionRef": "%s",
                      "instrumentSymbol": "AAPL",
                      "side": "BUY",
                      "failedQuantity": "3",
                      "intendedSettlementDate": "2026-05-27",
                      "failReason": "sec_not_delivered",
                      "resolutionStatus": "open"
                    }
                  ]
                }
                """
                .formatted(fileId, executionRef);
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }
}
