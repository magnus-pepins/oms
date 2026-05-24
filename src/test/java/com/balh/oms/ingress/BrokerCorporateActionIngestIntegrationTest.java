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

/** Phase D Slice 11a (gap plan §5.9): broker corporate-action file ingest + apply. */
class BrokerCorporateActionIngestIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void importAndApply_persistsCorporateActionEvent() {
        long batchId = ingestBatch("CA-F1", "ca-ev-1");
        ResponseEntity<SettlementController.BrokerCorporateActionApplyResponse> apply = postApply(batchId);

        assertThat(apply.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(apply.getBody()).isNotNull();
        assertThat(apply.getBody().insertedCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM corporate_action_event WHERE broker_event_id = 'ca-ev-1'",
                        Integer.class))
                .isEqualTo(1);
    }

    @Test
    void apply_duplicateBrokerEvent_isIdempotent() {
        long batchId = ingestBatch("CA-F2", "ca-ev-dup");
        postApply(batchId);
        long batchId2 = ingestBatch("CA-F2B", "ca-ev-dup");
        ResponseEntity<SettlementController.BrokerCorporateActionApplyResponse> second = postApply(batchId2);

        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().duplicateCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM corporate_action_event WHERE broker_event_id = 'ca-ev-dup'",
                        Integer.class))
                .isEqualTo(1);
    }

    private long ingestBatch(String fileId, String brokerEventId) {
        String json = envelope(fileId, brokerEventId);
        ResponseEntity<SettlementController.BrokerCorporateActionImportResponse> res = http.exchange(
                base() + "/broker-corporate-actions/import-json?source=it",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(), headers()),
                SettlementController.BrokerCorporateActionImportResponse.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        return res.getBody().batchId();
    }

    private ResponseEntity<SettlementController.BrokerCorporateActionApplyResponse> postApply(long batchId) {
        return http.exchange(
                base() + "/broker-corporate-actions/batches/" + batchId + "/apply",
                HttpMethod.POST,
                new HttpEntity<>(headers()),
                SettlementController.BrokerCorporateActionApplyResponse.class);
    }

    private static String envelope(String fileId, String brokerEventId) {
        return """
                {
                  "schemaVersion": 1,
                  "brokerId": "DEFAULT",
                  "fileId": "%s",
                  "businessDate": "2026-05-27",
                  "events": [
                    {
                      "brokerEventId": "%s",
                      "instrumentSymbol": "AAPL",
                      "actionType": "CASH_DIVIDEND",
                      "effectiveDate": "2026-06-01",
                      "payableDate": "2026-06-15"
                    }
                  ]
                }
                """
                .formatted(fileId, brokerEventId);
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
