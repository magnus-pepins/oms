package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.ingress.ApiKeyFilter;
import com.balh.oms.settlement.BrokerCorporateActionApplyService;
import com.balh.oms.settlement.BrokerCorporateActionIngestService;
import com.balh.oms.settlement.CorporateActionReconciliationService;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/** CA broker reconciliation (gap plan §5.9 Phase 1 tail). */
class BrokerCorporateActionReconciliationIntegrationTest extends AbstractPostgresIntegrationTest {

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("oms.corporate-action.processor.enabled", () -> "false");
    }

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @LocalServerPort
    int port;

    @Test
    void reconcile_afterApply_matchesBrokerEvent() {
        String fileId = "CA-RECON-" + UUID.randomUUID();
        String brokerEventId = "ca-ev-recon-" + UUID.randomUUID();
        long batchId = ingestBatch(fileId, brokerEventId);
        assertThat(postApply(batchId).getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<SettlementController.CorporateActionReconciliationResponse> recon = postReconcile(batchId);
        assertThat(recon.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recon.getBody()).isNotNull();
        assertThat(recon.getBody().matchedCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'corporate_action_mismatch'",
                        Integer.class))
                .isZero();
    }

    private long ingestBatch(String fileId, String brokerEventId) {
        ResponseEntity<SettlementController.BrokerCorporateActionImportResponse> res =
                http.exchange(
                        base() + "/broker-corporate-actions/import-json?source=it",
                        HttpMethod.POST,
                        new HttpEntity<>(envelope(fileId, brokerEventId).getBytes(), headers()),
                        SettlementController.BrokerCorporateActionImportResponse.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        return res.getBody().batchId();
    }

    private ResponseEntity<SettlementController.BrokerCorporateActionApplyResponse> postApply(long batchId) {
        return http.exchange(
                base() + "/broker-corporate-actions/batches/" + batchId + "/apply",
                HttpMethod.POST,
                new HttpEntity<>(headers()),
                SettlementController.BrokerCorporateActionApplyResponse.class);
    }

    private ResponseEntity<SettlementController.CorporateActionReconciliationResponse> postReconcile(long batchId) {
        return http.exchange(
                base() + "/broker-corporate-actions/batches/" + batchId + "/reconcile",
                HttpMethod.POST,
                new HttpEntity<>(headers()),
                SettlementController.CorporateActionReconciliationResponse.class);
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

    private HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }

    private String base() {
        return "http://localhost:" + port + "/internal/v1/settlement";
    }
}
