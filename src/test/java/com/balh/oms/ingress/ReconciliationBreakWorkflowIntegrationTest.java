package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.settlement.ReconciliationBreakRepository;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ReconciliationBreakWorkflowIntegrationTest extends AbstractPostgresIntegrationTest {

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
    void assignResolve_writesAuditEvents() {
        long breakId = seedOpenBreak();

        HttpHeaders h = jsonHeaders();
        ResponseEntity<ReconciliationBreakRepository.BreakRow> assignRes = http.exchange(
                base() + "/reconciliation-breaks/" + breakId + "/assign",
                HttpMethod.POST,
                new HttpEntity<>(
                        """
                                {"assignedTo":"ops@example.com","actor":"alice@example.com","notes":"triage started"}
                                """
                                .trim(),
                        h),
                ReconciliationBreakRepository.BreakRow.class);
        assertThat(assignRes.getStatusCode()).isEqualTo(HttpStatus.OK);

        String status = jdbc.queryForObject(
                "SELECT status FROM reconciliation_breaks WHERE id = ?", String.class, breakId);
        assertThat(status).isEqualTo("investigating");

        ResponseEntity<ReconciliationBreakRepository.BreakRow> resolveRes = http.exchange(
                base() + "/reconciliation-breaks/" + breakId + "/resolve",
                HttpMethod.POST,
                new HttpEntity<>(
                        """
                                {"resolutionCode":"broker_correction_pending","resolutionNote":"awaiting broker amend file","actor":"alice@example.com"}
                                """
                                .trim(),
                        h),
                ReconciliationBreakRepository.BreakRow.class);
        assertThat(resolveRes.getStatusCode()).isEqualTo(HttpStatus.OK);

        assertThat(jdbc.queryForObject(
                        "SELECT status FROM reconciliation_breaks WHERE id = ?", String.class, breakId))
                .isEqualTo("resolved");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_break_events WHERE break_id = ?",
                        Integer.class,
                        breakId))
                .isEqualTo(2);
    }

    @Test
    void waive_fromOpen_closesBreak() {
        long breakId = seedOpenBreak();

        HttpHeaders h = jsonHeaders();
        ResponseEntity<Void> res = http.exchange(
                base() + "/reconciliation-breaks/" + breakId + "/waive",
                HttpMethod.POST,
                new HttpEntity<>(
                        """
                                {"resolutionNote":"immaterial drift","actor":"bob@example.com"}
                                """
                                .trim(),
                        h),
                Void.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(jdbc.queryForObject(
                        "SELECT status FROM reconciliation_breaks WHERE id = ?", String.class, breakId))
                .isEqualTo("waived");
    }

    @Test
    void assign_onResolved_returns409() {
        long breakId = seedOpenBreak();
        HttpHeaders h = jsonHeaders();
        http.exchange(
                base() + "/reconciliation-breaks/" + breakId + "/waive",
                HttpMethod.POST,
                new HttpEntity<>(
                        "{\"resolutionNote\":\"done\",\"actor\":\"bob@example.com\"}", h),
                Void.class);

        ResponseEntity<Void> second = http.exchange(
                base() + "/reconciliation-breaks/" + breakId + "/assign",
                HttpMethod.POST,
                new HttpEntity<>(
                        "{\"assignedTo\":\"ops@example.com\",\"actor\":\"alice@example.com\"}", h),
                Void.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    private long seedOpenBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-WF-" + UUID.randomUUID();
        String json =
                """
                        {
                          "schemaVersion": 1,
                          "brokerId": "broker_x",
                          "fileId": "F-%s",
                          "businessDate": "2026-05-23",
                          "rows": [
                            {
                              "brokerTradeId": "BT-WF-%s",
                              "venueExecRef": "%s",
                              "accountId": "%s",
                              "instrument": { "symbol": "AAPL", "currency": "USD" },
                              "side": "BUY",
                              "quantity": "10",
                              "price": "5",
                              "grossAmount": "50",
                              "tradeDate": "2026-05-23",
                              "settlementDate": "2026-05-27",
                              "settlementCurrency": "USD",
                              "correctionType": "new"
                            }
                          ]
                        }
                        """
                        .formatted(UUID.randomUUID(), UUID.randomUUID(), venueExecRef, accountId);
        HttpHeaders h = jsonHeaders();
        http.exchange(
                base() + "/broker-trade-confirms/import-json?source=it-workflow",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(StandardCharsets.UTF_8), h),
                new ParameterizedTypeReference<SettlementController.BrokerTradeConfirmImportResponse>() {});
        http.exchange(
                base() + "/broker-trade-confirms/process-pending-matches",
                HttpMethod.POST,
                new HttpEntity<>(jsonHeaders()),
                new ParameterizedTypeReference<SettlementController.BrokerTradeMatchBatchResponse>() {});

        Long breakId = jdbc.queryForObject("SELECT id FROM reconciliation_breaks ORDER BY id DESC LIMIT 1", Long.class);
        assertThat(breakId).isNotNull();
        return breakId;
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
}
