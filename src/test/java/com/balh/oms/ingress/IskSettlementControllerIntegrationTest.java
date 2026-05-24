package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class IskSettlementControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired TestRestTemplate rest;
    @Autowired NamedParameterJdbcTemplate jdbc;

    UUID accountId;
    UUID iskAccountId;

    @BeforeEach
    void seedIskAccount() {
        accountId = UUID.randomUUID();
        iskAccountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO oms_account_tax_wrapper (
                            account_id, tax_wrapper, isk_account_id, ledger_balance_id
                        ) VALUES (:accountId, 'SE_ISK', :iskAccountId, 'balance_test_isk')
                        ON CONFLICT (account_id) DO NOTHING
                        """,
                new MapSqlParameterSource()
                        .addValue("accountId", accountId)
                        .addValue("iskAccountId", iskAccountId));
    }

    @Test
    void captureValuationSnapshots_returnsQuarterSummary() {
        ResponseEntity<IskSettlementController.ValuationCaptureResponse> resp =
                rest.postForEntity(
                        "/internal/v1/settlement/isk/valuation-snapshots/capture?asOf=2026-03-15",
                        null,
                        IskSettlementController.ValuationCaptureResponse.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody()).isNotNull();
        assertThat(resp.getBody().quarterStart()).isEqualTo(LocalDate.of(2026, 1, 1));
        assertThat(resp.getBody().accountsCaptured()).isGreaterThanOrEqualTo(1);

        Integer rows =
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM isk_valuation_snapshot WHERE isk_account_id = :iskAccountId",
                        new MapSqlParameterSource("iskAccountId", iskAccountId),
                        Integer.class);
        assertThat(rows).isGreaterThanOrEqualTo(1);
    }

    @Test
    void exportKu30Draft_writesTaxYearExportRow() {
        rest.postForEntity(
                "/internal/v1/settlement/isk/valuation-snapshots/capture?asOf=2026-06-01",
                null,
                IskSettlementController.ValuationCaptureResponse.class);

        ResponseEntity<IskSettlementController.Ku30ExportResponse> resp =
                rest.postForEntity(
                        "/internal/v1/settlement/isk/ku30-export/2026",
                        null,
                        IskSettlementController.Ku30ExportResponse.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody()).isNotNull();
        assertThat(resp.getBody().taxYear()).isEqualTo(2026);
        assertThat(resp.getBody().accountCount()).isGreaterThanOrEqualTo(1);

        Integer exports =
                jdbc.queryForObject(
                        """
                                SELECT COUNT(*) FROM isk_tax_year_export
                                WHERE tax_year = 2026 AND isk_account_id = :iskAccountId
                                """,
                        new MapSqlParameterSource("iskAccountId", iskAccountId),
                        Integer.class);
        assertThat(exports).isEqualTo(1);
    }
}
