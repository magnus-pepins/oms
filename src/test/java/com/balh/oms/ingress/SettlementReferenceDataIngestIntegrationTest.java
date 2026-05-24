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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT for the two operator JSON ingest endpoints introduced in gap plan §5.3 Slice 2b-4:
 *
 * <ul>
 *   <li>{@code POST /internal/v1/settlement/instrument-profiles/import-json} —
 *       {@code instrument_settlement_profile} upsert (V61).</li>
 *   <li>{@code POST /internal/v1/settlement/settlement-calendars/import-json} —
 *       {@code settlement_calendar} upsert (V63).</li>
 * </ul>
 *
 * <p>Coverage:
 *
 * <ul>
 *   <li>insert + upsert lands the right counts on the response;</li>
 *   <li>per-row validation rejects invalid rows without poisoning the batch;</li>
 *   <li>oversized / malformed payloads return 400, not 500;</li>
 *   <li>auth filter shape matches the rest of {@code /internal/v1/**}.</li>
 * </ul>
 */
class SettlementReferenceDataIngestIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void truncate() {
        // The plan's V63 seed lives in settlement_calendar; we own these two tables here so
        // we clean them ourselves rather than extending SQL_TRUNCATE_ORDERS_AND_SETTLEMENT.
        // For the calendar table we keep the 2026 V63 seed (the calendar repo IT depends on
        // it) but drop everything dated >= 2027 — the test bodies below all live in 2027
        // precisely so this boundary works.
        jdbc.update("TRUNCATE TABLE instrument_settlement_profile RESTART IDENTITY");
        jdbc.update("DELETE FROM settlement_calendar WHERE holiday_date >= DATE '2027-01-01'");
    }

    // -------------------------------------------------------------------------
    // instrument_settlement_profile ingest
    // -------------------------------------------------------------------------

    @Test
    void profileImport_insertsNewRows_returns200AndCounts() {
        String body = """
                {
                  "rows": [
                    {
                      "instrumentId": "US0378331005",
                      "symbol": "AAPL",
                      "isin": "US0378331005",
                      "primaryMic": "XNAS",
                      "settlementCalendarId": "XNAS-CAL",
                      "settlementCycle": "T+1",
                      "settlementCurrency": "USD",
                      "iskEligible": false,
                      "effectiveFrom": "2024-05-28"
                    },
                    {
                      "instrumentId": "SE0000108656",
                      "symbol": "ERIC-B.ST",
                      "isin": "SE0000108656",
                      "primaryMic": "XSTO",
                      "settlementCalendarId": "XSTO-CAL",
                      "settlementCycle": "T+2",
                      "settlementCurrency": "SEK",
                      "iskEligible": true,
                      "effectiveFrom": "2024-01-01",
                      "effectiveTo": "2027-10-11"
                    }
                  ]
                }
                """;

        var res = postProfileJson(body);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().inserted()).isEqualTo(2);
        assertThat(res.getBody().updated()).isZero();
        assertThat(res.getBody().rejected()).isEmpty();

        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM instrument_settlement_profile", Integer.class);
        assertThat(count).isEqualTo(2);
        assertThat(jdbc.queryForObject(
                        "SELECT settlement_cycle FROM instrument_settlement_profile WHERE symbol = ?",
                        String.class,
                        "AAPL"))
                .isEqualTo("T+1");
    }

    @Test
    void profileImport_secondCallOnSameNaturalKey_updatesExistingRow() {
        String first = """
                {"rows":[{
                  "instrumentId":"US0378331005",
                  "symbol":"AAPL",
                  "primaryMic":"XNAS",
                  "settlementCalendarId":"XNAS-CAL",
                  "settlementCycle":"T+2",
                  "settlementCurrency":"USD",
                  "effectiveFrom":"2024-05-28"
                }]}
                """;
        String corrected = """
                {"rows":[{
                  "instrumentId":"US0378331005",
                  "symbol":"AAPL",
                  "primaryMic":"XNAS",
                  "settlementCalendarId":"XNAS-CAL",
                  "settlementCycle":"T+1",
                  "settlementCurrency":"USD",
                  "effectiveFrom":"2024-05-28"
                }]}
                """;

        var firstRes = postProfileJson(first);
        assertThat(firstRes.getBody()).isNotNull();
        assertThat(firstRes.getBody().inserted()).isEqualTo(1);

        var secondRes = postProfileJson(corrected);
        assertThat(secondRes.getBody()).isNotNull();
        assertThat(secondRes.getBody().inserted()).isZero();
        assertThat(secondRes.getBody().updated()).isEqualTo(1);

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_cycle FROM instrument_settlement_profile WHERE symbol = ?",
                        String.class,
                        "AAPL"))
                .isEqualTo("T+1");
        Integer rowCount = jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM instrument_settlement_profile", Integer.class);
        assertThat(rowCount).isEqualTo(1);
    }

    @Test
    void profileImport_invalidRowReturnsPerRowRejectionAndOtherRowsApply() {
        // Mix: row 0 valid, row 1 bad cycle (off-allowlist), row 2 valid.
        String body = """
                {"rows":[
                  {"instrumentId":"US0378331005","symbol":"AAPL","primaryMic":"XNAS",
                   "settlementCalendarId":"XNAS-CAL","settlementCycle":"T+1",
                   "settlementCurrency":"USD","effectiveFrom":"2024-05-28"},
                  {"instrumentId":"US12345","symbol":"BAD","primaryMic":"XNAS",
                   "settlementCalendarId":"XNAS-CAL","settlementCycle":"T+7",
                   "settlementCurrency":"USD","effectiveFrom":"2024-05-28"},
                  {"instrumentId":"SE0000108656","symbol":"ERIC-B.ST","primaryMic":"XSTO",
                   "settlementCalendarId":"XSTO-CAL","settlementCycle":"T+2",
                   "settlementCurrency":"SEK","effectiveFrom":"2024-01-01"}
                ]}
                """;

        var res = postProfileJson(body);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().inserted()).isEqualTo(2);
        assertThat(res.getBody().updated()).isZero();
        assertThat(res.getBody().rejected()).hasSize(1);
        assertThat(res.getBody().rejected().get(0).rowIndex()).isEqualTo(1);
        assertThat(res.getBody().rejected().get(0).reason()).contains("settlementCycle");

        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM instrument_settlement_profile WHERE symbol IN (?, ?, ?)",
                Integer.class,
                "AAPL",
                "BAD",
                "ERIC-B.ST");
        assertThat(count).isEqualTo(2);
    }

    @Test
    void profileImport_malformedJson_returns400() {
        var res = postProfileJson("not valid json");
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void profileImport_emptyBody_returns400() {
        HttpHeaders h = jsonHeaders();
        var res = http.exchange(
                base() + "/instrument-profiles/import-json",
                HttpMethod.POST,
                new HttpEntity<>(new byte[0], h),
                new ParameterizedTypeReference<SettlementController.SettlementProfileIngestResponse>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // -------------------------------------------------------------------------
    // settlement_calendar ingest
    // -------------------------------------------------------------------------

    @Test
    void calendarImport_insertsNewHolidays_returns200AndCounts() {
        // Use 2027 so we don't collide with the V63 seed.
        String body = """
                {"rows":[
                  {"calendarId":"XSTO-CAL","holidayDate":"2027-01-01","description":"New Year's Day"},
                  {"calendarId":"XSTO-CAL","holidayDate":"2027-04-02","description":"Good Friday"},
                  {"calendarId":"XNAS-CAL","holidayDate":"2027-01-01","description":"New Year's Day"}
                ]}
                """;

        var res = postCalendarJson(body);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().inserted()).isEqualTo(3);
        assertThat(res.getBody().updated()).isZero();
        assertThat(res.getBody().rejected()).isEmpty();

        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM settlement_calendar WHERE holiday_date >= DATE '2027-01-01'",
                Integer.class);
        assertThat(count).isEqualTo(3);
    }

    @Test
    void calendarImport_reupsertSameRow_updatesDescription() {
        String first = """
                {"rows":[{"calendarId":"XSTO-CAL","holidayDate":"2027-05-01","description":"Labour Day"}]}
                """;
        String corrected = """
                {"rows":[{"calendarId":"XSTO-CAL","holidayDate":"2027-05-01","description":"Labour Day (corrected)"}]}
                """;

        var r1 = postCalendarJson(first);
        var r2 = postCalendarJson(corrected);

        assertThat(r1.getBody()).isNotNull();
        assertThat(r2.getBody()).isNotNull();
        assertThat(r1.getBody().inserted()).isEqualTo(1);
        assertThat(r2.getBody().updated()).isEqualTo(1);

        assertThat(jdbc.queryForObject(
                        "SELECT description FROM settlement_calendar WHERE calendar_id = ? AND holiday_date = DATE '2027-05-01'",
                        String.class,
                        "XSTO-CAL"))
                .isEqualTo("Labour Day (corrected)");
    }

    @Test
    void calendarImport_invalidRowReturnsRejection_otherRowsApply() {
        // Row 1 is missing holidayDate.
        String body = """
                {"rows":[
                  {"calendarId":"XSTO-CAL","holidayDate":"2027-06-04","description":"National Day"},
                  {"calendarId":"XSTO-CAL","description":"missing date"},
                  {"calendarId":"XSTO-CAL","holidayDate":"2027-06-18","description":"Midsummer Eve"}
                ]}
                """;

        var res = postCalendarJson(body);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().inserted()).isEqualTo(2);
        assertThat(res.getBody().rejected()).hasSize(1);
        assertThat(res.getBody().rejected().get(0).rowIndex()).isEqualTo(1);
    }

    @Test
    void calendarImport_emptyBody_returns400() {
        HttpHeaders h = jsonHeaders();
        var res = http.exchange(
                base() + "/settlement-calendars/import-json",
                HttpMethod.POST,
                new HttpEntity<>(new byte[0], h),
                new ParameterizedTypeReference<SettlementController.SettlementCalendarIngestResponse>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private ResponseEntity<SettlementController.SettlementProfileIngestResponse> postProfileJson(String body) {
        HttpHeaders h = jsonHeaders();
        return http.exchange(
                base() + "/instrument-profiles/import-json",
                HttpMethod.POST,
                new HttpEntity<>(body.getBytes(StandardCharsets.UTF_8), h),
                new ParameterizedTypeReference<>() {});
    }

    private ResponseEntity<SettlementController.SettlementCalendarIngestResponse> postCalendarJson(String body) {
        HttpHeaders h = jsonHeaders();
        return http.exchange(
                base() + "/settlement-calendars/import-json",
                HttpMethod.POST,
                new HttpEntity<>(body.getBytes(StandardCharsets.UTF_8), h),
                new ParameterizedTypeReference<>() {});
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
