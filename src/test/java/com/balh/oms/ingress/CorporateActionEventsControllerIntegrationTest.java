package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CorporateActionEventsControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void ingestCreatesRow() {
        String url = "http://localhost:" + port + "/internal/v1/corporate-action-events";
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        Map<String, Object> body =
                Map.of(
                        "instrumentSymbol",
                        "AAPL",
                        "actionType",
                        "CASH_DIVIDEND",
                        "effectiveDate",
                        "2026-05-10",
                        "payloadJson",
                        Map.of("amount", "0.25"));

        ResponseEntity<CorporateActionEventsController.IngestResponse> res =
                http.exchange(
                        url,
                        HttpMethod.POST,
                        new HttpEntity<>(body, h),
                        new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(res.getBody()).isNotNull();
        long id = res.getBody().id();
        Integer processedNull =
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM corporate_action_event WHERE id = ? AND processed_at IS NULL",
                        Integer.class,
                        id);
        assertThat(processedNull).isEqualTo(1);
    }

    @Test
    void listAndGetDetail_roundTrip() {
        String url = "http://localhost:" + port + "/internal/v1/corporate-action-events";
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        Map<String, Object> body =
                Map.of(
                        "instrumentSymbol",
                        "MSFT",
                        "actionType",
                        "SPLIT",
                        "effectiveDate",
                        "2026-06-01",
                        "payloadJson",
                        Map.of("ratio", "2:1"));
        ResponseEntity<CorporateActionEventsController.IngestResponse> postRes =
                http.exchange(
                        url,
                        HttpMethod.POST,
                        new HttpEntity<>(body, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(postRes.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        long id = postRes.getBody().id();

        ResponseEntity<Map> listRes =
                http.exchange(
                        url + "?limit=10&offset=0",
                        HttpMethod.GET,
                        new HttpEntity<>(h),
                        Map.class);
        assertThat(listRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listRes.getBody()).isNotNull();

        ResponseEntity<Map> getRes =
                http.exchange(
                        url + "/" + id,
                        HttpMethod.GET,
                        new HttpEntity<>(h),
                        Map.class);
        assertThat(getRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getRes.getBody()).isNotNull();
        assertThat(getRes.getBody().get("instrumentSymbol")).isEqualTo("MSFT");
    }

    @Test
    void rejectsWithoutApiKey() {
        String url = "http://localhost:" + port + "/internal/v1/corporate-action-events";
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> res =
                http.exchange(url, HttpMethod.POST, new HttpEntity<>(Map.of(), h), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}
