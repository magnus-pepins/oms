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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeFlagsControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void rejectsWithoutInternalApiKey() {
        ResponseEntity<String> res = http.getForEntity(url(), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void getWhenNoRow_returnsFalseAndNullUpdatedAt() {
        jdbc.update("DELETE FROM oms_runtime_flags WHERE flag_key = 'global_halt'");
        ResponseEntity<GlobalHaltResponse> res =
                http.exchange(url(), HttpMethod.GET, new HttpEntity<>(headers()), new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().globalHalt()).isFalse();
        assertThat(res.getBody().updatedAt()).isNull();
    }

    @Test
    void patchRoundTrip_updatesRow() {
        jdbc.update("DELETE FROM oms_runtime_flags WHERE flag_key = 'global_halt'");
        HttpHeaders h = headers();
        Map<String, Object> on = new LinkedHashMap<>();
        on.put("globalHalt", true);
        on.put("updatedBy", "RuntimeFlagsControllerIntegrationTest");

        ResponseEntity<GlobalHaltResponse> onRes =
                http.exchange(
                        url(),
                        HttpMethod.PATCH,
                        new HttpEntity<>(on, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(onRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(onRes.getBody()).isNotNull();
        assertThat(onRes.getBody().globalHalt()).isTrue();
        assertThat(onRes.getBody().updatedAt()).isNotNull();

        ResponseEntity<GlobalHaltResponse> getRes =
                http.exchange(url(), HttpMethod.GET, new HttpEntity<>(h), new ParameterizedTypeReference<>() {});
        assertThat(getRes.getBody()).isNotNull();
        assertThat(getRes.getBody().globalHalt()).isTrue();

        Map<String, Object> off = Map.of("globalHalt", false);
        ResponseEntity<GlobalHaltResponse> offRes =
                http.exchange(
                        url(),
                        HttpMethod.PATCH,
                        new HttpEntity<>(off, h),
                        new ParameterizedTypeReference<>() {});
        assertThat(offRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(offRes.getBody()).isNotNull();
        assertThat(offRes.getBody().globalHalt()).isFalse();
    }

    private String url() {
        return "http://localhost:" + port + "/internal/v1/runtime-flags/global_halt";
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
    }
}
