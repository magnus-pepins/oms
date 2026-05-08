package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.persistence.FixRouteStateRepository;
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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FixRouteStateControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    FixRouteStateRepository fixRouteStateRepository;

    @Test
    void sodEnableSendOnAllRoutes_setsSendEnabledTrue() {
        fixRouteStateRepository.updateSendEnabled("default", false, "it", "off");
        assertThat(fixRouteStateRepository.findByRouteKey("default").orElseThrow().sendEnabled()).isFalse();

        int n = fixRouteStateRepository.sodEnableSendOnAllRoutes("sod-test");
        assertThat(n).isEqualTo(1);
        assertThat(fixRouteStateRepository.findByRouteKey("default").orElseThrow().sendEnabled()).isTrue();
    }

    @Test
    void rejectsWithoutInternalApiKey() {
        ResponseEntity<String> res = http.getForEntity(
                url("default"), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void getSeededDefault_returnsSendEnabled() {
        ResponseEntity<FixRouteStateResponse> res = http.exchange(
                url("default"),
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().routeKey()).isEqualTo("default");
        assertThat(res.getBody().sendEnabled()).isTrue();
    }

    @Test
    void patchTogglesSendEnabled_roundTrip() {
        HttpHeaders h = headers();
        Map<String, Object> off = new LinkedHashMap<>();
        off.put("sendEnabled", false);
        off.put("note", "it-off");
        off.put("updatedBy", "FixRouteStateControllerIntegrationTest");

        ResponseEntity<FixRouteStateResponse> offRes = http.exchange(
                url("default"),
                HttpMethod.PATCH,
                new HttpEntity<>(off, h),
                new ParameterizedTypeReference<>() {});
        assertThat(offRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(offRes.getBody()).isNotNull();
        assertThat(offRes.getBody().sendEnabled()).isFalse();
        assertThat(offRes.getBody().note()).isEqualTo("it-off");

        Map<String, Object> on = new LinkedHashMap<>();
        on.put("sendEnabled", true);
        ResponseEntity<FixRouteStateResponse> onRes = http.exchange(
                url("default"),
                HttpMethod.PATCH,
                new HttpEntity<>(on, h),
                new ParameterizedTypeReference<>() {});
        assertThat(onRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(onRes.getBody()).isNotNull();
        assertThat(onRes.getBody().sendEnabled()).isTrue();
    }

    @Test
    void getUnknownRoute_returns404() {
        ResponseEntity<String> res = http.exchange(
                url("no-such-route"),
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void patchUnknownRoute_returns404() {
        Map<String, Object> body = Map.of("sendEnabled", true);
        ResponseEntity<String> res = http.exchange(
                url("no-such-route"),
                HttpMethod.PATCH,
                new HttpEntity<>(body, headers()),
                String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private String url(String routeKey) {
        return "http://localhost:" + port + "/internal/v1/fix/route-state/" + routeKey;
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
    }
}
