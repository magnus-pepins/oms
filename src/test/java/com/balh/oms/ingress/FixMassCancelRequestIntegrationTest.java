package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "oms.routing.backend=fix",
            "oms.fix.manual-mass-cancel-enabled=true",
            "oms.fix.manual-mass-cancel-wire-enabled=false",
            "oms.fix.auto-start=false"
        })
class FixMassCancelRequestIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Test
    void signalOnly_returnsAccepted() {
        HttpHeaders h = new HttpHeaders();
        h.set("X-OMS-Internal-Key", "test-key");
        h.setContentType(MediaType.APPLICATION_JSON);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("requestedBy", "FixMassCancelRequestIntegrationTest");
        body.put("reason", "it-signal");
        body.put("wire", false);
        ResponseEntity<FixMassCancelRequestController.MassCancelResponseBody> res =
                http.exchange(
                        "http://localhost:" + port + "/internal/v1/fix/mass-cancel-request",
                        HttpMethod.POST,
                        new HttpEntity<>(body, h),
                        FixMassCancelRequestController.MassCancelResponseBody.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().mode()).isEqualTo("signal_only");
    }
}
