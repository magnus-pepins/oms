package com.balh.oms.reconciler;

import com.balh.oms.AbstractPostgresIntegrationTest;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.Subscription;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end: Postgres domain outbox + {@link DomainFanoutReconciler} with real NATS JetStream
 * (message visible on {@code oms.events.OrderAccepted}).
 */
@Testcontainers(disabledWithoutDocker = true)
class NatsDomainFanoutIntegrationTest extends AbstractPostgresIntegrationTest {

    @Container
    static final GenericContainer<?> NATS = new GenericContainer<>(DockerImageName.parse("nats:2.10.22-alpine"))
            .withExposedPorts(4222)
            .withCommand("-js");

    @DynamicPropertySource
    static void registerNats(DynamicPropertyRegistry registry) {
        registry.add("oms.events.nats.enabled", () -> "true");
        registry.add("oms.events.nats.url", () ->
                "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222));
    }

    @LocalServerPort int port;
    @Autowired TestRestTemplate http;
    @Autowired DomainFanoutReconciler domainFanoutReconciler;

    @Test
    void reconcilerPublishesOrderAcceptedEnvelopeToJetStreamSubject() throws Exception {
        Options opts = new Options.Builder()
                .server("nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222))
                .connectionTimeout(Duration.ofMillis(5000))
                .build();

        try (Connection nc = Nats.connect(opts)) {
            Subscription sub = nc.subscribe("oms.events.OrderAccepted");

            UUID accountId = UUID.randomUUID();
            ResponseEntity<Map<String, Object>> res = http.exchange(
                    "http://localhost:" + port + "/internal/v1/orders",
                    HttpMethod.POST,
                    new HttpEntity<>(jsonBody(accountId, "nats-fanout-1"), authHeaders()),
                    new ParameterizedTypeReference<>() {});
            assertThat(res.getStatusCode().is2xxSuccessful()).isTrue();

            domainFanoutReconciler.runOnce();

            Message m = sub.nextMessage(Duration.ofSeconds(5).toMillis());
            assertThat(m).isNotNull();
            String payload = new String(m.getData(), StandardCharsets.UTF_8);
            assertThat(payload).contains("\"type\":\"OrderAccepted\"");
            assertThat(payload).contains("\"schemaVersion\"");
        }
    }

    private static HttpHeaders authHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
    }

    private static String jsonBody(UUID accountId, String key) {
        return """
                {
                  "accountId": "%s",
                  "clientIdempotencyKey": "%s",
                  "side": "BUY",
                  "instrumentSymbol": "AAPL",
                  "quantity": "1",
                  "limitPrice": "5.00",
                  "timeInForce": "DAY"
                }
                """.formatted(accountId, key);
    }
}
