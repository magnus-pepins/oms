package com.balh.oms.reconciler;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.settlement.SettlementCustomerNotificationOutboxRepository;
import com.balh.oms.settlement.SettlementCustomerNotificationTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.Subscription;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class SettlementCustomerNotificationOutboxReconcilerIT extends AbstractPostgresIntegrationTest {

    @Container
    static final GenericContainer<?> NATS =
            new GenericContainer<>(DockerImageName.parse("nats:2.10.22-alpine"))
                    .withExposedPorts(4222)
                    .withCommand("-js");

    @DynamicPropertySource
    static void registerNatsAndPublisher(DynamicPropertyRegistry registry) {
        registry.add("oms.events.nats.enabled", () -> "true");
        registry.add("oms.events.nats.url", () ->
                "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222));
        registry.add("oms.settlement.customer-notification-publisher-enabled", () -> "true");
        registry.add("oms.settlement.customer-notification-publisher-age-ms", () -> "0");
    }

    @Autowired SettlementCustomerNotificationOutboxRepository outbox;
    @Autowired SettlementCustomerNotificationOutboxReconciler reconciler;
    @Autowired ObjectMapper objectMapper;

    @Test
    void reconcilerPublishesSettlementDelayedToJetStream() throws Exception {
        Options opts =
                new Options.Builder()
                        .server("nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222))
                        .connectionTimeout(Duration.ofMillis(5000))
                        .build();

        try (Connection nc = Nats.connect(opts)) {
            Subscription sub = nc.subscribe("oms.customer-notifications.SettlementDelayed");

            UUID accountId = UUID.randomUUID();
            ObjectNode envelope = objectMapper.createObjectNode();
            envelope.put("schemaVersion", 1);
            envelope.put("type", SettlementCustomerNotificationTypes.SETTLEMENT_DELAYED);
            envelope.put("accountId", accountId.toString());
            envelope.put("instrumentSymbol", "AAPL");
            outbox.insertIgnore(
                    SettlementCustomerNotificationTypes.SETTLEMENT_DELAYED,
                    accountId,
                    1L,
                    2L,
                    objectMapper.writeValueAsString(envelope));

            reconciler.runOnce();

            Message m = sub.nextMessage(Duration.ofSeconds(5).toMillis());
            assertThat(m).isNotNull();
            String payload = new String(m.getData(), StandardCharsets.UTF_8);
            assertThat(payload).contains("SettlementDelayed");
            assertThat(payload).contains(accountId.toString());
        }
    }
}
