package com.balh.oms.events;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies JetStream stream bootstrap + publish against a real NATS server.
 */
@Testcontainers(disabledWithoutDocker = true)
class NatsJetStreamPublishTest {

    @Container
    static final GenericContainer<?> NATS = new GenericContainer<>(DockerImageName.parse("nats:2.10.22-alpine"))
            .withExposedPorts(4222)
            .withCommand("-js");

    @Test
    void publishesOrderAcceptedAndIncreasesStreamMessageCount() throws Exception {
        OmsConfig cfg = new OmsConfig();
        cfg.getEvents().getNats().setUrl(
                "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222));
        cfg.getEvents().getNats().setSubjectPrefix("oms.events");
        cfg.getEvents().getNats().setStreamName("OMS_EVENTS");

        ObjectMapper om = new ObjectMapper().registerModule(new JavaTimeModule());
        Options opts = new Options.Builder()
                .server(cfg.getEvents().getNats().getUrl())
                .connectionTimeout(Duration.ofMillis(cfg.getEvents().getNats().getConnectionTimeoutMs()))
                .build();
        try (Connection nc = Nats.connect(opts)) {
            NatsDomainEventPublisher pub = new NatsDomainEventPublisher(nc, cfg, om, new SimpleMeterRegistry());
            pub.publish(new OrderAcceptedEvent(
                    UUID.randomUUID(),
                    0,
                    0,
                    "hash",
                    "BUY",
                    "AAPL",
                    new BigDecimal("1"),
                    new BigDecimal("2"),
                    "DAY",
                    Instant.now()));
            StreamInfo si = nc.jetStreamManagement().getStreamInfo("OMS_EVENTS");
            assertThat(si.getStreamState().getMsgCount()).isGreaterThanOrEqualTo(1L);
        }
    }
}
