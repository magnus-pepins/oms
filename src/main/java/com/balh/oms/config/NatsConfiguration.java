package com.balh.oms.config;

import com.balh.oms.events.DomainEventPublisher;
import com.balh.oms.events.NatsDomainEventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.time.Duration;

@Configuration
@ConditionalOnProperty(prefix = "oms.events.nats", name = "enabled", havingValue = "true")
public class NatsConfiguration {

    @Bean(destroyMethod = "close")
    Connection omsNatsConnection(OmsConfig config) throws IOException, InterruptedException {
        var nats = config.getEvents().getNats();
        Options opts = new Options.Builder()
                .server(nats.getUrl())
                .connectionTimeout(Duration.ofMillis(nats.getConnectionTimeoutMs()))
                .build();
        return Nats.connect(opts);
    }

    @Bean(name = "natsDomainEventPublisher")
    DomainEventPublisher natsDomainEventPublisher(
            Connection omsNatsConnection,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        return new NatsDomainEventPublisher(omsNatsConnection, config, objectMapper, meterRegistry);
    }
}
