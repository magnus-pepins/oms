package com.balh.oms.config;

import com.balh.oms.events.FanoutClient;
import com.balh.oms.events.NoOpFanoutClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FanoutClientConfiguration {

    /**
     * When {@code oms.events.nats.enabled=true}, {@link NatsConfiguration} supplies {@code natsFanoutClient}.
     * This noop bean must not register in that case: {@code @ConditionalOnMissingBean} alone is evaluated
     * before NATS beans are registered, so both implementations would exist and autowiring {@link FanoutClient}
     * fails.
     */
    @Bean
    @ConditionalOnProperty(
            prefix = "oms.events.nats",
            name = "enabled",
            havingValue = "false",
            matchIfMissing = true)
    @ConditionalOnMissingBean(FanoutClient.class)
    FanoutClient noopFanoutClient(ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        return new NoOpFanoutClient(objectMapper, meterRegistry);
    }
}
