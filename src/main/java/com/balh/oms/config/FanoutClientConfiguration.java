package com.balh.oms.config;

import com.balh.oms.events.FanoutClient;
import com.balh.oms.events.NoOpFanoutClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FanoutClientConfiguration {

    @Bean
    @ConditionalOnMissingBean(FanoutClient.class)
    FanoutClient noopFanoutClient(ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        return new NoOpFanoutClient(objectMapper, meterRegistry);
    }
}
