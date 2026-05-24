package com.balh.oms.config;

import com.balh.oms.settlement.CustomerNotificationPublisher;
import com.balh.oms.settlement.NatsCustomerNotificationPublisher;
import com.balh.oms.settlement.NoOpCustomerNotificationPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CustomerNotificationPublisherConfiguration {

    @Bean(name = "customerNotificationPublisher")
    @ConditionalOnProperty(prefix = "oms.events.nats", name = "enabled", havingValue = "true")
    CustomerNotificationPublisher natsCustomerNotificationPublisher(
            Connection omsNatsConnection,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        return new NatsCustomerNotificationPublisher(omsNatsConnection, config, objectMapper, meterRegistry);
    }

    @Bean(name = "customerNotificationPublisher")
    @ConditionalOnProperty(
            prefix = "oms.events.nats",
            name = "enabled",
            havingValue = "false",
            matchIfMissing = true)
    @ConditionalOnMissingBean(name = "customerNotificationPublisher")
    CustomerNotificationPublisher noopCustomerNotificationPublisher(
            ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        return new NoOpCustomerNotificationPublisher(objectMapper, meterRegistry);
    }
}
