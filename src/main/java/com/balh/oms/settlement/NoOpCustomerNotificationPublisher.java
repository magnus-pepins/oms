package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** No-op transport for tests and stacks without NATS. */
public final class NoOpCustomerNotificationPublisher implements CustomerNotificationPublisher {

    private static final Logger log = LoggerFactory.getLogger(NoOpCustomerNotificationPublisher.class);

    private final ObjectMapper objectMapper;
    private final Counter delivered;

    public NoOpCustomerNotificationPublisher(ObjectMapper objectMapper, MeterRegistry registry) {
        this.objectMapper = objectMapper;
        this.delivered = Counter.builder("oms_settlement_customer_notification_delivered_total")
                .tag("transport", "noop")
                .description("Settlement customer notification envelopes handed to the transport")
                .register(registry);
    }

    @Override
    public boolean deliver(String envelopeJson) {
        try {
            JsonNode n = objectMapper.readTree(envelopeJson);
            String type = n.path("type").asText("unknown");
            log.trace("noop customer notification type={}", type);
            delivered.increment();
            return true;
        } catch (Exception e) {
            log.warn("noop customer notification failed to parse envelope", e);
            return false;
        }
    }
}
