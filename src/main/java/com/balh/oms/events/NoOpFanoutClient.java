package com.balh.oms.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Default fanout transport: counts deliveries without sending anywhere.
 */
public final class NoOpFanoutClient implements FanoutClient {

    private static final Logger log = LoggerFactory.getLogger(NoOpFanoutClient.class);

    private final ObjectMapper objectMapper;
    private final Counter delivered;

    public NoOpFanoutClient(ObjectMapper objectMapper, MeterRegistry registry) {
        this.objectMapper = objectMapper;
        this.delivered = Counter.builder("oms_fanout_envelope_delivered_total")
                .tag("transport", "noop")
                .description("Domain fanout envelopes handed to the transport")
                .register(registry);
    }

    @Override
    public boolean deliver(String envelopeJson) {
        try {
            JsonNode n = objectMapper.readTree(envelopeJson);
            String type = n.path("type").asText("unknown");
            log.trace("noop fanout type={}", type);
            delivered.increment();
            return true;
        } catch (Exception e) {
            log.warn("noop fanout failed to parse envelope", e);
            return false;
        }
    }
}
