package com.balh.oms.events;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * JetStream fanout: publishes the full transactional envelope JSON as the message body.
 */
public final class NatsFanoutClient implements FanoutClient {

    private static final Logger log = LoggerFactory.getLogger(NatsFanoutClient.class);

    private final JetStream jetStream;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final Counter delivered;
    private final Counter deliverFailed;

    public NatsFanoutClient(
            Connection connection,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry registry) {
        this.config = config;
        this.objectMapper = objectMapper;
        try {
            this.jetStream = connection.jetStream();
            ensureJetStreamStream(connection);
        } catch (IOException | JetStreamApiException e) {
            throw new IllegalStateException("NATS JetStream initialisation failed", e);
        }
        this.delivered = Counter.builder("oms_fanout_envelope_delivered_total")
                .tag("transport", "nats")
                .description("Domain fanout envelopes published to NATS JetStream")
                .register(registry);
        this.deliverFailed = Counter.builder("oms_fanout_envelope_deliver_failed_total")
                .tag("transport", "nats")
                .description("Failed NATS JetStream envelope deliveries")
                .register(registry);
    }

    private void ensureJetStreamStream(Connection connection) throws IOException, JetStreamApiException {
        var natsCfg = config.getEvents().getNats();
        String streamName = natsCfg.getStreamName();
        JetStreamManagement jsm = connection.jetStreamManagement();
        String subjectWildcard = streamSubjectWildcard(natsCfg.getSubjectPrefix());
        StreamConfiguration sc = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subjectWildcard)
                .storageType(StorageType.File)
                .build();
        try {
            jsm.addStream(sc);
        } catch (JetStreamApiException e) {
            if (!looksLikeStreamNameAlreadyInUse(e)) {
                throw e;
            }
            log.debug("JetStream stream {} already exists", streamName);
        }
    }

    private static final int NATS_JS_ERR_STREAM_NAME_ALREADY_IN_USE = 10058;

    private static boolean looksLikeStreamNameAlreadyInUse(JetStreamApiException e) {
        String msg = e.getMessage() == null ? "" : e.getMessage();
        return msg.contains(String.valueOf(NATS_JS_ERR_STREAM_NAME_ALREADY_IN_USE))
                || msg.toLowerCase().contains("stream name already in use");
    }

    private static String streamSubjectWildcard(String subjectPrefixRaw) {
        String p = subjectPrefixRaw == null ? "" : subjectPrefixRaw.trim();
        while (p.endsWith(".")) {
            p = p.substring(0, p.length() - 1);
        }
        if (p.isEmpty()) {
            throw new IllegalArgumentException("oms.events.nats.subject-prefix must not be empty");
        }
        return p + ".>";
    }

    @Override
    public boolean deliver(String envelopeJson) {
        try {
            JsonNode root = objectMapper.readTree(envelopeJson);
            String type = root.path("type").asText("unknown");
            String subject = publishSubject(config.getEvents().getNats().getSubjectPrefix(), type);
            jetStream.publish(subject, envelopeJson.getBytes(StandardCharsets.UTF_8));
            delivered.increment();
            return true;
        } catch (Exception e) {
            deliverFailed.increment();
            log.error("NATS JetStream fanout deliver failed", e);
            return false;
        }
    }

    private static String publishSubject(String prefixRaw, String eventType) {
        String p = prefixRaw == null ? "" : prefixRaw.trim();
        while (p.endsWith(".")) {
            p = p.substring(0, p.length() - 1);
        }
        if (p.isEmpty()) {
            throw new IllegalArgumentException("oms.events.nats.subject-prefix must not be empty");
        }
        return p + "." + eventType;
    }
}
