package com.balh.oms.events;

import com.balh.oms.config.OmsConfig;
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

/**
 * JetStream-backed {@link DomainEventPublisher}. Invoked only after Postgres commit.
 */
public final class NatsDomainEventPublisher implements DomainEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(NatsDomainEventPublisher.class);

    private final JetStream jetStream;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final Counter published;
    private final Counter publishFailed;

    public NatsDomainEventPublisher(
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
        this.published = Counter.builder("oms_events_published_total")
                .tag("transport", "nats")
                .description("Domain events published to NATS JetStream")
                .register(registry);
        this.publishFailed = Counter.builder("oms_events_publish_failed_total")
                .tag("transport", "nats")
                .description("Failed NATS JetStream publish attempts")
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

    /**
     * jnats {@link JetStreamApiException} does not expose a stable typed error code across versions;
     * NATS uses {@code 10058} / "stream name already in use" when {@code addStream} collides.
     */
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
    public void publish(DomainEvent event) {
        try {
            String subject = publishSubject(config.getEvents().getNats().getSubjectPrefix(), event.type());
            byte[] payload = objectMapper.writeValueAsBytes(event);
            jetStream.publish(subject, payload);
            published.increment();
        } catch (Exception e) {
            publishFailed.increment();
            log.error("NATS JetStream publish failed for type={}", event.type(), e);
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
