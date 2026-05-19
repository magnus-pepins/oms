package com.balh.oms.fx;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/**
 * Best-effort MQTT publisher for fx_hedge_actions state changes.
 *
 * <p>Wire contract: each hedge state change ({@code pending} → {@code posted}
 * / {@code failed}) emits the full audit-row JSON (same shape as the
 * {@code /internal/v1/fx/hedge/recent} listing) on {@code fx/hedge/event}.
 * Consumers (trading-desk Treasury page) merge by {@code id} so an update
 * replaces the prior row in their local list. The page does a one-shot
 * {@code /recent} fetch on mount to backfill history; the stream covers
 * everything after that.
 *
 * <p>Off by default. Enable per environment with
 * {@code OMS_FX_HEDGE_PUBLISHER_ENABLED=true}. Reuses the same broker URL
 * convention as {@link OmsFxMidSubscriber} (a separate client id so EMQX ACLs
 * can be tightened independently). QoS 1, non-retained — the page always
 * loads {@code /recent} on mount so retaining the "last hedge" forever would
 * just produce duplicates.
 *
 * <p>Failures here never block the hedge: {@link FxHedgeService} calls
 * {@link #publish} after the audit row + Ledger txn are already durable,
 * so a broker outage just means the desk's live stream gaps until the next
 * publish lands. The trading-desk keeps a slow safety-net poll for that
 * exact case.
 */
@Component
@ConditionalOnProperty(name = "oms.fx.hedge-publisher.enabled", havingValue = "true")
public class OmsFxHedgePublisher implements MqttCallback {

    private static final Logger log = LoggerFactory.getLogger(OmsFxHedgePublisher.class);
    private static final int PUBLISH_QOS = 1;

    private final ObjectMapper mapper;
    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String clientIdPrefix;
    private final String topic;
    private final Counter publishOkCounter;
    private final Counter publishFailCounter;

    private volatile MqttAsyncClient client;

    public OmsFxHedgePublisher(
            ObjectMapper mapper,
            MeterRegistry registry,
            @Value("${oms.fx.hedge-publisher.broker-url}") String brokerUrl,
            @Value("${oms.fx.hedge-publisher.username:}") String username,
            @Value("${oms.fx.hedge-publisher.password:}") String password,
            @Value("${oms.fx.hedge-publisher.client-id-prefix:oms-fx-hedge}") String clientIdPrefix,
            @Value("${oms.fx.hedge-publisher.topic:fx/hedge/event}") String topic) {
        this.mapper = mapper;
        this.brokerUrl = brokerUrl;
        this.username = username == null ? "" : username;
        this.password = password == null ? "" : password;
        this.clientIdPrefix = clientIdPrefix == null || clientIdPrefix.isBlank() ? "oms-fx-hedge" : clientIdPrefix;
        this.topic = topic == null || topic.isBlank() ? "fx/hedge/event" : topic;
        this.publishOkCounter = Counter.builder("oms_fx_hedge_publish_total")
                .tag("outcome", "ok")
                .description("fx_hedge_actions row publishes to EMQX")
                .register(registry);
        this.publishFailCounter = Counter.builder("oms_fx_hedge_publish_total")
                .tag("outcome", "fail")
                .description("fx_hedge_actions row publishes to EMQX")
                .register(registry);
    }

    @PostConstruct
    public void start() {
        try {
            String clientId = clientIdPrefix + "-" + UUID.randomUUID().toString().substring(0, 8);
            client = new MqttAsyncClient(brokerUrl, clientId, new MemoryPersistence());
            client.setCallback(this);
            MqttConnectionOptions opts = new MqttConnectionOptions();
            opts.setAutomaticReconnect(true);
            opts.setCleanStart(true);
            opts.setKeepAliveInterval(30);
            opts.setConnectionTimeout(15);
            if (!username.isBlank()) opts.setUserName(username);
            if (!password.isBlank()) opts.setPassword(password.getBytes(StandardCharsets.UTF_8));
            log.info("[oms-fx-hedge-pub] Connecting to broker={} clientId={}", brokerUrl, clientId);
            client.connect(opts).waitForCompletion(15_000);
        } catch (Exception e) {
            log.warn("[oms-fx-hedge-pub] start failed; publishes will be best-effort no-ops until reconnect", e);
        }
    }

    @PreDestroy
    public void stop() {
        try {
            if (client != null && client.isConnected()) {
                client.disconnect().waitForCompletion(2_000);
            }
        } catch (Exception e) {
            log.debug("[oms-fx-hedge-pub] disconnect noise on shutdown", e);
        }
    }

    /**
     * Best-effort publish of a single hedge row. Never throws; never blocks
     * the caller. Returns false on any failure so a future caller can
     * surface the gap if needed (no caller does today).
     */
    public boolean publish(Map<String, Object> row) {
        MqttAsyncClient c = client;
        if (c == null || !c.isConnected()) {
            publishFailCounter.increment();
            return false;
        }
        try {
            byte[] payload = mapper.writeValueAsBytes(row);
            MqttMessage msg = new MqttMessage(payload);
            msg.setQos(PUBLISH_QOS);
            msg.setRetained(false);
            c.publish(topic, msg);
            publishOkCounter.increment();
            return true;
        } catch (Exception e) {
            publishFailCounter.increment();
            log.warn("[oms-fx-hedge-pub] publish failed (topic={}): {}", topic, e.getMessage());
            return false;
        }
    }

    @Override
    public void disconnected(MqttDisconnectResponse response) {
        log.warn("[oms-fx-hedge-pub] disconnected reason={} server={}",
                response == null ? "?" : response.getReturnCode(),
                response == null ? "?" : response.getReasonString());
    }

    @Override
    public void mqttErrorOccurred(MqttException exception) {
        log.warn("[oms-fx-hedge-pub] mqtt error: {}", exception.getMessage());
    }

    @Override public void messageArrived(String topic, MqttMessage message) { /* publisher-only */ }
    @Override public void deliveryComplete(IMqttToken token) { /* fire-and-forget */ }
    @Override public void connectComplete(boolean reconnect, String serverURI) {
        log.info("[oms-fx-hedge-pub] connect complete reconnect={} server={}", reconnect, serverURI);
    }
    @Override public void authPacketArrived(int reasonCode, MqttProperties properties) { /* not used */ }
}
