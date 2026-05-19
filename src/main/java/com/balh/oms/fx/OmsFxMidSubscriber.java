package com.balh.oms.fx;

import com.fasterxml.jackson.databind.JsonNode;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Phase 1.5 FX mid subscriber — bridges live BBO quotes from
 * marketdata-platform's EMQX topic {@code fx/{B}/{Q}/quote} into
 * {@link FxQuoteService}'s mid source.
 *
 * <p>Wire contract (post 2026-05): payload is {@code { base_currency, quote_currency,
 * bid, ask, timestamp, published_at }}. Mid is derived as {@code (bid+ask)/2}.
 *
 * <p>Behaviour:
 * <ul>
 *   <li><b>Off by default</b>. Enable per environment with {@code OMS_FX_MID_SUBSCRIBER_ENABLED=true}.</li>
 *   <li>On connect, subscribes to wildcard {@code fx/+/+/quote}. EMQX retained
 *       messages are delivered immediately so we have a quote per pair before
 *       the first vendor tick.</li>
 *   <li>Maintains a {@code (pair -> {mid, capturedAt})} table. Reads are O(1) via
 *       {@link #midFor(String)}; staleness beyond {@code OMS_FX_MID_STALENESS_MS}
 *       returns null so {@link FxQuoteService} can fall back to its STUB_MIDS.</li>
 *   <li>Auto-reconnects (Paho's built-in {@code setAutomaticReconnect}) and logs
 *       at WARN on every connectionLost/reconnect, INFO on subscribe ack.</li>
 * </ul>
 *
 * <p>What this <em>doesn't</em> do (out of scope for Phase 1.5):
 * <ul>
 *   <li>Persist the mid to disk for cold-start. {@link FxQuoteService#STUB_MIDS}
 *       remains the cold-start fallback. Long-term, the cold-start path is
 *       the {@code MassiveRestClient::getLastFxQuote} REST helper retained
 *       in marketdata-platform for exactly this purpose.</li>
 *   <li>Per-tier topic ({@code fx/{B}/{Q}/customer/{tier}/quote}) — that's
 *       Phase 2 once OMS owns customer pricing.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "oms.fx.mid-subscriber.enabled", havingValue = "true")
public class OmsFxMidSubscriber implements MqttCallback {

    private static final Logger log = LoggerFactory.getLogger(OmsFxMidSubscriber.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Topic shape: fx/{BASE}/{QUOTE}/quote. Wildcard covers every pair the
    // publisher emits; we never need to enumerate the configured pair list
    // here because EMQX retained delivery + per-tick updates do the work.
    private static final String SUBSCRIBE_TOPIC = "fx/+/+/quote";
    private static final int SUBSCRIBE_QOS = 0;

    /**
     * Default staleness threshold beyond which {@link #midFor(String)} returns
     * null. 60s is intentionally generous: even on a quiet pair the publisher
     * republishes at least every few seconds when connected; if we've heard
     * nothing for a minute the upstream is wedged and the caller should fall
     * back. Override with {@code OMS_FX_MID_STALENESS_MS}.
     */
    private static final long DEFAULT_STALENESS_MS = 60_000L;

    private final Clock clock;
    private final Counter ticksCounter;
    private final Counter parseErrorsCounter;
    private final Map<String, MidSample> mids = new ConcurrentHashMap<>();

    private final String brokerUrl;
    private final String username;
    private final String password;
    private final long stalenessMillis;
    private final String clientIdPrefix;

    private MqttAsyncClient client;

    public OmsFxMidSubscriber(
            Clock clock,
            MeterRegistry registry,
            @Value("${oms.fx.mid-subscriber.broker-url:${EMQX_MQTT_BROKER_URL:tcp://127.0.0.1:1883}}") String brokerUrl,
            @Value("${oms.fx.mid-subscriber.username:${EMQX_MQTT_USERNAME:oms-fx-subscriber}}") String username,
            @Value("${oms.fx.mid-subscriber.password:${EMQX_MQTT_PASSWORD:}}") String password,
            @Value("${oms.fx.mid-subscriber.staleness-ms:" + DEFAULT_STALENESS_MS + "}") long stalenessMillis,
            @Value("${oms.fx.mid-subscriber.client-id-prefix:oms-fx-mid}") String clientIdPrefix) {
        this.clock = clock;
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
        this.stalenessMillis = Math.max(1_000L, stalenessMillis);
        this.clientIdPrefix = clientIdPrefix;
        this.ticksCounter = Counter.builder("oms.fx.mid_subscriber.ticks_total")
                .description("FX BBO ticks consumed from marketdata-platform")
                .register(registry);
        this.parseErrorsCounter = Counter.builder("oms.fx.mid_subscriber.parse_errors_total")
                .description("FX BBO ticks that failed to parse as valid bid/ask")
                .register(registry);
    }

    @PostConstruct
    public void start() {
        String clientId = clientIdPrefix + "-" + UUID.randomUUID().toString().substring(0, 8);
        try {
            client = new MqttAsyncClient(brokerUrl, clientId, new MemoryPersistence());
            MqttConnectionOptions opts = new MqttConnectionOptions();
            opts.setAutomaticReconnect(true);
            opts.setCleanStart(true);
            opts.setConnectionTimeout(10);
            opts.setKeepAliveInterval(60);
            if (username != null && !username.isBlank()) opts.setUserName(username);
            if (password != null && !password.isBlank()) opts.setPassword(password.getBytes());
            client.setCallback(this);

            log.info("[oms-fx-mid] Connecting to MQTT broker={} clientId={}", brokerUrl, clientId);
            client.connect(opts).waitForCompletion(15_000);
            subscribe();
        } catch (MqttException e) {
            // Don't fail the application startup if the broker is briefly
            // unavailable — Paho will keep retrying. Log loudly so the failure
            // surfaces in dashboards.
            log.warn("[oms-fx-mid] MQTT connect failed (broker={}); will rely on auto-reconnect", brokerUrl, e);
        }
    }

    @PreDestroy
    public void stop() {
        if (client == null) return;
        try {
            client.disconnectForcibly(2_000);
            client.close();
        } catch (MqttException e) {
            log.warn("[oms-fx-mid] MQTT disconnect failed", e);
        }
    }

    private void subscribe() throws MqttException {
        client.subscribe(SUBSCRIBE_TOPIC, SUBSCRIBE_QOS);
        log.info("[oms-fx-mid] Subscribed topic={} qos={}", SUBSCRIBE_TOPIC, SUBSCRIBE_QOS);
    }

    /**
     * Returns the most recent mid for the given pair (e.g. "EURUSD") if it
     * was captured within the staleness window. Otherwise returns null and
     * the caller should fall back (typically to {@link FxQuoteService#STUB_MIDS}).
     */
    public BigDecimal midFor(String pair) {
        if (pair == null || pair.isBlank()) return null;
        MidSample sample = mids.get(pair.toUpperCase());
        if (sample == null) return null;
        Instant now = clock.instant();
        if (now.toEpochMilli() - sample.capturedAtMs() > stalenessMillis) return null;
        return sample.mid();
    }

    /** Diagnostic snapshot for {@code GET /internal/v1/fx/mids/live} and metrics. */
    public Map<String, MidSample> snapshot() {
        return Map.copyOf(mids);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        String[] parts = topic.split("/");
        if (parts.length < 4) return;
        String base = parts[1].toUpperCase();
        String quote = parts[2].toUpperCase();
        String pair = base + quote;
        try {
            JsonNode node = MAPPER.readTree(message.getPayload());
            JsonNode bidNode = node.get("bid");
            JsonNode askNode = node.get("ask");
            JsonNode rateNode = node.get("rate");

            BigDecimal mid = null;
            if (bidNode != null && askNode != null && bidNode.isNumber() && askNode.isNumber()) {
                BigDecimal bid = bidNode.decimalValue();
                BigDecimal ask = askNode.decimalValue();
                if (bid.signum() > 0 && ask.signum() > 0) {
                    mid = bid.add(ask).divide(BigDecimal.valueOf(2), 10, RoundingMode.HALF_UP);
                }
            } else if (rateNode != null && rateNode.isNumber() && rateNode.decimalValue().signum() > 0) {
                mid = rateNode.decimalValue();
            }
            if (mid == null) {
                parseErrorsCounter.increment();
                return;
            }
            mids.put(pair, new MidSample(mid, clock.millis()));
            ticksCounter.increment();
        } catch (Exception e) {
            parseErrorsCounter.increment();
            log.debug("[oms-fx-mid] Failed to parse tick on {} (payload bytes={})", topic,
                    message.getPayload() == null ? 0 : message.getPayload().length, e);
        }
    }

    @Override
    public void disconnected(MqttDisconnectResponse response) {
        log.warn("[oms-fx-mid] Disconnected returnCode={} reason={}",
                response.getReturnCode(), response.getReasonString());
    }

    @Override
    public void mqttErrorOccurred(MqttException e) {
        log.warn("[oms-fx-mid] MQTT error", e);
    }

    @Override
    public void deliveryComplete(IMqttToken token) {
        // No-op: subscriber doesn't publish.
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        log.info("[oms-fx-mid] Connect complete reconnect={} server={}", reconnect, serverURI);
        try {
            subscribe();
        } catch (MqttException e) {
            log.warn("[oms-fx-mid] (Re)subscribe failed after connect; broker may drop us on next keepalive", e);
        }
    }

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {
        // No-op: subscriber doesn't drive MQTT v5 AUTH packets.
    }

    /**
     * Snapshot of the latest mid for a pair plus the wall-clock time we
     * captured it. Used by {@link #midFor(String)} for staleness checks and
     * by {@link #snapshot()} for diagnostic endpoints.
     */
    public record MidSample(BigDecimal mid, long capturedAtMs) {
        public Instant capturedAt() { return Instant.ofEpochMilli(capturedAtMs); }
        public Duration ageFrom(Instant now) { return Duration.between(capturedAt(), now); }
    }

    @SuppressWarnings("unused") // Kept for an at-glance debug line in operator runbooks.
    public String summary() {
        return Arrays.toString(mids.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue().mid())
                .toArray());
    }
}
