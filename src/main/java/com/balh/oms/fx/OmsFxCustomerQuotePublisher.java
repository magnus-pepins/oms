package com.balh.oms.fx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Tier-aware FX quote publisher (§11.5.6 Phase 2 of
 * plans/oms-fix-gateway-and-settlement.md, §8 of
 * plans/oms-multi-currency-invest-accounts.md).
 *
 * <p>Pull-model design — every {@link #publishTickPeriodMs} milliseconds we
 * snapshot {@link OmsFxMidSubscriber} for the latest vendor mid per pair,
 * apply the per-tier markup grid loaded from {@code fx_pair_markups}, and
 * publish a tier-specific quote message on
 * {@code fx/{BASE}/{QUOTE}/customer/{tier}/quote}.
 *
 * <p>Why pull and not callback off the vendor MQTT stream:
 * <ul>
 *   <li>Vendor can tick faster than the customer UI needs (5–10/s during
 *       active hours). A pull at 1 s gives a natural throttle without an
 *       extra per-(pair,tier) timestamp map.
 *   <li>Avoids a second MQTT subscription to the same {@code fx/+/+/quote}
 *       topic from this process. The mid-subscriber already pays the
 *       SUB cost; we just read its snapshot.
 *   <li>If the vendor feed goes dark, the snapshot rows go stale and we
 *       stop publishing — no per-tick "is it stale?" decision in a hot
 *       callback path.
 * </ul>
 *
 * <p>The stream is <strong>display-only</strong>. {@code quoteId} is NOT
 * minted here — it is minted at submit-time via
 * {@link FxQuoteService#quote(String, String)} so the rate the customer
 * locks is the one OMS has in its accept-path cache. The MQTT message
 * carries {@code valid_until} so the UI can dim the ticket if the feed
 * has clearly stopped without an explicit gap message from the publisher.
 *
 * <p>Off by default. Enable per environment with
 * {@code OMS_FX_CUSTOMER_QUOTE_PUBLISHER_ENABLED=true} and ensure
 * {@code OMS_FX_MID_SUBSCRIBER_ENABLED=true} is also on so {@code snapshot()}
 * actually returns mids.
 */
@Component
@ConditionalOnProperty(name = "oms.fx.customer-quote-publisher.enabled", havingValue = "true")
public class OmsFxCustomerQuotePublisher {

    private static final Logger log = LoggerFactory.getLogger(OmsFxCustomerQuotePublisher.class);

    /** Display scale for bid/ask/mid in the MQTT payload (matches FxQuoteService). */
    private static final int RATE_SCALE = 8;
    /**
     * Basis-point denominator. {@code applyMarkup} below converts
     * {@code markup_bps} to a fractional factor via {@code bps / BPS_DIVISOR}.
     */
    private static final BigDecimal BPS_DIVISOR = new BigDecimal("10000");
    /** Scale used for the intermediate {@code factor} BigDecimal in applyMarkup. */
    private static final int MARKUP_FACTOR_SCALE = 10;

    private final OmsFxMidSubscriber midSubscriber;
    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final ObjectMapper mapper;
    private final FxMarkupOverridesService overrides;
    private final List<String> tiers;
    private final long publishTickPeriodMs;
    private final long maxMidAgeMs;
    private final long markupRefreshMs;
    private final String topicPattern;
    private final boolean retained;
    private final int qos;
    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String clientIdPrefix;

    private final Counter publishOkCounter;
    private final Counter publishFailCounter;
    private final Counter staleMidCounter;
    private final Counter missingMarkupCounter;

    /** (pair, tier, side) → markup_bps. Refreshed in {@link #refreshMarkups()}. */
    private volatile Map<MarkupKey, BigDecimal> markupCache = Collections.emptyMap();
    /** Wall-clock millis at which {@link #markupCache} was last loaded. */
    private volatile long markupCacheLoadedAtMs;
    /** Set of pairs we've already logged a "no markup, skipping" warning for — keeps logs quiet. */
    private final Set<String> warnedMissingPairs = ConcurrentHashMap.newKeySet();

    private volatile MqttAsyncClient client;

    public OmsFxCustomerQuotePublisher(
            OmsFxMidSubscriber midSubscriber,
            JdbcTemplate jdbc,
            Clock clock,
            ObjectMapper mapper,
            FxMarkupOverridesService overrides,
            MeterRegistry registry,
            @Value("${oms.fx.customer-quote-publisher.broker-url}") String brokerUrl,
            @Value("${oms.fx.customer-quote-publisher.username:}") String username,
            @Value("${oms.fx.customer-quote-publisher.password:}") String password,
            @Value("${oms.fx.customer-quote-publisher.client-id-prefix:oms-fx-customer-quote}") String clientIdPrefix,
            @Value("${oms.fx.customer-quote-publisher.tiers:basic,elite}") String tiersCsv,
            @Value("${oms.fx.customer-quote-publisher.tick-period-ms:1000}") long publishTickPeriodMs,
            @Value("${oms.fx.customer-quote-publisher.max-mid-age-ms:30000}") long maxMidAgeMs,
            @Value("${oms.fx.customer-quote-publisher.markup-refresh-ms:60000}") long markupRefreshMs,
            @Value("${oms.fx.customer-quote-publisher.topic-pattern:fx/{base}/{quote}/customer/{tier}/quote}") String topicPattern,
            @Value("${oms.fx.customer-quote-publisher.retained:true}") boolean retained,
            @Value("${oms.fx.customer-quote-publisher.qos:0}") int qos) {
        this.midSubscriber = midSubscriber;
        this.jdbc = jdbc;
        this.clock = clock;
        this.mapper = mapper;
        this.overrides = overrides;
        this.brokerUrl = brokerUrl;
        this.username = username == null ? "" : username;
        this.password = password == null ? "" : password;
        this.clientIdPrefix = clientIdPrefix == null || clientIdPrefix.isBlank()
                ? "oms-fx-customer-quote" : clientIdPrefix;
        this.tiers = parseTiers(tiersCsv);
        this.publishTickPeriodMs = Math.max(100L, publishTickPeriodMs);
        this.maxMidAgeMs = Math.max(1_000L, maxMidAgeMs);
        this.markupRefreshMs = Math.max(5_000L, markupRefreshMs);
        this.topicPattern = topicPattern == null || topicPattern.isBlank()
                ? "fx/{base}/{quote}/customer/{tier}/quote" : topicPattern;
        this.retained = retained;
        this.qos = qos < 0 || qos > 2 ? 0 : qos;

        this.publishOkCounter = Counter.builder("oms_fx_customer_quote_publish_total")
                .tag("outcome", "ok")
                .description("Customer-tier FX quote MQTT publishes")
                .register(registry);
        this.publishFailCounter = Counter.builder("oms_fx_customer_quote_publish_total")
                .tag("outcome", "fail")
                .description("Customer-tier FX quote MQTT publishes")
                .register(registry);
        this.staleMidCounter = Counter.builder("oms_fx_customer_quote_stale_mid_total")
                .description("Skipped customer quote publish because vendor mid was older than max-mid-age-ms")
                .register(registry);
        this.missingMarkupCounter = Counter.builder("oms_fx_customer_quote_missing_markup_total")
                .description("Skipped customer quote publish because no markup row matched (pair, side, tier)")
                .register(registry);
    }

    private static List<String> parseTiers(String csv) {
        if (csv == null || csv.isBlank()) return List.of("basic", "elite");
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .map(String::toLowerCase)
                .filter(s -> !s.isEmpty())
                .distinct()
                .collect(Collectors.toUnmodifiableList());
    }

    @PostConstruct
    public void start() {
        log.info("[oms-fx-cust-pub] starting tiers={} tickPeriodMs={} maxMidAgeMs={} topicPattern={} retained={} qos={}",
                tiers, publishTickPeriodMs, maxMidAgeMs, topicPattern, retained, qos);
        refreshMarkups();
        try {
            String clientId = clientIdPrefix + "-" + UUID.randomUUID().toString().substring(0, 8);
            client = new MqttAsyncClient(brokerUrl, clientId, new MemoryPersistence());
            MqttConnectionOptions opts = new MqttConnectionOptions();
            opts.setAutomaticReconnect(true);
            opts.setCleanStart(true);
            opts.setKeepAliveInterval(30);
            opts.setConnectionTimeout(15);
            if (!username.isBlank()) opts.setUserName(username);
            if (!password.isBlank()) opts.setPassword(password.getBytes(StandardCharsets.UTF_8));
            log.info("[oms-fx-cust-pub] Connecting broker={} clientId={}", brokerUrl, clientId);
            client.connect(opts).waitForCompletion(15_000);
        } catch (MqttException e) {
            log.warn("[oms-fx-cust-pub] MQTT connect failed; relying on auto-reconnect", e);
        }
    }

    @PreDestroy
    public void stop() {
        try {
            if (client != null && client.isConnected()) {
                client.disconnect().waitForCompletion(2_000);
            }
        } catch (Exception e) {
            log.debug("[oms-fx-cust-pub] disconnect noise on shutdown", e);
        }
    }

    /**
     * Periodic refresh of {@link #markupCache}. Runs on its own
     * {@code @Scheduled} cadence rather than re-querying per publish tick so
     * a publish at 1 Hz across (say) 6 pairs × 5 tiers does not generate
     * 30 DB hits/second on what is essentially a static table.
     *
     * <p>Failure is logged and ignored — the previous cache stays in place.
     * This is deliberate: a transient DB blip should not silently switch
     * us into "no quotes published" mode. The refresh metric is the
     * operator signal.
     */
    @Scheduled(fixedDelayString = "${oms.fx.customer-quote-publisher.markup-refresh-ms:60000}",
            initialDelayString = "${oms.fx.customer-quote-publisher.markup-refresh-ms:60000}")
    public void refreshMarkups() {
        try {
            String sql = "SELECT pair, side, tier, markup_bps FROM fx_pair_markups "
                    + "WHERE is_active = TRUE AND tier = ANY(?)";
            String[] tierArr = markupLoadTiers().toArray(new String[0]);
            Map<MarkupKey, BigDecimal> next = new HashMap<>();
            jdbc.query(sql, ps -> ps.setArray(1, ps.getConnection().createArrayOf("text", tierArr)), rs -> {
                String pair = rs.getString("pair");
                String side = rs.getString("side");
                String tier = rs.getString("tier");
                BigDecimal bps = rs.getBigDecimal("markup_bps");
                if (pair == null || side == null || tier == null || bps == null) return;
                next.put(new MarkupKey(pair.toUpperCase(), tier.toLowerCase(), side.toUpperCase()), bps);
            });
            markupCache = Collections.unmodifiableMap(next);
            markupCacheLoadedAtMs = clock.millis();
            warnedMissingPairs.clear();
            log.debug("[oms-fx-cust-pub] markup cache refreshed entries={} tiers={}", next.size(), tiers);
        } catch (DataAccessException e) {
            log.warn("[oms-fx-cust-pub] markup refresh failed; reusing prior cache size={}", markupCache.size(), e);
        }
    }

    /**
     * Publish loop. Snapshots the live mid map, applies per-tier markups,
     * publishes one MQTT message per (pair, tier) where:
     *
     *   * a mid is present AND fresher than {@link #maxMidAgeMs}, AND
     *   * a markup row exists for (pair, BID, tier) AND (pair, ASK, tier)
     *     in {@link #markupCache} (or {@code default} as fallback handled
     *     via {@code fx_pair_markups} waterfall — but we do that lookup
     *     here too so the cache stays in lockstep with FxQuoteService).
     *
     * <p>Per-tier publish is best-effort: if one tier message fails to
     * encode or send, the loop continues to the next so a single bad
     * row does not stop the stream for everyone.
     */
    @Scheduled(fixedDelayString = "${oms.fx.customer-quote-publisher.tick-period-ms:1000}",
            initialDelayString = "${oms.fx.customer-quote-publisher.tick-period-ms:1000}")
    public void publishTick() {
        MqttAsyncClient c = client;
        if (c == null || !c.isConnected()) return;
        if (markupCache.isEmpty()) return;
        Map<String, OmsFxMidSubscriber.MidSample> snapshot = midSubscriber.snapshot();
        if (snapshot.isEmpty()) return;
        long nowMs = clock.millis();
        Instant nowInstant = Instant.ofEpochMilli(nowMs);
        for (Map.Entry<String, OmsFxMidSubscriber.MidSample> e : snapshot.entrySet()) {
            String pair = e.getKey();
            OmsFxMidSubscriber.MidSample sample = e.getValue();
            if (sample == null) continue;
            long age = nowMs - sample.capturedAtMs();
            if (age > maxMidAgeMs) {
                staleMidCounter.increment();
                continue;
            }
            for (String tier : tiers) {
                publishOne(c, pair, tier, sample, nowInstant);
            }
        }
    }

    private void publishOne(MqttAsyncClient c, String pair, String tier,
                            OmsFxMidSubscriber.MidSample sample, Instant now) {
        BigDecimal bidBps = lookupMarkup(pair, "BID", tier);
        BigDecimal askBps = lookupMarkup(pair, "ASK", tier);
        if (bidBps == null || askBps == null) {
            missingMarkupCounter.increment();
            if (warnedMissingPairs.add(pair + ":" + tier)) {
                log.warn("[oms-fx-cust-pub] missing markup pair={} tier={} (BID={} ASK={}); skipping publish",
                        pair, tier, bidBps, askBps);
            }
            return;
        }
        BigDecimal mid = sample.mid();
        BigDecimal bid = applyMarkup(mid, bidBps, -1);
        BigDecimal ask = applyMarkup(mid, askBps, +1);
        if (pair.length() < 6) {
            return;
        }
        String base = pair.substring(0, 3);
        String quote = pair.substring(3, 6);
        String topic = topicPattern
                .replace("{base}", base)
                .replace("{quote}", quote)
                .replace("{tier}", tier);
        Instant validUntil = Instant.ofEpochMilli(sample.capturedAtMs() + maxMidAgeMs);
        try {
            ObjectNode body = mapper.createObjectNode();
            body.put("base_currency", base);
            body.put("quote_currency", quote);
            body.put("tier", tier);
            body.put("bid", bid.setScale(RATE_SCALE, RoundingMode.HALF_UP).toPlainString());
            body.put("ask", ask.setScale(RATE_SCALE, RoundingMode.HALF_UP).toPlainString());
            body.put("mid", mid.setScale(RATE_SCALE, RoundingMode.HALF_UP).toPlainString());
            body.put("bidMarkupBps", bidBps.toPlainString());
            body.put("askMarkupBps", askBps.toPlainString());
            body.put("sourceFeed", "vendor-mid-live");
            body.put("captured_at", sample.capturedAt().toString());
            body.put("valid_until", validUntil.toString());
            body.put("published_at", now.toString());
            byte[] payload = mapper.writeValueAsBytes(body);
            MqttMessage msg = new MqttMessage(payload);
            msg.setQos(qos);
            msg.setRetained(retained);
            c.publish(topic, msg);
            publishOkCounter.increment();
        } catch (Exception ex) {
            publishFailCounter.increment();
            log.debug("[oms-fx-cust-pub] publish failed pair={} tier={} topic={}: {}",
                    pair, tier, topic, ex.getMessage());
        }
    }

    /**
     * Looks up the effective markup for (pair, side, tier) — the rate-card
     * value from {@link #markupCache} (with a {@code default} fallback)
     * plus any active tactical overrides from
     * {@link FxMarkupOverridesService}. Mirrors
     * {@link FxQuoteService#lookupMarkupBps(String, String, String)} so
     * the streaming quote and the HTTP /quote endpoint agree exactly on
     * the rate the customer sees vs. the rate locked at submit.
     *
     * <p>Override matching uses the <em>requested</em> tier, not the
     * tier the base markup ultimately resolved to. A widening row on
     * tier=elite applies to an elite request even when its base falls
     * back to the tier=default row, because the operator's intent
     * ("widen elite") is tier-scoped not row-scoped.
     *
     * <p>Returns {@code null} only when there is no base row at all —
     * an override on a pair with no rate-card cannot conjure a quote
     * out of thin air. Effective bps clamps to zero when an additive
     * override would push the value negative, matching the
     * {@code fx_pair_markups.markup_bps >= 0} invariant.
     */
    BigDecimal lookupMarkup(String pair, String side, String tier) {
        BigDecimal base = lookupBaseMarkup(pair, side, tier);
        if (base == null) return null;
        BigDecimal additive = overrides.additiveBpsFor(pair, side, tier, clock.instant());
        if (additive.signum() == 0) return base;
        BigDecimal effective = base.add(additive);
        return effective.signum() < 0 ? BigDecimal.ZERO : effective;
    }

    private BigDecimal lookupBaseMarkup(String pair, String side, String tier) {
        MarkupKey k = new MarkupKey(pair, tier, side);
        BigDecimal v = markupCache.get(k);
        if (v != null) return v;
        if (!"default".equals(tier)) {
            return markupCache.get(new MarkupKey(pair, "default", side));
        }
        return null;
    }

    /**
     * Tiers loaded into {@link #markupCache} on refresh.
     *
     * <p>Always includes {@code default} on top of the configured publish
     * tiers so {@link #lookupMarkup} can waterfall to the {@code default}
     * row when an exotic pair (e.g. {@code USDMXN}, {@code EURAUD}) only
     * has a {@code default} markup in {@code fx_pair_markups}. Without this,
     * the streaming publisher would silently skip every non-major pair —
     * even though {@link FxQuoteService#lookupMarkupBps} (HTTP /quote path)
     * resolves them correctly via its own DB-side waterfall.
     */
    List<String> markupLoadTiers() {
        List<String> result = new ArrayList<>(tiers);
        if (!result.contains("default")) {
            result.add("default");
        }
        return Collections.unmodifiableList(result);
    }

    /** Package-private for unit tests. */
    void primeMarkupCache(Map<MarkupKey, BigDecimal> cache) {
        this.markupCache = Collections.unmodifiableMap(new HashMap<>(cache));
        this.markupCacheLoadedAtMs = clock.millis();
    }

    static BigDecimal applyMarkup(BigDecimal mid, BigDecimal bps, int direction) {
        BigDecimal factor = BigDecimal.ONE.add(
                bps.divide(BPS_DIVISOR, MARKUP_FACTOR_SCALE, RoundingMode.HALF_UP)
                        .multiply(BigDecimal.valueOf(direction)));
        return mid.multiply(factor);
    }

    /** Diagnostic snapshot for {@code GET /internal/v1/fx/customer-quote/status}. */
    public PublisherStatus status() {
        return new PublisherStatus(
                tiers,
                markupCache.size(),
                markupCacheLoadedAtMs,
                client != null && client.isConnected(),
                publishTickPeriodMs,
                maxMidAgeMs,
                overrides.status());
    }

    /** Composite key for the (pair, tier, side) markup lookup. */
    record MarkupKey(String pair, String tier, String side) {
        MarkupKey {
            Objects.requireNonNull(pair);
            Objects.requireNonNull(tier);
            Objects.requireNonNull(side);
        }
    }

    /** Returned by {@link #status()} for ops/diagnostic surfaces. */
    public record PublisherStatus(
            List<String> tiers,
            int markupCacheSize,
            long markupCacheLoadedAtMs,
            boolean mqttConnected,
            long publishTickPeriodMs,
            long maxMidAgeMs,
            FxMarkupOverridesService.OverridesStatus overrides
    ) {}
}
