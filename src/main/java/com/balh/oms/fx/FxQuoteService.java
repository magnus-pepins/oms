package com.balh.oms.fx;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FX quote service — builds the customer-visible BID/ASK for a (pair, tier)
 * combination on top of the prime-broker mid and the tier-specific markup
 * grid in {@code fx_pair_markups} (Flyway V37).
 *
 * <p>Demo-scope choices:
 * <ul>
 *   <li>The mid source is a static seed map below (EURUSD = 1.0850 etc.). When
 *       the PB market-data subscriber lands, replace
 *       {@link #midWithSourceForPair(String)} with a feed read; everything else stays.
 *   <li>Each call mints a {@code quoteId} and caches the quote with its
 *       expiry so a follow-up hedge or customer-side accept can be
 *       reconciled. The validity window is configurable (default 30s) — see
 *       {@link #quote(String, String)}.
 *   <li>The cache is in-memory only; surviving a restart is out of scope for
 *       the demo. Production should persist or skip; never silently
 *       re-quote at acceptance.
 * </ul>
 */
@Service
public class FxQuoteService {

    private static final Logger log = LoggerFactory.getLogger(FxQuoteService.class);

    /** Default validity window for a fresh quote. Tunable via {@link #setValidityMillis(long)}. */
    private static final long DEFAULT_VALIDITY_MS = 30_000L;
    private static final int  RATE_SCALE = 8;

    /**
     * Static prime-broker mid map. Stub for the demo until the marketdata-platform
     * subscriber lands. Override per-pair via {@code OMS_FX_MID_<PAIR>} (not wired
     * yet — left as a follow-up so the demo numbers stay predictable).
     */
    private static final Map<String, BigDecimal> STUB_MIDS = Map.of(
            "EURUSD", new BigDecimal("1.0850"),
            "USDEUR", new BigDecimal("0.9217"),
            "GBPUSD", new BigDecimal("1.2700"),
            "USDGBP", new BigDecimal("0.7874"),
            "EURGBP", new BigDecimal("0.8543"),
            "GBPEUR", new BigDecimal("1.1706")
    );

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final Counter quotesCounter;
    private final Counter quoteMissesCounter;
    private final Counter midFromSubscriberCounter;
    private final Counter midFromStubCounter;
    private final Map<String, CachedQuote> cache = new ConcurrentHashMap<>();
    private volatile long validityMillis = DEFAULT_VALIDITY_MS;

    /**
     * Optional live mid source. Wired only when {@link OmsFxMidSubscriber} is on
     * the classpath as a Spring bean (i.e. {@code oms.fx.mid-subscriber.enabled=true}).
     * Reads fall back to {@link #STUB_MIDS} when null, empty, or stale.
     */
    @Autowired(required = false)
    private OmsFxMidSubscriber liveMidSubscriber;

    public FxQuoteService(JdbcTemplate jdbc, Clock clock, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.quotesCounter = Counter.builder("oms.fx.quote.issued_total")
                .description("FX quotes issued by FxQuoteService")
                .register(registry);
        this.quoteMissesCounter = Counter.builder("oms.fx.quote.missing_markup_total")
                .description("FX quote requests where no markup row matched (pair, side, tier)")
                .register(registry);
        this.midFromSubscriberCounter = Counter.builder("oms.fx.quote.mid_source_total")
                .tag("source", "subscriber")
                .description("FX quotes whose mid came from the live Phase 1.5 subscriber")
                .register(registry);
        this.midFromStubCounter = Counter.builder("oms.fx.quote.mid_source_total")
                .tag("source", "stub")
                .description("FX quotes whose mid came from STUB_MIDS (subscriber stale/absent)")
                .register(registry);
    }

    public void setValidityMillis(long ms) {
        this.validityMillis = Math.max(1_000L, ms);
    }

    public long getValidityMillis() {
        return validityMillis;
    }

    /**
     * Resolves a fresh quote for the requested (pair, tier).
     *
     * @return ordered map suitable for direct JSON serialisation. Keys: quoteId, pair, tier, mid, bid, ask,
     *         bidMarkupBps, askMarkupBps, source ("internal-mid-stub"), capturedAt, expiresAt.
     * @throws IllegalArgumentException if the pair has no stub mid (demo only).
     * @throws IllegalStateException if markup rows are missing for the requested tier
     */
    public Map<String, Object> quote(String pair, String tier) {
        String pairUp = pair == null ? "" : pair.toUpperCase();
        String tierLow = tier == null || tier.isBlank() ? "default" : tier.toLowerCase();
        MidWithSource ms = midWithSourceForPair(pairUp);
        BigDecimal mid = ms.mid();

        BigDecimal bidBps = lookupMarkupBps(pairUp, "BID", tierLow);
        BigDecimal askBps = lookupMarkupBps(pairUp, "ASK", tierLow);

        BigDecimal bid = applyMarkup(mid, bidBps, /*direction*/ -1);
        BigDecimal ask = applyMarkup(mid, askBps, /*direction*/ +1);

        Instant now = clock.instant();
        Instant exp = now.plusMillis(validityMillis);
        String quoteId = "q_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("quoteId", quoteId);
        body.put("pair", pairUp);
        body.put("tier", tierLow);
        body.put("mid", mid.setScale(RATE_SCALE, RoundingMode.HALF_UP).toPlainString());
        body.put("bid", bid.setScale(RATE_SCALE, RoundingMode.HALF_UP).toPlainString());
        body.put("ask", ask.setScale(RATE_SCALE, RoundingMode.HALF_UP).toPlainString());
        body.put("bidMarkupBps", bidBps.toPlainString());
        body.put("askMarkupBps", askBps.toPlainString());
        body.put("source", ms.source());
        body.put("capturedAt", now.toString());
        body.put("expiresAt", exp.toString());
        body.put("validityMs", validityMillis);

        cache.put(quoteId, new CachedQuote(quoteId, pairUp, tierLow, bid, ask, mid, now, exp));
        quotesCounter.increment();
        purgeExpired(now);
        return body;
    }

    /**
     * Recall a previously issued quote (for hedge submission), returning null when
     * not found or already expired.
     */
    public CachedQuote recall(String quoteId) {
        if (quoteId == null || quoteId.isBlank()) return null;
        CachedQuote q = cache.get(quoteId);
        if (q == null) return null;
        if (clock.instant().isAfter(q.expiresAt)) {
            cache.remove(quoteId);
            return null;
        }
        return q;
    }

    /**
     * Mid resolution with a two-stage waterfall:
     *
     *   1. The Phase 1.5 {@link OmsFxMidSubscriber} if it's wired in and has
     *      a fresh tick (within its staleness window). This is the live PB
     *      mid published by marketdata-platform — surfaced as
     *      {@code source = "vendor-mid-live"} in the quote payload so
     *      downstream surfaces (trading-desk treasury, beard-admin) can
     *      visibly distinguish a live quote from the stub fallback.
     *   2. The static {@link #STUB_MIDS} table — predictable demo numbers,
     *      and the cold-start fallback before the first tick arrives
     *      ({@code source = "internal-mid-stub"}).
     *
     * Either source still expects the pair to be configured upstream
     * (markups in {@code fx_pair_markups}, and a STUB_MIDS entry for the
     * cold-start case). An unknown pair throws — the controller maps that
     * to HTTP 400.
     */
    private MidWithSource midWithSourceForPair(String pair) {
        if (liveMidSubscriber != null) {
            BigDecimal live = liveMidSubscriber.midFor(pair);
            if (live != null) {
                midFromSubscriberCounter.increment();
                return new MidWithSource(live, "vendor-mid-live");
            }
        }
        BigDecimal stub = STUB_MIDS.get(pair);
        if (stub == null) {
            throw new IllegalArgumentException("no mid configured for pair " + pair);
        }
        midFromStubCounter.increment();
        return new MidWithSource(stub, "internal-mid-stub");
    }

    private record MidWithSource(BigDecimal mid, String source) {}

    /**
     * @return the resolved markup in bps for (pair, side, tier), falling back to tier='default' when
     *         the requested tier has no row. Throws if both queries miss.
     */
    private BigDecimal lookupMarkupBps(String pair, String side, String tier) {
        String sql = "SELECT markup_bps FROM fx_pair_markups "
                + "WHERE pair = ? AND side = ? AND tier = ? AND is_active = TRUE LIMIT 1";
        try {
            List<BigDecimal> rows = jdbc.queryForList(sql, BigDecimal.class, pair, side, tier);
            if (!rows.isEmpty()) return rows.get(0);
            if (!"default".equals(tier)) {
                List<BigDecimal> def = jdbc.queryForList(sql, BigDecimal.class, pair, side, "default");
                if (!def.isEmpty()) {
                    return def.get(0);
                }
            }
        } catch (DataAccessException e) {
            log.warn("[fx-quote] markup lookup failed pair={} side={} tier={}", pair, side, tier, e);
        }
        quoteMissesCounter.increment();
        throw new IllegalStateException("no markup row for " + pair + " " + side + " (tier=" + tier + " nor default)");
    }

    private static BigDecimal applyMarkup(BigDecimal mid, BigDecimal bps, int direction) {
        // markup_bps is in basis points (1 bp = 0.0001). bid = mid * (1 - bps/10_000),
        // ask = mid * (1 + bps/10_000). direction = -1 for bid, +1 for ask.
        BigDecimal factor = BigDecimal.ONE.add(
                bps.divide(new BigDecimal("10000"), 10, RoundingMode.HALF_UP)
                        .multiply(new BigDecimal(direction)));
        return mid.multiply(factor);
    }

    private void purgeExpired(Instant now) {
        // Soft GC of the cache so a long-running process does not keep stale
        // entries around. Cheap because the map is tiny (a few hundred entries
        // at most during normal use).
        cache.entrySet().removeIf(e -> now.isAfter(e.getValue().expiresAt.plus(Duration.ofMinutes(5))));
    }

    /** Cached quote snapshot reused by {@link FxHedgeController}. */
    public record CachedQuote(
            String quoteId,
            String pair,
            String tier,
            BigDecimal bid,
            BigDecimal ask,
            BigDecimal mid,
            Instant capturedAt,
            Instant expiresAt
    ) {}

    /**
     * Returns a copy of the static stub mid table — exposed for the desk console
     * landing page so it can show "current internal mid" without hitting quote()
     * for every pair.
     */
    public Map<String, BigDecimal> stubMids() {
        return new HashMap<>(STUB_MIDS);
    }
}
