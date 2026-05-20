package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.RejectCode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
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
     * Cold-start prime-broker mid map.
     *
     * <p>Used both as (a) the catalogue advertised by {@code /internal/v1/fx/mids}
     * (the trading-desk Treasury page renders one card per entry here) and
     * (b) the fallback {@link #midWithSourceForPair} returns when the
     * {@link OmsFxMidSubscriber} has not yet seen a live tick for the pair
     * (typical for the first ~1 s after a fresh deploy, or for pairs the
     * vendor feed is currently muted on).
     *
     * <p>Keep in lockstep with:
     * <ul>
     *   <li>{@code marketdata-platform/src/config/massive-fx-limits.ts#MASSIVE_FX_DEFAULT_PAIRS}
     *       — the vendor subscription that produces the live ticks that
     *       eventually overwrite these stubs.
     *   <li>{@code db/migration/V38__fx_pair_markups_european_bank_set.sql}
     *       — at least one {@code default}-tier markup row per pair so
     *       {@link #lookupMarkupBps} resolves on every advertised pair.
     * </ul>
     *
     * <p>Values are approximate G10/Nordic/CEE market levels as of 2026-05;
     * they only get used until the first live tick arrives, so absolute
     * precision is not required.
     */
    private static final Map<String, BigDecimal> STUB_MIDS = Map.ofEntries(
            // EUR base
            Map.entry("EURUSD", new BigDecimal("1.0850")),
            Map.entry("EURGBP", new BigDecimal("0.8543")),
            Map.entry("EURCHF", new BigDecimal("0.9525")),
            Map.entry("EURJPY", new BigDecimal("166.55")),
            Map.entry("EURSEK", new BigDecimal("11.3383")),
            Map.entry("EURNOK", new BigDecimal("11.7180")),
            Map.entry("EURDKK", new BigDecimal("7.4540")),
            Map.entry("EURPLN", new BigDecimal("4.3671")),
            Map.entry("EURCZK", new BigDecimal("25.1178")),
            Map.entry("EURHUF", new BigDecimal("391.14")),
            Map.entry("EURCAD", new BigDecimal("1.4810")),
            Map.entry("EURAUD", new BigDecimal("1.6316")),

            // GBP base
            Map.entry("GBPUSD", new BigDecimal("1.2700")),
            Map.entry("GBPEUR", new BigDecimal("1.1706")),
            Map.entry("GBPCHF", new BigDecimal("1.1151")),
            Map.entry("GBPJPY", new BigDecimal("194.95")),
            Map.entry("GBPSEK", new BigDecimal("13.2715")),
            Map.entry("GBPNOK", new BigDecimal("13.7160")),
            Map.entry("GBPAUD", new BigDecimal("1.9098")),
            Map.entry("GBPCAD", new BigDecimal("1.7336")),

            // USD base
            Map.entry("USDEUR", new BigDecimal("0.9217")),
            Map.entry("USDGBP", new BigDecimal("0.7874")),
            Map.entry("USDJPY", new BigDecimal("153.50")),
            Map.entry("USDCHF", new BigDecimal("0.8780")),
            Map.entry("USDCAD", new BigDecimal("1.3650")),
            Map.entry("USDSEK", new BigDecimal("10.4500")),
            Map.entry("USDNOK", new BigDecimal("10.8000")),
            Map.entry("USDDKK", new BigDecimal("6.8700")),
            Map.entry("USDPLN", new BigDecimal("4.0250")),
            Map.entry("USDCZK", new BigDecimal("23.1500")),
            Map.entry("USDHUF", new BigDecimal("360.50")),
            Map.entry("USDSGD", new BigDecimal("1.3500")),
            Map.entry("USDHKD", new BigDecimal("7.7900")),
            Map.entry("USDCNH", new BigDecimal("7.2500")),
            Map.entry("USDMXN", new BigDecimal("17.2500")),
            Map.entry("USDZAR", new BigDecimal("18.3500")),
            Map.entry("USDTRY", new BigDecimal("32.8000")),

            // *-USD (parity with the prior list / customer-frontend rate fetches)
            Map.entry("AUDUSD", new BigDecimal("0.6650")),
            Map.entry("NZDUSD", new BigDecimal("0.6050"))
    );

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final OmsConfig omsConfig;
    private final FxMarkupOverridesService overrides;
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

    public FxQuoteService(JdbcTemplate jdbc, Clock clock, OmsConfig omsConfig,
                          FxMarkupOverridesService overrides, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.omsConfig = omsConfig;
        this.overrides = overrides;
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
        if (!omsConfig.getFx().isStubMidsAllowed()) {
            throw new FxQuoteStaleException(
                    HttpStatus.UNPROCESSABLE_ENTITY,
                    RejectCode.RISK_FX_STALE_QUOTE,
                    "fx_mid_stale",
                    "vendor mid stale or absent for pair " + pair);
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
     * Read-only listing of the active {@code fx_pair_markups} grid for
     * operator surfaces (beard-admin FX Markups page). Returns one row per
     * (pair, side, tier) tuple with the markup in bps + description, ordered
     * by pair / tier / side for stable rendering. Filters are inclusive (all
     * null/blank = no filter); the description column is the operator's
     * intent string, useful for audit trails.
     *
     * <p>Read-only by design: writes still go through SQL today (operators
     * use Flyway migrations + ad-hoc {@code UPDATE} from the OMS DB shell).
     * A v1.5 follow-up can wrap this with a four-eyes editor flow once we
     * have the audit trail story decided. See plans/oms-multi-currency-invest-accounts.md
     * §8.9 "deferred to v1.5".
     *
     * @param pairFilter optional pair to filter on (case-insensitive), or null for all
     * @param tierFilter optional tier to filter on (case-insensitive), or null for all
     */
    public List<Map<String, Object>> listMarkups(String pairFilter, String tierFilter) {
        StringBuilder sql = new StringBuilder(
                "SELECT pair, side, tier, markup_bps, description, is_active, updated_at "
                        + "FROM fx_pair_markups WHERE is_active = TRUE");
        java.util.List<Object> args = new java.util.ArrayList<>();
        if (pairFilter != null && !pairFilter.isBlank()) {
            sql.append(" AND UPPER(pair) = ?");
            args.add(pairFilter.toUpperCase());
        }
        if (tierFilter != null && !tierFilter.isBlank()) {
            sql.append(" AND LOWER(tier) = ?");
            args.add(tierFilter.toLowerCase());
        }
        sql.append(" ORDER BY pair ASC, tier ASC, side ASC");
        try {
            return jdbc.queryForList(sql.toString(), args.toArray());
        } catch (DataAccessException e) {
            log.warn("[fx-quote] listMarkups failed pair={} tier={}", pairFilter, tierFilter, e);
            return List.of();
        }
    }

    /**
     * @return the effective markup in bps for (pair, side, tier) — the
     *         rate-card row from {@code fx_pair_markups} (with the
     *         tier=default waterfall) plus any active tactical overrides
     *         from {@link FxMarkupOverridesService}. Mirrors
     *         {@link OmsFxCustomerQuotePublisher#lookupMarkup} so the
     *         streamed display rate and the locked submit-time rate
     *         compute the same number.
     * @throws IllegalStateException when neither the tier row nor the
     *         default row exists. Overrides do not invent a quote on a
     *         pair without a rate-card.
     */
    BigDecimal lookupMarkupBps(String pair, String side, String tier) {
        BigDecimal base = lookupBaseMarkupBps(pair, side, tier);
        BigDecimal additive = overrides.additiveBpsFor(pair, side, tier, clock.instant());
        if (additive.signum() == 0) return base;
        BigDecimal effective = base.add(additive);
        return effective.signum() < 0 ? BigDecimal.ZERO : effective;
    }

    private BigDecimal lookupBaseMarkupBps(String pair, String side, String tier) {
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
