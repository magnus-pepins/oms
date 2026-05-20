package com.balh.oms.fx;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tactical, time-bounded additive markup overrides on top of the
 * permanent {@code fx_pair_markups} grid.
 *
 * <p>Backs P3.6 of {@code plans/fx-tier-quotes-production.md}. Trading-desk
 * Treasury (P3.8) writes rows to {@code fx_pair_markup_overrides} (V47);
 * this service caches the active ones and exposes
 * {@link #additiveBpsFor(String, String, String, Instant)} so both the
 * streaming publisher ({@link OmsFxCustomerQuotePublisher}) and the
 * HTTP {@code /quote} path ({@link FxQuoteService}) compute the same
 * effective bps. They <strong>must</strong> stay in lockstep: if only
 * one side picks up an override the BFF drift check (P2.1) will 409
 * every order.
 *
 * <p>Cache strategy
 * <ul>
 *   <li>Refreshed on a {@code @Scheduled} cadence (default 60s, matches
 *       {@code OmsFxCustomerQuotePublisher} markup cache).
 *   <li>Each refresh pulls every unrevoked row whose window has not yet
 *       fully ended ({@code valid_until > now}). Rows whose
 *       {@code valid_from} is still in the future are kept so a
 *       scheduled-ahead override goes live within one refresh tick
 *       after its start.
 *   <li>The matching helper filters again at call-time on
 *       {@code valid_from <= now < valid_until}.
 * </ul>
 *
 * <p>Sizing assumption: single-digit active rows at most. Walking a
 * small {@link List} on every lookup is cheaper than any index scheme
 * for this volume and keeps the matcher trivial to reason about.
 */
@Service
public class FxMarkupOverridesService {

    private static final Logger log = LoggerFactory.getLogger(FxMarkupOverridesService.class);

    /** Minimum refresh cadence we'll accept from config to avoid pathological reload loops. */
    private static final long MIN_REFRESH_MS = 5_000L;

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final Counter refreshOkCounter;
    private final Counter refreshFailCounter;

    private volatile List<OverrideRow> cache = Collections.emptyList();
    private volatile long cacheLoadedAtMs;

    public FxMarkupOverridesService(
            JdbcTemplate jdbc,
            Clock clock,
            MeterRegistry registry,
            @Value("${oms.fx.markup-overrides.refresh-ms:60000}") long refreshMs) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.refreshOkCounter = Counter.builder("oms_fx_markup_overrides_refresh_total")
                .tag("outcome", "ok")
                .description("FxMarkupOverridesService cache refreshes")
                .register(registry);
        this.refreshFailCounter = Counter.builder("oms_fx_markup_overrides_refresh_total")
                .tag("outcome", "fail")
                .description("FxMarkupOverridesService cache refreshes")
                .register(registry);
        // Read but unused so an operator who sets the config below the floor sees the warning early.
        long effective = Math.max(MIN_REFRESH_MS, refreshMs);
        if (effective != refreshMs) {
            log.warn("[fx-mk-ovr] refresh-ms {} below floor; clamped to {}", refreshMs, effective);
        }
    }

    @Scheduled(
            fixedDelayString = "${oms.fx.markup-overrides.refresh-ms:60000}",
            initialDelayString = "${oms.fx.markup-overrides.refresh-ms:60000}")
    public void refresh() {
        refreshNow();
    }

    /**
     * Synchronous refresh entry point — wired up after a write API call
     * (P3.7) so a newly inserted override applies immediately rather
     * than at the next {@code @Scheduled} tick.
     */
    public void refreshNow() {
        String sql = "SELECT pair, side, tier, additive_bps, valid_from, valid_until "
                + "FROM fx_pair_markup_overrides "
                + "WHERE revoked_at IS NULL AND valid_until > ?";
        try {
            Instant now = clock.instant();
            List<OverrideRow> next = new ArrayList<>();
            jdbc.query(sql,
                    ps -> ps.setObject(1, java.sql.Timestamp.from(now)),
                    rs -> {
                        String pair = rs.getString("pair");
                        String side = rs.getString("side");
                        String tier = rs.getString("tier");
                        BigDecimal bps = rs.getBigDecimal("additive_bps");
                        Instant from = rs.getTimestamp("valid_from").toInstant();
                        Instant until = rs.getTimestamp("valid_until").toInstant();
                        if (bps == null || from == null || until == null) return;
                        next.add(new OverrideRow(
                                pair == null ? null : pair.toUpperCase(),
                                side == null ? null : side.toUpperCase(),
                                tier == null ? null : tier.toLowerCase(),
                                bps,
                                from,
                                until));
                    });
            cache = Collections.unmodifiableList(next);
            cacheLoadedAtMs = clock.millis();
            refreshOkCounter.increment();
            log.debug("[fx-mk-ovr] cache refreshed entries={}", next.size());
        } catch (DataAccessException e) {
            refreshFailCounter.increment();
            log.warn("[fx-mk-ovr] refresh failed; reusing prior cache size={}", cache.size(), e);
        }
    }

    /**
     * Sum of additive bps for the (pair, side, tier) tuple at {@code now},
     * across every cached row that matches by wildcard semantics (a
     * {@code null} dimension on the row matches any value) and whose
     * window contains {@code now}.
     *
     * @return {@link BigDecimal#ZERO} if no rows match. Never null.
     */
    public BigDecimal additiveBpsFor(String pair, String side, String tier, Instant now) {
        if (pair == null || side == null || tier == null || now == null) return BigDecimal.ZERO;
        String pairUp = pair.toUpperCase();
        String sideUp = side.toUpperCase();
        String tierLo = tier.toLowerCase();
        BigDecimal sum = BigDecimal.ZERO;
        for (OverrideRow r : cache) {
            if (!r.matches(pairUp, sideUp, tierLo, now)) continue;
            sum = sum.add(r.additiveBps());
        }
        return sum;
    }

    /** Diagnostic snapshot for the customer-quote/status endpoint. */
    public OverridesStatus status() {
        Instant now = clock.instant();
        int active = 0;
        long nextExpiryMs = 0L;
        for (OverrideRow r : cache) {
            if (r.matchesWindow(now)) {
                active++;
                long t = r.validUntil().toEpochMilli();
                if (nextExpiryMs == 0L || t < nextExpiryMs) nextExpiryMs = t;
            }
        }
        return new OverridesStatus(cache.size(), active, nextExpiryMs, cacheLoadedAtMs);
    }

    /** Package-private for unit tests — prime the cache directly. */
    void primeCache(List<OverrideRow> rows) {
        this.cache = Collections.unmodifiableList(new ArrayList<>(rows));
        this.cacheLoadedAtMs = clock.millis();
    }

    /**
     * One row of the override cache. {@code pair} / {@code side} /
     * {@code tier} are nullable to encode wildcard scope. Pair is upper
     * case, side is upper case, tier is lower case — matches the
     * canonical form the publisher and {@link FxQuoteService} use.
     */
    public record OverrideRow(
            String pair,
            String side,
            String tier,
            BigDecimal additiveBps,
            Instant validFrom,
            Instant validUntil
    ) {
        boolean matches(String pairUp, String sideUp, String tierLo, Instant now) {
            if (!matchesWindow(now)) return false;
            if (pair != null && !pair.equals(pairUp)) return false;
            if (side != null && !side.equals(sideUp)) return false;
            if (tier != null && !tier.equals(tierLo)) return false;
            return true;
        }

        boolean matchesWindow(Instant now) {
            return !now.isBefore(validFrom) && now.isBefore(validUntil);
        }
    }

    /** Status payload for {@code /internal/v1/fx/customer-quote/status}. */
    public record OverridesStatus(
            int cachedRowCount,
            int activeCount,
            long nextExpiryEpochMs,
            long cacheLoadedAtMs
    ) {}
}
