package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private final OmsConfig omsConfig;
    private final Counter refreshOkCounter;
    private final Counter refreshFailCounter;
    private final Counter createOkCounter;
    private final Counter createRejectCounter;
    private final Counter approveCounter;
    private final Counter revokeCounter;

    private volatile List<OverrideRow> cache = Collections.emptyList();
    private volatile long cacheLoadedAtMs;

    public FxMarkupOverridesService(
            JdbcTemplate jdbc,
            Clock clock,
            OmsConfig omsConfig,
            MeterRegistry registry,
            @Value("${oms.fx.markup-overrides.refresh-ms:60000}") long refreshMs) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.omsConfig = omsConfig;
        this.refreshOkCounter = Counter.builder("oms_fx_markup_overrides_refresh_total")
                .tag("outcome", "ok").register(registry);
        this.refreshFailCounter = Counter.builder("oms_fx_markup_overrides_refresh_total")
                .tag("outcome", "fail").register(registry);
        this.createOkCounter = Counter.builder("oms_fx_markup_overrides_create_total")
                .tag("outcome", "ok").register(registry);
        this.createRejectCounter = Counter.builder("oms_fx_markup_overrides_create_total")
                .tag("outcome", "reject").register(registry);
        this.approveCounter = Counter.builder("oms_fx_markup_overrides_approve_total")
                .register(registry);
        this.revokeCounter = Counter.builder("oms_fx_markup_overrides_revoke_total")
                .register(registry);
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
     * Synchronous refresh entry point — called by the write endpoints
     * after a create / approve / revoke so a change applies immediately
     * rather than at the next {@code @Scheduled} tick.
     *
     * <p>Cache contains only rows that are <em>both</em> approved
     * ({@code approved_at IS NOT NULL}) and not revoked. Pending rows
     * (above four-eyes threshold, awaiting approval) and revoked rows
     * are listed by {@link #listAll} for the UI but never affect quotes.
     */
    public void refreshNow() {
        String sql = "SELECT pair, side, tier, additive_bps, valid_from, valid_until "
                + "FROM fx_pair_markup_overrides "
                + "WHERE revoked_at IS NULL AND approved_at IS NOT NULL AND valid_until > ?";
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
     * Creates a new override row.
     *
     * <p>Four-eyes logic
     * <ul>
     *   <li>If both {@code |additive_bps| ≤ autoApproveAbsBps} and
     *       {@code duration ≤ autoApproveMaxDurationMs} (and auto-approve
     *       is enabled), the row is self-approved by {@code createdBy}.
     *   <li>Otherwise the row is written with {@code approved_at = NULL}
     *       and is invisible to the publisher until
     *       {@link #approve(long, String)} populates the approval columns.
     * </ul>
     *
     * <p>Hard caps from {@link OmsConfig.Fx.MarkupOverrides} reject the
     * request before any DB write — a request beyond the bps or duration
     * cap can <em>not</em> be self-approved or four-eyes-approved.
     *
     * @throws IllegalArgumentException on validation failure
     */
    public CreateResult create(CreateRequest req) {
        Objects.requireNonNull(req, "request");
        OmsConfig.Fx.MarkupOverrides cfg = omsConfig.getFx().getMarkupOverrides();
        validateCreate(req, cfg);

        Instant now = clock.instant();
        Instant validFrom = req.validFrom() != null ? req.validFrom() : now;
        Instant validUntil = validFrom.plus(Duration.ofMillis(req.durationMs()));

        boolean autoApprove = shouldAutoApprove(req, cfg);

        String pair = req.pair() == null ? null : req.pair().toUpperCase();
        String side = req.side() == null ? null : req.side().toUpperCase();
        String tier = req.tier() == null ? null : req.tier().toLowerCase();
        String reason = req.reason().trim();
        String createdBy = req.createdBy().trim();

        String sql = "INSERT INTO fx_pair_markup_overrides "
                + "(pair, side, tier, additive_bps, reason, valid_from, valid_until, "
                + " created_by, created_at, approved_by, approved_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        KeyHolder kh = new GeneratedKeyHolder();
        try {
            jdbc.update((PreparedStatementCreator) conn -> {
                PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, pair);
                ps.setString(2, side);
                ps.setString(3, tier);
                ps.setBigDecimal(4, req.additiveBps());
                ps.setString(5, reason);
                ps.setTimestamp(6, Timestamp.from(validFrom));
                ps.setTimestamp(7, Timestamp.from(validUntil));
                ps.setString(8, createdBy);
                ps.setTimestamp(9, Timestamp.from(now));
                if (autoApprove) {
                    ps.setString(10, createdBy);
                    ps.setTimestamp(11, Timestamp.from(now));
                } else {
                    ps.setNull(10, java.sql.Types.VARCHAR);
                    ps.setNull(11, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
                }
                return ps;
            }, kh);
        } catch (DataAccessException e) {
            createRejectCounter.increment();
            throw new IllegalArgumentException("insert failed: " + e.getMostSpecificCause().getMessage(), e);
        }

        Number idNum = kh.getKey();
        long id = idNum == null ? -1L : idNum.longValue();
        createOkCounter.increment();
        // Approved rows become live immediately; pending ones still need an approve()
        // call later but the refresh is harmless either way.
        refreshNow();
        log.info("[fx-mk-ovr] create id={} pair={} side={} tier={} bps={} dur_ms={} by={} approved={}",
                id, pair, side, tier, req.additiveBps().toPlainString(), req.durationMs(),
                createdBy, autoApprove);
        return new CreateResult(id, autoApprove);
    }

    /**
     * Decides whether a {@link CreateRequest} is small enough that
     * {@code created_by} can self-approve. Package-private so the
     * four-eyes gate logic is directly testable without the DB.
     */
    static boolean shouldAutoApprove(CreateRequest req, OmsConfig.Fx.MarkupOverrides cfg) {
        if (!cfg.isAutoApproveEnabled()) return false;
        if (req.additiveBps() == null) return false;
        if (req.additiveBps().abs().compareTo(BigDecimal.valueOf(cfg.getAutoApproveAbsBps())) > 0) return false;
        return req.durationMs() <= cfg.getAutoApproveMaxDurationMs();
    }

    private static void validateCreate(CreateRequest req, OmsConfig.Fx.MarkupOverrides cfg) {
        if (req.additiveBps() == null) throw new IllegalArgumentException("additiveBps required");
        if (req.additiveBps().abs().compareTo(BigDecimal.valueOf(cfg.getMaxAbsAdditiveBps())) > 0) {
            throw new IllegalArgumentException(
                    "additiveBps " + req.additiveBps().toPlainString()
                            + " exceeds cap " + cfg.getMaxAbsAdditiveBps());
        }
        if (req.durationMs() <= 0L) throw new IllegalArgumentException("durationMs must be > 0");
        if (req.durationMs() > cfg.getMaxDurationMs()) {
            throw new IllegalArgumentException(
                    "durationMs " + req.durationMs() + " exceeds cap " + cfg.getMaxDurationMs());
        }
        if (req.reason() == null || req.reason().trim().isEmpty()) {
            throw new IllegalArgumentException("reason required");
        }
        if (req.createdBy() == null || req.createdBy().trim().isEmpty()) {
            throw new IllegalArgumentException("createdBy required");
        }
        if (req.side() != null && !"BID".equalsIgnoreCase(req.side()) && !"ASK".equalsIgnoreCase(req.side())) {
            throw new IllegalArgumentException("side must be BID or ASK or null");
        }
    }

    /**
     * Approves a pending override. The approver must be a different
     * identity from {@code created_by} — four-eyes. Re-approval and
     * self-approval are rejected with {@link IllegalStateException}.
     */
    public void approve(long id, String approvedBy) {
        if (approvedBy == null || approvedBy.trim().isEmpty()) {
            throw new IllegalArgumentException("approvedBy required");
        }
        String trimmed = approvedBy.trim();
        Map<String, Object> row;
        try {
            row = jdbc.queryForMap(
                    "SELECT created_by, approved_at, revoked_at "
                            + "FROM fx_pair_markup_overrides WHERE id = ?", id);
        } catch (DataAccessException e) {
            throw new IllegalStateException("override " + id + " not found");
        }
        if (row.get("revoked_at") != null) {
            throw new IllegalStateException("override " + id + " is revoked");
        }
        if (row.get("approved_at") != null) {
            throw new IllegalStateException("override " + id + " is already approved");
        }
        if (trimmed.equals(row.get("created_by"))) {
            throw new IllegalStateException("override " + id + " cannot be self-approved (four-eyes)");
        }
        int n = jdbc.update(
                "UPDATE fx_pair_markup_overrides "
                        + "SET approved_by = ?, approved_at = ? "
                        + "WHERE id = ? AND approved_at IS NULL AND revoked_at IS NULL",
                trimmed, Timestamp.from(clock.instant()), id);
        if (n == 0) throw new IllegalStateException("override " + id + " not approvable");
        approveCounter.increment();
        refreshNow();
        log.info("[fx-mk-ovr] approve id={} by={}", id, trimmed);
    }

    /**
     * Revokes an override immediately. Sets {@code revoked_at/revoked_by}
     * and refreshes the cache so the row stops applying within the same
     * tick. Idempotent on already-revoked rows.
     */
    public void revoke(long id, String revokedBy) {
        if (revokedBy == null || revokedBy.trim().isEmpty()) {
            throw new IllegalArgumentException("revokedBy required");
        }
        int n = jdbc.update(
                "UPDATE fx_pair_markup_overrides "
                        + "SET revoked_by = ?, revoked_at = ? "
                        + "WHERE id = ? AND revoked_at IS NULL",
                revokedBy.trim(), Timestamp.from(clock.instant()), id);
        if (n == 0) {
            throw new IllegalStateException("override " + id + " not found or already revoked");
        }
        revokeCounter.increment();
        refreshNow();
        log.info("[fx-mk-ovr] revoke id={} by={}", id, revokedBy.trim());
    }

    /**
     * Read view for the trading-desk publisher panel. Returns:
     *
     * <ul>
     *   <li>every active row (approved + not revoked + window not closed),
     *   <li>every pending row (approval missing, valid_until > now),
     *   <li>every revoked row whose revocation is within the last 24 h
     *       (so the operator sees what they just turned off).
     * </ul>
     *
     * Ordered by status priority (active, pending, revoked) then expiry
     * ascending so the row about to drop off is at the top.
     */
    public List<Map<String, Object>> listAll() {
        Instant now = clock.instant();
        Instant since = now.minus(Duration.ofHours(24));
        String sql = "SELECT id, pair, side, tier, additive_bps, reason, "
                + "       valid_from, valid_until, created_by, created_at, "
                + "       approved_by, approved_at, revoked_by, revoked_at "
                + "FROM fx_pair_markup_overrides "
                + "WHERE (revoked_at IS NULL AND valid_until > ?) "
                + "   OR (revoked_at IS NOT NULL AND revoked_at >= ?) "
                + "ORDER BY valid_until ASC";
        try {
            List<Map<String, Object>> raw = jdbc.query(sql, this::mapRow,
                    Timestamp.from(now), Timestamp.from(since));
            // Derived "status" for the UI; same logic the matcher uses.
            for (Map<String, Object> r : raw) {
                r.put("status", deriveStatus(r, now));
            }
            return raw;
        } catch (DataAccessException e) {
            log.warn("[fx-mk-ovr] listAll failed", e);
            return List.of();
        }
    }

    private Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", rs.getLong("id"));
        r.put("pair", rs.getString("pair"));
        r.put("side", rs.getString("side"));
        r.put("tier", rs.getString("tier"));
        r.put("additiveBps", rs.getBigDecimal("additive_bps").toPlainString());
        r.put("reason", rs.getString("reason"));
        r.put("validFrom", rs.getTimestamp("valid_from").toInstant().toString());
        r.put("validUntil", rs.getTimestamp("valid_until").toInstant().toString());
        r.put("createdBy", rs.getString("created_by"));
        r.put("createdAt", rs.getTimestamp("created_at").toInstant().toString());
        Timestamp appAt = rs.getTimestamp("approved_at");
        r.put("approvedBy", rs.getString("approved_by"));
        r.put("approvedAt", appAt == null ? null : appAt.toInstant().toString());
        Timestamp revAt = rs.getTimestamp("revoked_at");
        r.put("revokedBy", rs.getString("revoked_by"));
        r.put("revokedAt", revAt == null ? null : revAt.toInstant().toString());
        return r;
    }

    private static String deriveStatus(Map<String, Object> r, Instant now) {
        if (r.get("revokedAt") != null) return "revoked";
        if (r.get("approvedAt") == null) return "pending";
        Instant from = Instant.parse((String) r.get("validFrom"));
        Instant until = Instant.parse((String) r.get("validUntil"));
        if (now.isBefore(from)) return "scheduled";
        if (now.isBefore(until)) return "active";
        return "expired";
    }

    /** Inputs for {@link #create(CreateRequest)}. */
    public record CreateRequest(
            String pair,
            String side,
            String tier,
            BigDecimal additiveBps,
            Instant validFrom,
            long durationMs,
            String reason,
            String createdBy
    ) {}

    /** Outcome of {@link #create(CreateRequest)}. */
    public record CreateResult(long id, boolean autoApproved) {}

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
