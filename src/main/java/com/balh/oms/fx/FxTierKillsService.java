package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
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
 * Per-tier publisher kill-switches (plan A2 in
 * {@code plans/fx-treasury-auto-hedger-and-publisher-controls.md}).
 *
 * <p>Sibling of {@link FxMarkupOverridesService} — same governance
 * columns (created_by / approved_by / revoked_by), same NATS-bus
 * cross-JVM invalidation, same scheduled-refresh safety net — but a
 * different semantic model:
 *
 * <ul>
 *   <li>A row "matches" or it doesn't (no numeric sum). The publisher
 *       calls {@link #isKilled(String, String, Instant)} per (pair, tier)
 *       and skips publish when true.
 *   <li>Wildcard pair (NULL) means "this tier across every pair we
 *       publish" — primary use case. A scoped pair (e.g. {@code EURUSD})
 *       kills only that pair's tier stream.
 *   <li>No BID/ASK side: a half-killed quote is a customer-side bug
 *       waiting to happen, and the kill button operationally means "stop
 *       quoting this tier" not "stop quoting one side of it".
 * </ul>
 *
 * <p>Four-eyes rules (plan A2 decision):
 * <ul>
 *   <li>Scoped-pair kills under {@code autoApproveMaxDurationMs} auto-
 *       approve via {@code created_by = approved_by}.
 *   <li>Wildcard-pair kills (pair=NULL) <strong>always</strong> require a
 *       second approver. A wildcard "kill business" reaches every
 *       customer in that tier; the bar is intentionally higher than the
 *       analogous markup widening.
 * </ul>
 *
 * <p>Sizing assumption: single-digit active rows at most. Walking a
 * small {@link List} on every {@code isKilled} call is cheaper than
 * any index scheme for this volume and keeps the matcher trivial.
 */
@Service
public class FxTierKillsService {

    private static final Logger log = LoggerFactory.getLogger(FxTierKillsService.class);

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

    private volatile List<KillRow> cache = Collections.emptyList();
    private volatile long cacheLoadedAtMs;
    private volatile TierKillsChangePropagator changePropagator = TierKillsChangePropagator.NOOP;

    public FxTierKillsService(
            JdbcTemplate jdbc,
            Clock clock,
            OmsConfig omsConfig,
            MeterRegistry registry,
            @Value("${oms.fx.tier-kills.refresh-ms:60000}") long refreshMs) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.omsConfig = omsConfig;
        this.refreshOkCounter = Counter.builder("oms_fx_tier_kills_refresh_total")
                .tag("outcome", "ok").register(registry);
        this.refreshFailCounter = Counter.builder("oms_fx_tier_kills_refresh_total")
                .tag("outcome", "fail").register(registry);
        this.createOkCounter = Counter.builder("oms_fx_tier_kills_create_total")
                .tag("outcome", "ok").register(registry);
        this.createRejectCounter = Counter.builder("oms_fx_tier_kills_create_total")
                .tag("outcome", "reject").register(registry);
        this.approveCounter = Counter.builder("oms_fx_tier_kills_approve_total")
                .register(registry);
        this.revokeCounter = Counter.builder("oms_fx_tier_kills_revoke_total")
                .register(registry);
        long effective = Math.max(MIN_REFRESH_MS, refreshMs);
        if (effective != refreshMs) {
            log.warn("[fx-tier-kills] refresh-ms {} below floor; clamped to {}", refreshMs, effective);
        }
    }

    /** Setter-wired by {@link FxTierKillsNatsInvalidationBus} when NATS is enabled. */
    public void setChangePropagator(TierKillsChangePropagator p) {
        this.changePropagator = p == null ? TierKillsChangePropagator.NOOP : p;
    }

    @Scheduled(
            fixedDelayString = "${oms.fx.tier-kills.refresh-ms:60000}",
            initialDelayString = "${oms.fx.tier-kills.refresh-ms:60000}")
    public void refresh() {
        refreshNow();
    }

    /**
     * Synchronous refresh entry point — called by the write endpoints
     * after a create / approve / revoke so a change applies immediately
     * rather than at the next {@code @Scheduled} tick. Cache contains
     * only rows that are both approved and not revoked; pending rows
     * (above four-eyes threshold) are listed by {@link #listAll} for
     * the UI but never affect the publisher.
     */
    public void refreshNow() {
        String sql = "SELECT pair, tier, valid_from, valid_until "
                + "FROM fx_pair_tier_kills "
                + "WHERE revoked_at IS NULL AND approved_at IS NOT NULL AND valid_until > ?";
        try {
            Instant now = clock.instant();
            List<KillRow> next = new ArrayList<>();
            jdbc.query(sql,
                    ps -> ps.setObject(1, java.sql.Timestamp.from(now)),
                    rs -> {
                        String pair = rs.getString("pair");
                        String tier = rs.getString("tier");
                        Instant from = rs.getTimestamp("valid_from").toInstant();
                        Instant until = rs.getTimestamp("valid_until").toInstant();
                        if (tier == null || from == null || until == null) return;
                        next.add(new KillRow(
                                pair == null ? null : pair.toUpperCase(),
                                tier.toLowerCase(),
                                from,
                                until));
                    });
            cache = Collections.unmodifiableList(next);
            cacheLoadedAtMs = clock.millis();
            refreshOkCounter.increment();
            log.debug("[fx-tier-kills] cache refreshed entries={}", next.size());
        } catch (DataAccessException e) {
            refreshFailCounter.increment();
            log.warn("[fx-tier-kills] refresh failed; reusing prior cache size={}", cache.size(), e);
        }
    }

    /**
     * Returns {@code true} if any cached, in-window row matches the
     * (pair, tier) tuple at {@code now}. A row with {@code pair = null}
     * matches every pair; {@code tier} must equal exactly.
     */
    public boolean isKilled(String pair, String tier, Instant now) {
        if (pair == null || tier == null || now == null) return false;
        String pairUp = pair.toUpperCase();
        String tierLo = tier.toLowerCase();
        for (KillRow r : cache) {
            if (r.matches(pairUp, tierLo, now)) return true;
        }
        return false;
    }

    /** Diagnostic snapshot for the customer-quote/status endpoint. */
    public KillsStatus status() {
        Instant now = clock.instant();
        int active = 0;
        long nextExpiryMs = 0L;
        for (KillRow r : cache) {
            if (r.matchesWindow(now)) {
                active++;
                long t = r.validUntil().toEpochMilli();
                if (nextExpiryMs == 0L || t < nextExpiryMs) nextExpiryMs = t;
            }
        }
        return new KillsStatus(cache.size(), active, nextExpiryMs, cacheLoadedAtMs);
    }

    /** Package-private for unit tests — prime the cache directly. */
    void primeCache(List<KillRow> rows) {
        this.cache = Collections.unmodifiableList(new ArrayList<>(rows));
        this.cacheLoadedAtMs = clock.millis();
    }

    /**
     * Creates a new kill row. See {@link FxTierKillsService} class
     * javadoc for four-eyes semantics. Wildcard-pair kills always go
     * through approval; scoped kills under the auto-approve duration
     * self-approve.
     *
     * @throws IllegalArgumentException on validation failure
     */
    public CreateResult create(CreateRequest req) {
        Objects.requireNonNull(req, "request");
        OmsConfig.Fx.TierKills cfg = omsConfig.getFx().getTierKills();
        validateCreate(req, cfg);

        Instant now = clock.instant();
        Instant validFrom = req.validFrom() != null ? req.validFrom() : now;
        Instant validUntil = validFrom.plus(Duration.ofMillis(req.durationMs()));

        boolean autoApprove = shouldAutoApprove(req, cfg);

        String pair = req.pair() == null ? null : req.pair().toUpperCase();
        String tier = req.tier().toLowerCase();
        String reason = req.reason().trim();
        String createdBy = req.createdBy().trim();

        String sql = "INSERT INTO fx_pair_tier_kills "
                + "(pair, tier, reason, valid_from, valid_until, "
                + " created_by, created_at, approved_by, approved_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "RETURNING id";

        long id;
        try {
            Long idBoxed = jdbc.queryForObject(sql, Long.class,
                    pair, tier, reason,
                    Timestamp.from(validFrom), Timestamp.from(validUntil),
                    createdBy, Timestamp.from(now),
                    autoApprove ? createdBy : null,
                    autoApprove ? Timestamp.from(now) : null);
            id = idBoxed == null ? -1L : idBoxed;
        } catch (DataAccessException e) {
            createRejectCounter.increment();
            throw new IllegalArgumentException("insert failed: " + e.getMostSpecificCause().getMessage(), e);
        }
        createOkCounter.increment();
        refreshNow();
        changePropagator.localChanged("create", id);
        log.info("[fx-tier-kills] create id={} pair={} tier={} dur_ms={} by={} approved={}",
                id, pair, tier, req.durationMs(), createdBy, autoApprove);
        return new CreateResult(id, autoApprove);
    }

    /**
     * A kill row may self-approve only when (a) auto-approve is on, (b)
     * the duration fits the auto-approve cap, and (c) the kill is
     * pair-scoped. A wildcard kill always goes through four-eyes —
     * see {@link FxTierKillsService} class javadoc decision.
     */
    static boolean shouldAutoApprove(CreateRequest req, OmsConfig.Fx.TierKills cfg) {
        if (!cfg.isAutoApproveEnabled()) return false;
        if (req.pair() == null) return false;
        return req.durationMs() <= cfg.getAutoApproveMaxDurationMs();
    }

    private static void validateCreate(CreateRequest req, OmsConfig.Fx.TierKills cfg) {
        if (req.tier() == null || req.tier().trim().isEmpty()) {
            throw new IllegalArgumentException("tier required");
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
    }

    /**
     * Approves a pending kill. The approver must be a different identity
     * from {@code created_by} — four-eyes. Re-approval and self-approval
     * are rejected with {@link IllegalStateException}.
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
                            + "FROM fx_pair_tier_kills WHERE id = ?", id);
        } catch (DataAccessException e) {
            throw new IllegalStateException("kill " + id + " not found");
        }
        if (row.get("revoked_at") != null) {
            throw new IllegalStateException("kill " + id + " is revoked");
        }
        if (row.get("approved_at") != null) {
            throw new IllegalStateException("kill " + id + " is already approved");
        }
        if (trimmed.equals(row.get("created_by"))) {
            throw new IllegalStateException("kill " + id + " cannot be self-approved (four-eyes)");
        }
        int n = jdbc.update(
                "UPDATE fx_pair_tier_kills "
                        + "SET approved_by = ?, approved_at = ? "
                        + "WHERE id = ? AND approved_at IS NULL AND revoked_at IS NULL",
                trimmed, Timestamp.from(clock.instant()), id);
        if (n == 0) throw new IllegalStateException("kill " + id + " not approvable");
        approveCounter.increment();
        refreshNow();
        changePropagator.localChanged("approve", id);
        log.info("[fx-tier-kills] approve id={} by={}", id, trimmed);
    }

    /** Revokes a kill immediately. Idempotent on already-revoked rows. */
    public void revoke(long id, String revokedBy) {
        if (revokedBy == null || revokedBy.trim().isEmpty()) {
            throw new IllegalArgumentException("revokedBy required");
        }
        int n = jdbc.update(
                "UPDATE fx_pair_tier_kills "
                        + "SET revoked_by = ?, revoked_at = ? "
                        + "WHERE id = ? AND revoked_at IS NULL",
                revokedBy.trim(), Timestamp.from(clock.instant()), id);
        if (n == 0) {
            throw new IllegalStateException("kill " + id + " not found or already revoked");
        }
        revokeCounter.increment();
        refreshNow();
        changePropagator.localChanged("revoke", id);
        log.info("[fx-tier-kills] revoke id={} by={}", id, revokedBy.trim());
    }

    /**
     * Read view for the trading-desk publisher panel. Returns active +
     * pending + scheduled + last 24h revoked rows with a derived
     * {@code status} string. Same shape as
     * {@link FxMarkupOverridesService#listAll} so the desk UI table
     * reuses the existing column set.
     */
    public List<Map<String, Object>> listAll() {
        Instant now = clock.instant();
        Instant since = now.minus(Duration.ofHours(24));
        String sql = "SELECT id, pair, tier, reason, "
                + "       valid_from, valid_until, created_by, created_at, "
                + "       approved_by, approved_at, revoked_by, revoked_at "
                + "FROM fx_pair_tier_kills "
                + "WHERE (revoked_at IS NULL AND valid_until > ?) "
                + "   OR (revoked_at IS NOT NULL AND revoked_at >= ?) "
                + "ORDER BY valid_until ASC";
        try {
            List<Map<String, Object>> raw = jdbc.query(sql, this::mapRow,
                    Timestamp.from(now), Timestamp.from(since));
            for (Map<String, Object> r : raw) {
                r.put("status", deriveStatus(r, now));
            }
            return raw;
        } catch (DataAccessException e) {
            log.warn("[fx-tier-kills] listAll failed", e);
            return List.of();
        }
    }

    private Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", rs.getLong("id"));
        r.put("pair", rs.getString("pair"));
        r.put("tier", rs.getString("tier"));
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
            String tier,
            Instant validFrom,
            long durationMs,
            String reason,
            String createdBy
    ) {}

    /** Outcome of {@link #create(CreateRequest)}. */
    public record CreateResult(long id, boolean autoApproved) {}

    /**
     * One row of the kill cache. {@code pair} is nullable to encode
     * wildcard scope; tier is always present (uppercase pair, lowercase
     * tier — canonical form matching {@link OmsFxCustomerQuotePublisher}).
     */
    public record KillRow(
            String pair,
            String tier,
            Instant validFrom,
            Instant validUntil
    ) {
        boolean matches(String pairUp, String tierLo, Instant now) {
            if (!matchesWindow(now)) return false;
            if (pair != null && !pair.equals(pairUp)) return false;
            return tier.equals(tierLo);
        }

        boolean matchesWindow(Instant now) {
            return !now.isBefore(validFrom) && now.isBefore(validUntil);
        }
    }

    /** Status payload for {@code /internal/v1/fx/customer-quote/status}. */
    public record KillsStatus(
            int cachedRowCount,
            int activeCount,
            long nextExpiryEpochMs,
            long cacheLoadedAtMs
    ) {}
}
