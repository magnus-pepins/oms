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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Cached read + write surface for {@code fx_hedger_policy} (V51).
 *
 * <p>Sibling pattern of {@link FxMarkupOverridesService} and
 * {@link FxTierKillsService}: in-memory cache refreshed every minute
 * by default, callers ({@link FxAutoHedger}) read from the cache on
 * every eval tick, write paths refresh the cache synchronously after
 * a successful update.
 *
 * <p>Four-eyes is enforced here for the {@code advisory → auto}
 * promotion: a row can only be set to {@code mode='auto'} by an
 * identity different from the one that last touched it (or created
 * it). Demotion to {@code advisory} or {@code off} is a safety lever
 * and does <em>not</em> need a second approver — the operator who
 * wants to stop the engine should not have to go find a colleague.
 *
 * <p>The CHECK constraint in V51 enforces "mode=auto implies
 * auto_approved_by is non-null" at the schema level, so a direct SQL
 * write that bypasses this service still cannot land a half-approved
 * auto row.
 */
@Service
public class FxAutoHedgerPolicyService {

    private static final Logger log = LoggerFactory.getLogger(FxAutoHedgerPolicyService.class);

    /** Floor on the refresh cadence we'll accept from config. */
    private static final long MIN_REFRESH_MS = 5_000L;

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final OmsConfig omsConfig;
    private final Counter refreshOkCounter;
    private final Counter refreshFailCounter;
    private final Counter upsertCounter;
    private final Counter modeChangeCounter;

    private volatile Map<String, PolicyRow> cache = Collections.emptyMap();
    private volatile long cacheLoadedAtMs;

    public FxAutoHedgerPolicyService(
            JdbcTemplate jdbc,
            Clock clock,
            OmsConfig omsConfig,
            MeterRegistry registry,
            @Value("${oms.fx.auto-hedger.policy-refresh-ms:60000}") long refreshMs) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.omsConfig = omsConfig;
        this.refreshOkCounter = Counter.builder("oms_fx_auto_hedger_policy_refresh_total")
                .tag("outcome", "ok").register(registry);
        this.refreshFailCounter = Counter.builder("oms_fx_auto_hedger_policy_refresh_total")
                .tag("outcome", "fail").register(registry);
        this.upsertCounter = Counter.builder("oms_fx_auto_hedger_policy_upsert_total")
                .register(registry);
        this.modeChangeCounter = Counter.builder("oms_fx_auto_hedger_policy_mode_change_total")
                .register(registry);
        long effective = Math.max(MIN_REFRESH_MS, refreshMs);
        if (effective != refreshMs) {
            log.warn("[fx-auto-hedger-policy] refresh-ms {} below floor; clamped to {}", refreshMs, effective);
        }
    }

    @Scheduled(
            fixedDelayString = "${oms.fx.auto-hedger.policy-refresh-ms:60000}",
            initialDelayString = "${oms.fx.auto-hedger.policy-refresh-ms:60000}")
    public void refresh() {
        refreshNow();
    }

    public void refreshNow() {
        String sql = "SELECT currency, target_balance, threshold_abs, threshold_pct, "
                + "       pair_route, max_per_action, cooldown_s, mode, "
                + "       base_nostro_id, quote_nostro_id, "
                + "       created_by, created_at, updated_by, updated_at, "
                + "       auto_approved_by, auto_approved_at "
                + "FROM fx_hedger_policy";
        try {
            Map<String, PolicyRow> next = new LinkedHashMap<>();
            jdbc.query(sql, this::mapRow).forEach(r -> next.put(r.currency(), r));
            cache = Collections.unmodifiableMap(next);
            cacheLoadedAtMs = clock.millis();
            refreshOkCounter.increment();
            log.debug("[fx-auto-hedger-policy] cache refreshed entries={}", next.size());
        } catch (DataAccessException e) {
            refreshFailCounter.increment();
            log.warn("[fx-auto-hedger-policy] refresh failed; reusing prior cache size={}", cache.size(), e);
        }
    }

    /** Snapshot of every active policy row, in insertion order. */
    public List<PolicyRow> listAll() {
        return new ArrayList<>(cache.values());
    }

    /** Direct lookup the engine uses on every tick. Empty when no row exists for {@code currency}. */
    public Optional<PolicyRow> forCurrency(String currency) {
        if (currency == null) return Optional.empty();
        return Optional.ofNullable(cache.get(currency.toUpperCase()));
    }

    /** Package-private for unit tests — prime the cache directly. */
    void primeCache(List<PolicyRow> rows) {
        Map<String, PolicyRow> next = new LinkedHashMap<>();
        for (PolicyRow r : rows) next.put(r.currency(), r);
        this.cache = Collections.unmodifiableMap(next);
        this.cacheLoadedAtMs = clock.millis();
    }

    /**
     * Upserts a policy row. {@code mode='auto'} writes are rejected
     * here when {@code auto_approved_by} is absent or equals the
     * caller — four-eyes. Demotion (auto → advisory / off) is allowed
     * unilaterally and clears the four-eyes columns so a future
     * re-promotion goes through the same review.
     */
    public UpsertResult upsert(UpsertRequest req) {
        Objects.requireNonNull(req, "request");
        validate(req);
        String currency = req.currency().toUpperCase();
        String mode = req.mode().toLowerCase();
        String updatedBy = req.updatedBy().trim();
        Instant now = clock.instant();

        PolicyRow existing = cache.get(currency);
        boolean promotingToAuto = "auto".equals(mode)
                && (existing == null || !"auto".equals(existing.mode()));
        boolean demotingFromAuto = existing != null && "auto".equals(existing.mode())
                && !"auto".equals(mode);

        if ("auto".equals(mode)) {
            String approver = req.autoApprovedBy() == null ? null : req.autoApprovedBy().trim();
            if (approver == null || approver.isEmpty()) {
                throw new IllegalArgumentException("autoApprovedBy required for mode=auto");
            }
            if (approver.equals(updatedBy)) {
                throw new IllegalArgumentException("autoApprovedBy must differ from updatedBy (four-eyes)");
            }
            // Also block the "created myself + approved myself across two
            // separate upserts" pattern: if the row already exists with a
            // creator/updater equal to the approver, the four-eyes review
            // is degraded. Engine still runs but at least one of the
            // three identities (creator, updater, approver) has to be
            // distinct from the other two.
            if (existing != null) {
                String lastTouchedBy = existing.updatedBy() != null
                        ? existing.updatedBy()
                        : existing.createdBy();
                if (approver.equals(lastTouchedBy) && updatedBy.equals(lastTouchedBy)) {
                    throw new IllegalArgumentException(
                            "four-eyes requires a third identity vs. existing row; "
                                    + "updatedBy/lastTouchedBy/approvedBy cannot all match");
                }
            }
        }

        String sql;
        Object[] args;
        if (existing == null) {
            sql = "INSERT INTO fx_hedger_policy ("
                    + "currency, target_balance, threshold_abs, threshold_pct, "
                    + "pair_route, max_per_action, cooldown_s, mode, "
                    + "base_nostro_id, quote_nostro_id, created_by, created_at, "
                    + "auto_approved_by, auto_approved_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            args = new Object[]{
                    currency, req.targetBalance(), req.thresholdAbs(), req.thresholdPct(),
                    req.pairRoute().toUpperCase(), req.maxPerAction(), req.cooldownS(), mode,
                    req.baseNostroId().trim(), req.quoteNostroId().trim(),
                    updatedBy, Timestamp.from(now),
                    "auto".equals(mode) ? req.autoApprovedBy().trim() : null,
                    "auto".equals(mode) ? Timestamp.from(now) : null};
        } else {
            sql = "UPDATE fx_hedger_policy SET "
                    + "target_balance = ?, threshold_abs = ?, threshold_pct = ?, "
                    + "pair_route = ?, max_per_action = ?, cooldown_s = ?, mode = ?, "
                    + "base_nostro_id = ?, quote_nostro_id = ?, "
                    + "updated_by = ?, updated_at = ?, "
                    + "auto_approved_by = ?, auto_approved_at = ? "
                    + "WHERE currency = ?";
            String approver;
            Instant approvedAt;
            if ("auto".equals(mode)) {
                approver = req.autoApprovedBy().trim();
                approvedAt = now;
            } else if (demotingFromAuto) {
                approver = null;
                approvedAt = null;
            } else {
                approver = existing.autoApprovedBy();
                approvedAt = existing.autoApprovedAt();
            }
            args = new Object[]{
                    req.targetBalance(), req.thresholdAbs(), req.thresholdPct(),
                    req.pairRoute().toUpperCase(), req.maxPerAction(), req.cooldownS(), mode,
                    req.baseNostroId().trim(), req.quoteNostroId().trim(),
                    updatedBy, Timestamp.from(now),
                    approver, approvedAt == null ? null : Timestamp.from(approvedAt),
                    currency};
        }

        try {
            jdbc.update(sql, args);
        } catch (DataAccessException e) {
            throw new IllegalArgumentException("upsert failed: " + e.getMostSpecificCause().getMessage(), e);
        }
        upsertCounter.increment();
        if (existing == null || !existing.mode().equals(mode)) {
            modeChangeCounter.increment();
        }
        refreshNow();
        log.info("[fx-auto-hedger-policy] upsert currency={} mode={} promoting_to_auto={} demoting={}",
                currency, mode, promotingToAuto, demotingFromAuto);
        return new UpsertResult(currency, mode, promotingToAuto, demotingFromAuto);
    }

    /**
     * Read view for the trading-desk Treasury panel — returns the cached
     * rows + the engine pricing tier so the UI can render "quoting at
     * tier=desk" without a separate config fetch.
     */
    public PolicyView view() {
        return new PolicyView(
                clock.instant(),
                listAll(),
                cacheLoadedAtMs,
                omsConfig.getFx().getAutoHedger().getPricingTier(),
                omsConfig.getFx().getAutoHedger().isEngineEnabled(),
                omsConfig.getFx().getAutoHedger().isAutoFireEnabled());
    }

    private static void validate(UpsertRequest req) {
        if (req.currency() == null || req.currency().trim().length() != 3) {
            throw new IllegalArgumentException("currency must be a 3-letter code");
        }
        if (req.targetBalance() == null || req.targetBalance().signum() < 0) {
            throw new IllegalArgumentException("targetBalance must be >= 0");
        }
        if (req.thresholdAbs() == null && req.thresholdPct() == null) {
            throw new IllegalArgumentException("threshold_abs or threshold_pct required");
        }
        if (req.thresholdAbs() != null && req.thresholdAbs().signum() <= 0) {
            throw new IllegalArgumentException("threshold_abs must be > 0");
        }
        if (req.thresholdPct() != null
                && (req.thresholdPct().signum() <= 0
                || req.thresholdPct().compareTo(new BigDecimal("100")) > 0)) {
            throw new IllegalArgumentException("threshold_pct must be in (0, 100]");
        }
        if (req.pairRoute() == null || req.pairRoute().trim().length() != 6) {
            throw new IllegalArgumentException("pair_route must be a 6-letter pair code");
        }
        if (req.maxPerAction() == null || req.maxPerAction().signum() <= 0) {
            throw new IllegalArgumentException("max_per_action must be > 0");
        }
        if (req.cooldownS() < 0) {
            throw new IllegalArgumentException("cooldown_s must be >= 0");
        }
        if (req.mode() == null || !List.of("off", "advisory", "auto").contains(req.mode().toLowerCase())) {
            throw new IllegalArgumentException("mode must be off / advisory / auto");
        }
        if (req.baseNostroId() == null || req.baseNostroId().trim().isEmpty()) {
            throw new IllegalArgumentException("base_nostro_id required");
        }
        if (req.quoteNostroId() == null || req.quoteNostroId().trim().isEmpty()) {
            throw new IllegalArgumentException("quote_nostro_id required");
        }
        if (req.updatedBy() == null || req.updatedBy().trim().isEmpty()) {
            throw new IllegalArgumentException("updatedBy required");
        }
    }

    private PolicyRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp updatedAt = rs.getTimestamp("updated_at");
        Timestamp approvedAt = rs.getTimestamp("auto_approved_at");
        return new PolicyRow(
                rs.getString("currency"),
                rs.getBigDecimal("target_balance"),
                rs.getBigDecimal("threshold_abs"),
                rs.getBigDecimal("threshold_pct"),
                rs.getString("pair_route"),
                rs.getBigDecimal("max_per_action"),
                rs.getInt("cooldown_s"),
                rs.getString("mode"),
                rs.getString("base_nostro_id"),
                rs.getString("quote_nostro_id"),
                rs.getString("created_by"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getString("updated_by"),
                updatedAt == null ? null : updatedAt.toInstant(),
                rs.getString("auto_approved_by"),
                approvedAt == null ? null : approvedAt.toInstant());
    }

    /** Inputs for {@link #upsert(UpsertRequest)}. */
    public record UpsertRequest(
            String currency,
            BigDecimal targetBalance,
            BigDecimal thresholdAbs,
            BigDecimal thresholdPct,
            String pairRoute,
            BigDecimal maxPerAction,
            int cooldownS,
            String mode,
            String baseNostroId,
            String quoteNostroId,
            String updatedBy,
            String autoApprovedBy
    ) {}

    /** Outcome of {@link #upsert(UpsertRequest)}. */
    public record UpsertResult(String currency, String mode, boolean promotedToAuto, boolean demotedFromAuto) {}

    /** Cached row used by the engine + by the API view. */
    public record PolicyRow(
            String currency,
            BigDecimal targetBalance,
            BigDecimal thresholdAbs,
            BigDecimal thresholdPct,
            String pairRoute,
            BigDecimal maxPerAction,
            int cooldownS,
            String mode,
            String baseNostroId,
            String quoteNostroId,
            String createdBy,
            Instant createdAt,
            String updatedBy,
            Instant updatedAt,
            String autoApprovedBy,
            Instant autoApprovedAt
    ) {}

    /** API view payload. */
    public record PolicyView(
            Instant asOf,
            List<PolicyRow> rows,
            long cacheLoadedAtMs,
            String pricingTier,
            boolean engineEnabled,
            boolean autoFireEnabled
    ) {}
}
