package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Scheduled drift evaluator for the auto-hedger (plan B1).
 *
 * <p>On each tick:
 * <ol>
 *   <li>Pull the current per-currency available balance via
 *       {@link FxNostroSnapshotService}. The snapshot already sums
 *       per nostro, so we aggregate by currency here.</li>
 *   <li>For each policy row, compute drift = current - target and
 *       compare to the policy threshold. Below threshold = no-op;
 *       still expose the drift on a Micrometer gauge so the UI / Grafana
 *       can show "we are within tolerance".</li>
 *   <li>Above threshold + cooldown elapsed: mint a fresh quote at the
 *       desk pricing tier, write a {@code fx_hedger_recommendations}
 *       row (idempotent on {@code action_key}), and — if the policy is
 *       in {@code auto} mode <em>and</em>
 *       {@code oms.fx.auto-hedger.auto-fire-enabled} is on — call
 *       {@link FxHedgeService#submit(FxHedgeService.HedgeRequest)} to
 *       fire the hedge. The submitter is the configured service-account
 *       string (default {@code "fx-auto-hedger"}) so audit filters can
 *       split manual vs automatic.</li>
 *   <li>Within-cooldown ticks above threshold update the drift gauge
 *       but write nothing.</li>
 * </ol>
 *
 * <p>The bean is always wired so the metrics registry sees the gauges,
 * but the loop body short-circuits when the engine is disabled in
 * {@code OmsConfig.Fx.AutoHedger.engineEnabled}.
 */
@Service
public class FxAutoHedger {

    private static final Logger log = LoggerFactory.getLogger(FxAutoHedger.class);
    private static final int AMOUNT_SCALE = 8;

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final OmsConfig omsConfig;
    private final FxAutoHedgerPolicyService policyService;
    private final FxNostroSnapshotService nostroSnapshot;
    private final FxQuoteService quoteService;
    private final ObjectProvider<FxHedgeService> hedgeService;
    private final ObjectMapper objectMapper;
    private final MeterRegistry registry;

    private final Counter ticksCounter;
    private final Counter recommendationsCounter;
    private final Counter autoFiresCounter;
    private final Counter autoFireFailuresCounter;
    private final Counter quoteFailuresCounter;
    private final Counter belowThresholdCounter;
    private final Counter cooldownSkipCounter;
    private final Counter engineDisabledTickCounter;
    /**
     * Engine status gauges. Lift the two config flags onto the metrics
     * surface so Grafana can render an on/off pill without hitting the
     * controller. Useful for the {@code FxAutoHedgerStalled} alert to
     * cross-reference with operator intent ("engine was supposed to be
     * on") without pulling config.
     */
    private final AtomicReference<Double> engineEnabledGauge = new AtomicReference<>(0.0);
    private final AtomicReference<Double> autoFireEnabledGauge = new AtomicReference<>(0.0);

    /** Per-currency drift gauge value (current - target). Updated every tick, even when engine is disabled. */
    private final Map<String, AtomicReference<Double>> driftValues = new ConcurrentHashMap<>();
    /** Per-currency latest-balance gauge value. */
    private final Map<String, AtomicReference<Double>> balanceValues = new ConcurrentHashMap<>();
    /**
     * Per-currency "capped" counter — incremented when the recommended
     * base amount was reduced by {@code max_per_action} because drift
     * exceeded the per-action cap. Sustained increments on one currency
     * mean the policy's cap is too low to ever close the gap; operator
     * must widen {@code max_per_action} or accept the standing exposure.
     * Drives the {@code FxAutoHedgerCurrencyAtPolicyMax} alert.
     */
    private final Map<String, Counter> cappedCounters = new ConcurrentHashMap<>();
    /** Last-fire epoch ms per currency to enforce cooldown. */
    private final Map<String, Long> lastFireMs = new ConcurrentHashMap<>();
    /** Last tick result for diagnostics endpoint. */
    private final AtomicReference<TickStatus> lastTick = new AtomicReference<>();

    public FxAutoHedger(
            JdbcTemplate jdbc,
            Clock clock,
            OmsConfig omsConfig,
            FxAutoHedgerPolicyService policyService,
            FxNostroSnapshotService nostroSnapshot,
            FxQuoteService quoteService,
            ObjectProvider<FxHedgeService> hedgeService,
            ObjectMapper objectMapper,
            MeterRegistry registry) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.omsConfig = omsConfig;
        this.policyService = policyService;
        this.nostroSnapshot = nostroSnapshot;
        this.quoteService = quoteService;
        this.hedgeService = hedgeService;
        this.objectMapper = objectMapper;
        this.registry = registry;
        this.ticksCounter = Counter.builder("oms_fx_auto_hedger_tick_total").register(registry);
        this.recommendationsCounter = Counter.builder("oms_fx_auto_hedger_recommendation_total")
                .register(registry);
        this.autoFiresCounter = Counter.builder("oms_fx_auto_hedger_auto_fire_total")
                .tag("outcome", "ok").register(registry);
        this.autoFireFailuresCounter = Counter.builder("oms_fx_auto_hedger_auto_fire_total")
                .tag("outcome", "fail").register(registry);
        this.quoteFailuresCounter = Counter.builder("oms_fx_auto_hedger_quote_fail_total")
                .register(registry);
        this.belowThresholdCounter = Counter.builder("oms_fx_auto_hedger_below_threshold_total")
                .register(registry);
        this.cooldownSkipCounter = Counter.builder("oms_fx_auto_hedger_cooldown_skip_total")
                .register(registry);
        this.engineDisabledTickCounter = Counter.builder("oms_fx_auto_hedger_engine_disabled_tick_total")
                .register(registry);
        Gauge.builder("oms_fx_auto_hedger_engine_enabled", engineEnabledGauge, AtomicReference::get)
                .description("1 when oms.fx.auto-hedger.engine-enabled is true on this JVM, 0 otherwise")
                .register(registry);
        Gauge.builder("oms_fx_auto_hedger_auto_fire_enabled", autoFireEnabledGauge, AtomicReference::get)
                .description("1 when the global auto-fire kill-switch is armed, 0 otherwise")
                .register(registry);
    }

    @Scheduled(
            fixedDelayString = "${oms.fx.auto-hedger.eval-interval-ms:15000}",
            initialDelayString = "${oms.fx.auto-hedger.eval-interval-ms:15000}")
    public void tick() {
        try {
            tickInternal();
        } catch (RuntimeException e) {
            log.error("[fx-auto-hedger] tick failed", e);
            lastTick.set(new TickStatus(clock.instant(), false, e.getMessage(), List.of()));
        }
    }

    /** Package-private for unit tests so they can drive a deterministic tick without sleeping on the scheduler. */
    TickStatus tickInternal() {
        ticksCounter.increment();
        OmsConfig.Fx.AutoHedger cfg = omsConfig.getFx().getAutoHedger();
        engineEnabledGauge.set(cfg.isEngineEnabled() ? 1.0 : 0.0);
        autoFireEnabledGauge.set(cfg.isAutoFireEnabled() ? 1.0 : 0.0);
        if (!omsConfig.getFx().isModuleEnabled()) {
            engineDisabledTickCounter.increment();
            TickStatus s = new TickStatus(clock.instant(), false, "fx_module_disabled", List.of());
            lastTick.set(s);
            return s;
        }
        if (!cfg.isEngineEnabled()) {
            engineDisabledTickCounter.increment();
            // Still emit drift telemetry — operators want to see numbers
            // even when the engine is "off" so the eventual flip to
            // advisory isn't a leap of faith.
            Map<String, BigDecimal> balances = readBalances();
            for (FxAutoHedgerPolicyService.PolicyRow r : policyService.listAll()) {
                BigDecimal bal = balances.getOrDefault(r.currency(), BigDecimal.ZERO);
                updateGauges(r.currency(), bal, r.targetBalance());
            }
            TickStatus s = new TickStatus(clock.instant(), false, "engine_disabled", List.of());
            lastTick.set(s);
            return s;
        }

        Map<String, BigDecimal> balances = readBalances();
        List<TickResult> results = new ArrayList<>();
        for (FxAutoHedgerPolicyService.PolicyRow r : policyService.listAll()) {
            try {
                results.add(evaluateOne(r, balances, cfg));
            } catch (RuntimeException e) {
                log.warn("[fx-auto-hedger] policy currency={} eval failed: {}", r.currency(), e.getMessage(), e);
                results.add(new TickResult(r.currency(), "error", null, null, e.getMessage()));
            }
        }
        TickStatus s = new TickStatus(clock.instant(), true, null, results);
        lastTick.set(s);
        return s;
    }

    private TickResult evaluateOne(
            FxAutoHedgerPolicyService.PolicyRow r,
            Map<String, BigDecimal> balances,
            OmsConfig.Fx.AutoHedger cfg) {
        BigDecimal current = balances.getOrDefault(r.currency(), BigDecimal.ZERO);
        BigDecimal drift = current.subtract(r.targetBalance()).setScale(AMOUNT_SCALE, RoundingMode.HALF_UP);
        BigDecimal absDrift = drift.abs();
        updateGauges(r.currency(), current, r.targetBalance());

        BigDecimal threshold = effectiveThreshold(r);
        if (absDrift.compareTo(threshold) < 0) {
            belowThresholdCounter.increment();
            return new TickResult(r.currency(), "below_threshold", drift, null, null);
        }
        if ("off".equals(r.mode())) {
            // Engine is on, policy is off: just keep emitting drift telemetry.
            return new TickResult(r.currency(), "policy_off", drift, null, null);
        }

        Instant now = clock.instant();
        Long last = lastFireMs.get(r.currency());
        if (last != null && now.toEpochMilli() - last < (long) r.cooldownS() * 1000L) {
            cooldownSkipCounter.increment();
            return new TickResult(r.currency(), "cooldown", drift, null, null);
        }

        // Side derived from currency position in the pair_route.
        String pair = r.pairRoute().toUpperCase();
        String baseCcy = pair.substring(0, 3);
        String quoteCcy = pair.substring(3);
        String side;
        BigDecimal baseAmount;
        if (r.currency().equals(baseCcy)) {
            // currency is the base — excess base means SELL pair (give up base for quote).
            side = drift.signum() > 0 ? "SELL" : "BUY";
            baseAmount = absDrift;
        } else if (r.currency().equals(quoteCcy)) {
            // currency is the quote — excess quote means BUY pair (give up quote for base).
            // baseAmount in pair's base currency = |drift_quote| / mid_rate.
            BigDecimal mid = midRateOrNull(pair);
            if (mid == null || mid.signum() <= 0) {
                quoteFailuresCounter.increment();
                return new TickResult(r.currency(), "no_mid_for_quote_side", drift, null,
                        "cannot size base_amount without mid rate for " + pair);
            }
            side = drift.signum() > 0 ? "BUY" : "SELL";
            baseAmount = absDrift.divide(mid, AMOUNT_SCALE, RoundingMode.HALF_UP);
        } else {
            return new TickResult(r.currency(), "currency_not_in_pair", drift, null,
                    r.currency() + " not in pair_route " + pair);
        }

        // Defence vs runaway: cap per-action notional in the pair's base currency.
        BigDecimal capInBase;
        if (r.currency().equals(baseCcy)) {
            capInBase = r.maxPerAction();
        } else {
            BigDecimal mid = midRateOrNull(pair);
            capInBase = (mid != null && mid.signum() > 0)
                    ? r.maxPerAction().divide(mid, AMOUNT_SCALE, RoundingMode.HALF_UP)
                    : null;
        }
        if (capInBase != null && baseAmount.compareTo(capInBase) > 0) {
            baseAmount = capInBase;
            cappedCounters.computeIfAbsent(r.currency(), this::newCappedCounter).increment();
        }

        // Mint a fresh quote at the desk pricing tier. We do this even
        // for auto-fire so the recommendation row carries the exact
        // price we used — operator can trace "engine fired @ 1.0875"
        // back to the same quote_id.
        Map<String, Object> quote;
        try {
            quote = quoteService.quote(pair, cfg.getPricingTier());
        } catch (RuntimeException e) {
            quoteFailuresCounter.increment();
            return new TickResult(r.currency(), "quote_failed", drift, null, e.getMessage());
        }
        BigDecimal rate = new BigDecimal((String) quote.get("BUY".equals(side) ? "ask" : "bid"));
        String quoteId = (String) quote.get("quoteId");

        // Idempotency: action_key = currency + tick second + drift-sign.
        // Same currency within one tick second cannot create two rows;
        // the unique index in V52 backstops it.
        String actionKey = "fx-auto-" + r.currency() + "-" + now.getEpochSecond() + "-"
                + (drift.signum() > 0 ? "excess" : "shortfall");
        long ttlMs = (long) r.cooldownS() * 2L * 1000L;
        Instant expiresAt = now.plus(Math.max(ttlMs, 1_000L), ChronoUnit.MILLIS);
        boolean recCreated = insertRecommendation(
                actionKey, r.currency(), pair, side, baseAmount, rate, quoteId,
                drift, r.targetBalance(), current, r.mode(), now, expiresAt);
        if (!recCreated) {
            return new TickResult(r.currency(), "duplicate_action_key", drift, actionKey, null);
        }
        recommendationsCounter.increment();
        lastFireMs.put(r.currency(), now.toEpochMilli());

        // Auto-fire path: gated by the global flag AND the per-policy mode.
        if ("auto".equals(r.mode()) && cfg.isAutoFireEnabled()) {
            FxHedgeService svc = hedgeService.getIfAvailable();
            if (svc == null) {
                autoFireFailuresCounter.increment();
                log.warn("[fx-auto-hedger] auto-mode but FxHedgeService unavailable; recommendation stays unfired actionKey={}",
                        actionKey);
                return new TickResult(r.currency(), "auto_unavailable", drift, actionKey, "hedge_service_unavailable");
            }
            try {
                FxHedgeService.HedgeRequest req = new FxHedgeService.HedgeRequest(
                        actionKey,
                        cfg.getAutoFireSubmitter(),
                        pair,
                        side,
                        cfg.getPricingTier(),
                        quoteId,
                        baseAmount,
                        r.baseNostroId(),
                        r.quoteNostroId(),
                        "auto-hedge drift=" + drift.toPlainString() + " " + r.currency());
                Map<String, Object> hedgeRow = svc.submit(req);
                Object hedgeIdObj = hedgeRow.get("id");
                if (hedgeIdObj instanceof Number n) {
                    linkAutoFire(actionKey, n.longValue());
                }
                autoFiresCounter.increment();
                return new TickResult(r.currency(), "auto_fired", drift, actionKey, null);
            } catch (RuntimeException e) {
                autoFireFailuresCounter.increment();
                log.warn("[fx-auto-hedger] auto-fire failed actionKey={} reason={}", actionKey, e.getMessage(), e);
                return new TickResult(r.currency(), "auto_fire_failed", drift, actionKey, e.getMessage());
            }
        }
        return new TickResult(r.currency(),
                "auto".equals(r.mode()) ? "auto_inhibited_by_kill_switch" : "advisory_emitted",
                drift, actionKey, null);
    }

    private BigDecimal effectiveThreshold(FxAutoHedgerPolicyService.PolicyRow r) {
        BigDecimal abs = r.thresholdAbs();
        BigDecimal pct = r.thresholdPct();
        BigDecimal byPct = null;
        if (pct != null) {
            byPct = r.targetBalance().multiply(pct).divide(new BigDecimal("100"), AMOUNT_SCALE, RoundingMode.HALF_UP);
        }
        if (abs != null && byPct != null) return abs.max(byPct);
        if (abs != null) return abs;
        if (byPct != null) return byPct;
        // Schema CHECK already prevents both null; defensive fallback.
        return BigDecimal.ZERO;
    }

    private Map<String, BigDecimal> readBalances() {
        Map<String, BigDecimal> out = new HashMap<>();
        try {
            Object payload = nostroSnapshot.buildSnapshot();
            if (!(payload instanceof Map<?, ?> m)) return out;
            Object rows = m.get("balances");
            if (!(rows instanceof List<?> list)) return out;
            for (Object row : list) {
                if (!(row instanceof Map<?, ?> rm)) continue;
                Object ccy = rm.get("currency");
                Object bal = rm.get("availableBalance");
                if (ccy == null || bal == null) continue;
                BigDecimal v;
                try {
                    v = new BigDecimal(bal.toString());
                } catch (NumberFormatException e) {
                    continue;
                }
                out.merge(ccy.toString().toUpperCase(), v, BigDecimal::add);
            }
        } catch (RuntimeException e) {
            log.warn("[fx-auto-hedger] nostro snapshot failed: {}", e.getMessage());
        }
        return out;
    }

    private BigDecimal midRateOrNull(String pair) {
        try {
            Map<String, Object> q = quoteService.quote(pair, omsConfig.getFx().getAutoHedger().getPricingTier());
            Object mid = q.get("mid");
            return mid == null ? null : new BigDecimal(mid.toString());
        } catch (RuntimeException e) {
            return null;
        }
    }

    private boolean insertRecommendation(
            String actionKey, String currency, String pair, String side,
            BigDecimal baseAmount, BigDecimal rate, String quoteId,
            BigDecimal drift, BigDecimal target, BigDecimal current,
            String mode, Instant createdAt, Instant expiresAt) {
        try {
            int n = jdbc.update(
                    "INSERT INTO fx_hedger_recommendations ("
                            + "action_key, currency, pair, side, base_amount, quoted_rate, quote_id, "
                            + "drift, target_balance, current_balance, mode, created_at, expires_at) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    actionKey, currency, pair, side,
                    baseAmount.setScale(AMOUNT_SCALE, RoundingMode.HALF_UP),
                    rate, quoteId,
                    drift, target, current,
                    "auto".equals(mode) ? "auto" : "advisory",
                    Timestamp.from(createdAt), Timestamp.from(expiresAt));
            return n > 0;
        } catch (DuplicateKeyException e) {
            return false;
        } catch (DataAccessException e) {
            log.warn("[fx-auto-hedger] insert recommendation failed actionKey={}: {}", actionKey, e.getMessage());
            return false;
        }
    }

    private void linkAutoFire(String actionKey, long hedgeActionId) {
        try {
            jdbc.update(
                    "UPDATE fx_hedger_recommendations SET auto_fired_action_id = ? WHERE action_key = ?",
                    hedgeActionId, actionKey);
        } catch (DataAccessException e) {
            log.warn("[fx-auto-hedger] link auto-fire failed actionKey={}: {}", actionKey, e.getMessage());
        }
    }

    private Counter newCappedCounter(String currency) {
        return Counter.builder("oms_fx_auto_hedger_recommendation_capped_total")
                .tag("currency", currency)
                .description("Recommendations where base_amount was reduced by max_per_action")
                .register(registry);
    }

    private void updateGauges(String currency, BigDecimal current, BigDecimal target) {
        AtomicReference<Double> driftRef = driftValues.computeIfAbsent(currency, k -> {
            AtomicReference<Double> ref = new AtomicReference<>(0.0);
            Gauge.builder("oms_fx_auto_hedger_drift", ref, AtomicReference::get)
                    .tags(Tags.of("currency", currency))
                    .description("Auto-hedger drift (current - target) per currency")
                    .register(registry);
            return ref;
        });
        AtomicReference<Double> balRef = balanceValues.computeIfAbsent(currency, k -> {
            AtomicReference<Double> ref = new AtomicReference<>(0.0);
            Gauge.builder("oms_fx_auto_hedger_balance", ref, AtomicReference::get)
                    .tags(Tags.of("currency", currency))
                    .description("Auto-hedger observed available balance per currency")
                    .register(registry);
            return ref;
        });
        driftRef.set(current.subtract(target).doubleValue());
        balRef.set(current.doubleValue());
    }

    /** Diagnostics endpoint payload. */
    public TickStatus lastTickStatus() {
        TickStatus s = lastTick.get();
        return s != null ? s : new TickStatus(clock.instant(), false, "no_tick_yet", List.of());
    }

    /** Lists active (un-acted-on, un-expired) recommendation rows for the UI. */
    public List<Map<String, Object>> activeRecommendations(int limit) {
        int lim = Math.max(1, Math.min(200, limit));
        return jdbc.query(
                "SELECT id, action_key, currency, pair, side, base_amount, quoted_rate, quote_id, "
                        + "drift, target_balance, current_balance, mode, created_at, expires_at, "
                        + "dismissed_by, dismissed_at, executed_action_id, auto_fired_action_id "
                        + "FROM fx_hedger_recommendations "
                        + "WHERE expires_at > NOW() AND dismissed_at IS NULL "
                        + "  AND executed_action_id IS NULL AND auto_fired_action_id IS NULL "
                        + "ORDER BY created_at DESC LIMIT ?",
                (rs, n) -> recRowToMap(rs),
                lim);
    }

    /** Lists recent recommendation rows including terminal states for audit. */
    public List<Map<String, Object>> recentRecommendations(int limit) {
        int lim = Math.max(1, Math.min(500, limit));
        return jdbc.query(
                "SELECT id, action_key, currency, pair, side, base_amount, quoted_rate, quote_id, "
                        + "drift, target_balance, current_balance, mode, created_at, expires_at, "
                        + "dismissed_by, dismissed_at, executed_action_id, auto_fired_action_id "
                        + "FROM fx_hedger_recommendations ORDER BY created_at DESC LIMIT ?",
                (rs, n) -> recRowToMap(rs),
                lim);
    }

    /** Marks a recommendation as dismissed. Returns the row or null when not found / already terminal. */
    public Map<String, Object> dismiss(long id, String dismissedBy) {
        if (dismissedBy == null || dismissedBy.isBlank()) {
            throw new IllegalArgumentException("dismissedBy required");
        }
        int n = jdbc.update(
                "UPDATE fx_hedger_recommendations "
                        + "SET dismissed_by = ?, dismissed_at = ? "
                        + "WHERE id = ? AND dismissed_at IS NULL "
                        + "  AND executed_action_id IS NULL AND auto_fired_action_id IS NULL",
                dismissedBy.trim(), Timestamp.from(clock.instant()), id);
        if (n == 0) return null;
        return jdbc.query(
                "SELECT id, action_key, currency, pair, side, base_amount, quoted_rate, quote_id, "
                        + "drift, target_balance, current_balance, mode, created_at, expires_at, "
                        + "dismissed_by, dismissed_at, executed_action_id, auto_fired_action_id "
                        + "FROM fx_hedger_recommendations WHERE id = ?",
                rs -> rs.next() ? recRowToMap(rs) : null,
                id);
    }

    private Map<String, Object> recRowToMap(java.sql.ResultSet rs) throws java.sql.SQLException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", rs.getLong("id"));
        m.put("actionKey", rs.getString("action_key"));
        m.put("currency", rs.getString("currency"));
        m.put("pair", rs.getString("pair"));
        m.put("side", rs.getString("side"));
        m.put("baseAmount", rs.getBigDecimal("base_amount").toPlainString());
        m.put("quotedRate", rs.getBigDecimal("quoted_rate").toPlainString());
        m.put("quoteId", rs.getString("quote_id"));
        m.put("drift", rs.getBigDecimal("drift").toPlainString());
        m.put("targetBalance", rs.getBigDecimal("target_balance").toPlainString());
        m.put("currentBalance", rs.getBigDecimal("current_balance").toPlainString());
        m.put("mode", rs.getString("mode"));
        m.put("createdAt", rs.getTimestamp("created_at").toInstant().toString());
        m.put("expiresAt", rs.getTimestamp("expires_at").toInstant().toString());
        Timestamp d = rs.getTimestamp("dismissed_at");
        m.put("dismissedAt", d == null ? null : d.toInstant().toString());
        m.put("dismissedBy", rs.getString("dismissed_by"));
        long exec = rs.getLong("executed_action_id");
        m.put("executedActionId", rs.wasNull() ? null : exec);
        long fired = rs.getLong("auto_fired_action_id");
        m.put("autoFiredActionId", rs.wasNull() ? null : fired);
        return m;
    }

    public record TickResult(String currency, String outcome, BigDecimal drift, String actionKey, String detail) {}

    public record TickStatus(Instant asOf, boolean engineRan, String reason, List<TickResult> results) {}
}
