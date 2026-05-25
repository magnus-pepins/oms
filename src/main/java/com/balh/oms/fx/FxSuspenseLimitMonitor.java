package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerBalanceClient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Periodic poller that publishes Micrometer gauges and counters for the
 * {@code @FX-Suspense-<CCY>} balances.
 *
 * <h2>Gauges</h2>
 * <ul>
 *   <li>{@code oms_fx_suspense_balance{currency}} — signed available balance
 *       (negative = short the currency in suspense).</li>
 *   <li>{@code oms_fx_suspense_abs_balance{currency}} — absolute value;
 *       drives the Prometheus over-limit alert.</li>
 *   <li>{@code oms_fx_suspense_max_abs{currency}} — configured soft limit
 *       (only present when a limit is configured).</li>
 * </ul>
 *
 * <h2>Counter</h2>
 * <ul>
 *   <li>{@code oms_fx_suspense_over_limit_total{currency}} — increments
 *       once per poll cycle where {@code |available| > limit}. Wired so
 *       a rate over a baseline window catches sustained breaches without
 *       a single-tick blip flapping the alert.</li>
 * </ul>
 *
 * <p>Off by default ({@code oms.fx.suspense.limit-monitor-enabled=false}).
 * Enable on a single OMS role (typically the projector, same place
 * {@link FxAutoHedger} lives) so the gauge is published from one source.
 */
@Service
public class FxSuspenseLimitMonitor {

    private static final Logger log = LoggerFactory.getLogger(FxSuspenseLimitMonitor.class);
    private static final String SUSPENSE_INDICATOR_PREFIX = "@FX-Suspense-";

    private final OmsConfig omsConfig;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalanceClient;
    private final MeterRegistry registry;

    private final Map<String, AtomicReference<Double>> signedRefs = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> absRefs = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> limitRefs = new ConcurrentHashMap<>();
    private final Map<String, Counter> overLimitCounters = new ConcurrentHashMap<>();
    private final Counter pollOkCounter;
    private final Counter pollFailCounter;

    public FxSuspenseLimitMonitor(
            OmsConfig omsConfig,
            ObjectProvider<LedgerBalanceClient> ledgerBalanceClient,
            MeterRegistry registry) {
        this.omsConfig = omsConfig;
        this.ledgerBalanceClient = ledgerBalanceClient;
        this.registry = registry;
        this.pollOkCounter = Counter.builder("oms_fx_suspense_poll_total")
                .tag("outcome", "ok")
                .description("FX suspense limit-monitor poll cycles by outcome")
                .register(registry);
        this.pollFailCounter = Counter.builder("oms_fx_suspense_poll_total")
                .tag("outcome", "fail")
                .description("FX suspense limit-monitor poll cycles by outcome")
                .register(registry);
    }

    /**
     * Fixed-delay poll. Cadence is the configured
     * {@code oms.fx.suspense.limit-monitor-poll-interval-ms} (clamped to
     * ≥10s in {@link OmsConfig.Fx.Suspense#setLimitMonitorPollIntervalMs}).
     * Reads through {@code @Scheduled} on the bean so Spring picks up
     * config at boot; we don't restart the loop when config changes
     * (operator restart applies new cadence — same pattern as the
     * auto-hedger eval loop).
     */
    @Scheduled(fixedDelayString = "${oms.fx.suspense.limit-monitor-poll-interval-ms:30000}")
    public void poll() {
        var fx = omsConfig.getFx();
        var cfg = fx.getSuspense();
        if (!fx.isModuleEnabled() || !cfg.isLimitMonitorEnabled()) {
            return;
        }
        var currencies = cfg.currencies();
        if (currencies.isEmpty()) {
            return;
        }
        LedgerBalanceClient client = ledgerBalanceClient.getIfAvailable();
        if (client == null) {
            pollFailCounter.increment();
            return;
        }
        Map<String, BigDecimal> limits = cfg.maxAbsByCurrency();
        boolean anyFailed = false;
        for (String ccy : currencies) {
            String indicator = SUSPENSE_INDICATOR_PREFIX + ccy;
            try {
                BigDecimal available = client.fetchAvailableBalanceByIndicator(indicator, ccy);
                publishSigned(ccy, available);
                BigDecimal abs = available.abs();
                publishAbs(ccy, abs);
                BigDecimal limit = limits.get(ccy);
                if (limit != null) {
                    publishLimit(ccy, limit);
                    if (abs.compareTo(limit) > 0) {
                        overLimitCounter(ccy).increment();
                        log.warn(
                                "[fx-suspense] {} over limit: available={} |available|={} limit={}",
                                indicator, available.toPlainString(), abs.toPlainString(),
                                limit.toPlainString());
                    }
                }
            } catch (LedgerBalanceClient.LedgerServiceException e) {
                anyFailed = true;
                log.warn(
                        "[fx-suspense] poll failed for {}: {}", indicator, e.getMessage());
            }
        }
        if (anyFailed) {
            pollFailCounter.increment();
        } else {
            pollOkCounter.increment();
        }
    }

    private void publishSigned(String ccy, BigDecimal value) {
        signedRefs.computeIfAbsent(ccy, c -> registerGauge(
                "oms_fx_suspense_balance",
                "Signed available balance on @FX-Suspense-<CCY> (positive=long that currency in suspense).",
                c));
        signedRefs.get(ccy).set(value.doubleValue());
    }

    private void publishAbs(String ccy, BigDecimal abs) {
        absRefs.computeIfAbsent(ccy, c -> registerGauge(
                "oms_fx_suspense_abs_balance",
                "Absolute available balance on @FX-Suspense-<CCY>; drives over-limit alert.",
                c));
        absRefs.get(ccy).set(abs.doubleValue());
    }

    private void publishLimit(String ccy, BigDecimal limit) {
        limitRefs.computeIfAbsent(ccy, c -> registerGauge(
                "oms_fx_suspense_max_abs",
                "Configured per-currency |suspense| soft limit (oms.fx.suspense.max-abs-csv).",
                c));
        limitRefs.get(ccy).set(limit.doubleValue());
    }

    private Counter overLimitCounter(String ccy) {
        return overLimitCounters.computeIfAbsent(ccy, c -> Counter.builder("oms_fx_suspense_over_limit_total")
                .tag("currency", c)
                .description("Poll cycles where |suspense| exceeded the configured soft limit.")
                .register(registry));
    }

    private AtomicReference<Double> registerGauge(String name, String description, String currency) {
        AtomicReference<Double> ref = new AtomicReference<>(0.0);
        Gauge.builder(name, ref, AtomicReference::get)
                .description(description)
                .tags(Tags.of("currency", currency))
                .register(registry);
        return ref;
    }
}
