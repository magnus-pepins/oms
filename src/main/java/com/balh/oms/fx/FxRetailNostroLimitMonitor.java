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
 * retail FX conversion pools on the plain {@code @Nostro-<CCY>} balances.
 *
 * <p>Sibling of {@link FxSuspenseLimitMonitor} for the <em>retail</em> book:
 * the customer-app "move money" cross-currency path nets its open position
 * into these pools, so their signed balance is the retail FX exposure the
 * desk supervises and (via {@code exposure_source='retail'} auto-hedger
 * policies) hedges.
 *
 * <h2>Gauges</h2>
 * <ul>
 *   <li>{@code oms_fx_retail_nostro_balance{currency}} — signed available
 *       balance (negative = short that currency in the retail pool).</li>
 *   <li>{@code oms_fx_retail_nostro_abs_balance{currency}} — absolute value;
 *       drives the Prometheus over-limit alert.</li>
 *   <li>{@code oms_fx_retail_nostro_max_abs{currency}} — configured soft
 *       limit (only present when a limit is configured).</li>
 * </ul>
 *
 * <h2>Counter</h2>
 * <ul>
 *   <li>{@code oms_fx_retail_nostro_over_limit_total{currency}} — increments
 *       once per poll cycle where {@code |available| > limit}.</li>
 * </ul>
 *
 * <p>Off by default ({@code oms.fx.retail-nostro.limit-monitor-enabled=false}).
 * Enable on a single OMS role (typically the projector, same place
 * {@link FxAutoHedger} lives) so the gauge is published from one source.
 */
@Service
public class FxRetailNostroLimitMonitor {

    private static final Logger log = LoggerFactory.getLogger(FxRetailNostroLimitMonitor.class);

    private final OmsConfig omsConfig;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalanceClient;
    private final MeterRegistry registry;

    private final Map<String, AtomicReference<Double>> signedRefs = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> absRefs = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> limitRefs = new ConcurrentHashMap<>();
    private final Map<String, Counter> overLimitCounters = new ConcurrentHashMap<>();
    private final Counter pollOkCounter;
    private final Counter pollFailCounter;

    public FxRetailNostroLimitMonitor(
            OmsConfig omsConfig,
            ObjectProvider<LedgerBalanceClient> ledgerBalanceClient,
            MeterRegistry registry) {
        this.omsConfig = omsConfig;
        this.ledgerBalanceClient = ledgerBalanceClient;
        this.registry = registry;
        this.pollOkCounter = Counter.builder("oms_fx_retail_nostro_poll_total")
                .tag("outcome", "ok")
                .description("FX retail-nostro limit-monitor poll cycles by outcome")
                .register(registry);
        this.pollFailCounter = Counter.builder("oms_fx_retail_nostro_poll_total")
                .tag("outcome", "fail")
                .description("FX retail-nostro limit-monitor poll cycles by outcome")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${oms.fx.retail-nostro.limit-monitor-poll-interval-ms:30000}")
    public void poll() {
        var fx = omsConfig.getFx();
        var cfg = fx.getRetailNostro();
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
            String indicator = FxRetailNostroSnapshotService.RETAIL_NOSTRO_INDICATOR_PREFIX + ccy;
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
                                "[fx-retail-nostro] {} over limit: available={} |available|={} limit={}",
                                indicator, available.toPlainString(), abs.toPlainString(),
                                limit.toPlainString());
                    }
                }
            } catch (LedgerBalanceClient.LedgerServiceException e) {
                anyFailed = true;
                log.warn(
                        "[fx-retail-nostro] poll failed for {}: {}", indicator, e.getMessage());
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
                "oms_fx_retail_nostro_balance",
                "Signed available balance on the retail @Nostro-<CCY> pool (positive=long that currency).",
                c));
        signedRefs.get(ccy).set(value.doubleValue());
    }

    private void publishAbs(String ccy, BigDecimal abs) {
        absRefs.computeIfAbsent(ccy, c -> registerGauge(
                "oms_fx_retail_nostro_abs_balance",
                "Absolute available balance on the retail @Nostro-<CCY> pool; drives over-limit alert.",
                c));
        absRefs.get(ccy).set(abs.doubleValue());
    }

    private void publishLimit(String ccy, BigDecimal limit) {
        limitRefs.computeIfAbsent(ccy, c -> registerGauge(
                "oms_fx_retail_nostro_max_abs",
                "Configured per-currency |retail pool| soft limit (oms.fx.retail-nostro.max-abs-csv).",
                c));
        limitRefs.get(ccy).set(limit.doubleValue());
    }

    private Counter overLimitCounter(String ccy) {
        return overLimitCounters.computeIfAbsent(ccy, c -> Counter.builder("oms_fx_retail_nostro_over_limit_total")
                .tag("currency", c)
                .description("Poll cycles where |retail pool| exceeded the configured soft limit.")
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
