package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.FixRouteStateRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

/**
 * Optional start-of-day reconciliation: sets {@code fix_route_state.send_enabled = true} for all routes.
 * Disabled by default; enable with {@code oms.fix.route-state-sod-enabled=true} and set
 * {@code oms.fix.route-state-sod-cron} (see {@code application.yaml}). Intended for environments where
 * {@code oms.routing.backend=fix}; harmless on other backends but typically left off.
 */
@Component
@ConditionalOnProperty(name = "oms.fix.route-state-sod-enabled", havingValue = "true")
public class FixRouteStateSodScheduler {

    private static final Logger log = LoggerFactory.getLogger(FixRouteStateSodScheduler.class);

    private final FixRouteStateRepository fixRouteStateRepository;
    private final MeterRegistry meterRegistry;
    private final OmsConfig omsConfig;
    private final FixSodPolicyEngine sodPolicyEngine;

    public FixRouteStateSodScheduler(
            FixRouteStateRepository fixRouteStateRepository,
            MeterRegistry meterRegistry,
            OmsConfig omsConfig,
            FixSodPolicyEngine sodPolicyEngine) {
        this.fixRouteStateRepository = fixRouteStateRepository;
        this.meterRegistry = meterRegistry;
        this.omsConfig = omsConfig;
        this.sodPolicyEngine = sodPolicyEngine;
    }

    @Scheduled(cron = "${oms.fix.route-state-sod-cron}")
    public void reconcileRouteStateAtSod() {
        String cron = omsConfig.getFix().getRouteStateSodCron();
        if (cron == null || cron.isBlank()) {
            log.warn("oms.fix.route-state-sod-cron is blank; skipping FIX route-state SOD reconciliation");
            return;
        }
        Optional<String> skip = sodPolicyEngine.skipReasonAt(Instant.now());
        if (skip.isPresent()) {
            meterRegistry.counter(FixMetrics.METRIC_ROUTE_STATE_SOD_SKIPPED, FixMetrics.TAG_SKIP_REASON, skip.get())
                    .increment();
            log.info("FIX route-state SOD reconciliation skipped ({})", skip.get());
            return;
        }
        int n = fixRouteStateRepository.sodEnableSendOnAllRoutes("sod-reconciler");
        meterRegistry.counter(FixMetrics.METRIC_ROUTE_STATE_SOD_RECONCILIATIONS).increment();
        log.info("FIX route-state SOD reconciliation updated {} row(s)", n);
    }
}
