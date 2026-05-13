package com.balh.oms.observability.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Centralised Micrometer recording for the ingress accept timer (Prometheus via
 * {@code /actuator/prometheus}). The Phase-1 control-apply / fix-outbound / ingress-to-fix-nos
 * recorders were removed in slice 4i once their producers had been deleted across Phase 3.
 */
public final class OmsPipelineMetrics {

    private OmsPipelineMetrics() {}

    public static void finishIngressAccept(MeterRegistry registry, Timer.Sample sample, String outcome) {
        sample.stop(baseTimer(OmsPipelineMeterNames.INGRESS_ACCEPT)
                .description(
                        "Postgres transaction for internal order accept until commit (domain_event_outbox + optional ledger_inflight_outbox); orders writes moved to OmsPostgresProjector in slice 2c")
                .tag("outcome", outcome)
                .register(registry));
    }

    private static Timer.Builder baseTimer(String name) {
        return Timer.builder(name)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(OmsPipelineLatencyBounds.MICROMETER_MIN_EXPECTED_MS))
                .maximumExpectedValue(Duration.ofMillis(OmsPipelineLatencyBounds.MICROMETER_MAX_EXPECTED_MS));
    }
}
