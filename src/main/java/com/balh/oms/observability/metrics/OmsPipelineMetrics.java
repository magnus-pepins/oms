package com.balh.oms.observability.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Centralised Micrometer recording for slice-1 pipeline latency (Prometheus via {@code /actuator/prometheus}).
 */
public final class OmsPipelineMetrics {

    private OmsPipelineMetrics() {}

    public static void finishIngressAccept(MeterRegistry registry, Timer.Sample sample, String outcome) {
        sample.stop(baseTimer(OmsPipelineMeterNames.INGRESS_ACCEPT)
                .description(
                        "Postgres transaction for internal order accept until commit (orders + control_outbox + domain_event_outbox)")
                .tag("outcome", outcome)
                .register(registry));
    }

    public static void recordOutboxToChronicleLag(MeterRegistry registry, Duration lag) {
        baseTimer(OmsPipelineMeterNames.CONTROL_OUTBOX_TO_CHRONICLE_LAG)
                .description("Wall time from control_outbox.enqueued_at until Chronicle append succeeded")
                .register(registry)
                .record(lag.compareTo(Duration.ZERO) < 0 ? Duration.ZERO : lag);
    }

    public static void finishChronicleAppend(MeterRegistry registry, Timer.Sample sample) {
        sample.stop(baseTimer(OmsPipelineMeterNames.CONTROL_CHRONICLE_APPEND)
                .description("Chronicle append plus control_outbox markAppended for one row")
                .register(registry));
    }

    public static void finishControlApply(MeterRegistry registry, Timer.Sample sample, String result) {
        sample.stop(baseTimer(OmsPipelineMeterNames.CONTROL_APPLY)
                .description("ControlTailer.apply: stale guard, risk, buying power, CAS, domain outbox")
                .tag("result", result)
                .register(registry));
    }

    public static void finishFixOutboundNos(MeterRegistry registry, Timer.Sample sample, String outcome) {
        sample.stop(baseTimer(OmsPipelineMeterNames.FIX_OUTBOUND_NOS)
                .description("FIX outbound worker: build NewOrderSingle and Session.sendToTarget")
                .tag("outcome", outcome)
                .register(registry));
    }

    /**
     * Records the same wall duration (ms) as the OTel ingress→NOS histogram when that sample exists.
     */
    public static void recordPipelineIngressToFixNos(MeterRegistry registry, double milliseconds) {
        Duration d = Duration.ofNanos(Math.round(milliseconds * 1_000_000d));
        baseTimer(OmsPipelineMeterNames.PIPELINE_INGRESS_TO_FIX_NOS)
                .description("Committed internal accept to successful FIX NewOrderSingle send (WORKING path)")
                .register(registry)
                .record(d);
    }

    private static Timer.Builder baseTimer(String name) {
        return Timer.builder(name)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(OmsPipelineLatencyBounds.MICROMETER_MIN_EXPECTED_MS))
                .maximumExpectedValue(Duration.ofMillis(OmsPipelineLatencyBounds.MICROMETER_MAX_EXPECTED_MS));
    }
}
