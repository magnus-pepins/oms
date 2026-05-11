package com.balh.oms.observability.otel;

import java.util.List;

/**
 * Named bounds for {@link IngressToFixNosLatencyRecorder} (ecosystem config-and-limits: no bare literals at call sites).
 */
public final class IngressToFixNosLatencyLimits {

    private IngressToFixNosLatencyLimits() {}

    /** OpenTelemetry instrument name (Prometheus scrape maps dots to underscores). */
    public static final String OTEL_HISTOGRAM_NAME = "oms.fix.ingress_to_nos";

    public static final String OTEL_DISCARDED_COUNTER_NAME = "oms.fix.ingress_to_nos.samples_discarded";

    public static final String ATTR_REASON = "reason";

    public static final String REASON_CONTROL_REJECT = "control_reject";

    public static final String REASON_OUTBOUND_EXPIRED = "outbound_expired";

    public static final String REASON_QUEUE_FULL = "queue_full";

    public static final String REASON_TTL_EVICT = "ttl_evict";

    /** Upper bound latency buckets in milliseconds (classic histogram → {@code histogram_quantile} in Prometheus). */
    public static final List<Double> INGRESS_TO_NOS_HISTOGRAM_BOUNDARIES_MS = List.of(
            0.5d,
            1d,
            2d,
            5d,
            10d,
            25d,
            50d,
            100d,
            250d,
            500d,
            1_000d,
            2_500d,
            5_000d,
            10_000d,
            30_000d,
            120_000d);
}
