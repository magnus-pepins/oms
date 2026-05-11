package com.balh.oms.observability.metrics;

/**
 * Expected span bounds for Micrometer percentile histograms on pipeline timers (no bare literals at call sites).
 */
public final class OmsPipelineLatencyBounds {

    private OmsPipelineLatencyBounds() {}

    public static final long MICROMETER_MIN_EXPECTED_MS = 1L;

    public static final long MICROMETER_MAX_EXPECTED_MS = 120_000L;
}
