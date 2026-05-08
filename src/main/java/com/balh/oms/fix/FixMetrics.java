package com.balh.oms.fix;

/**
 * Micrometer metric names for the FIX slice (per config-and-limits: no bare string literals at call sites).
 */
public final class FixMetrics {

    public static final String METRIC_NOS_SENT = "oms_fix_nos_sent_total";
    public static final String METRIC_INBOUND_ER = "oms_fix_inbound_execution_reports_total";
    public static final String TAG_DISPOSITION = "disposition";

    public static final String METRIC_OUTBOUND_JOB_EXPIRED = "oms_fix_outbound_job_expired_total";
    public static final String METRIC_OUTBOUND_ROUTE_DISABLED_SKIPS = "oms_fix_outbound_route_disabled_skips_total";
    public static final String METRIC_OUTBOUND_THROTTLED_REQUEUES = "oms_fix_outbound_throttled_requeues_total";
    public static final String METRIC_ROUTE_STATE_SOD_RECONCILIATIONS = "oms_fix_route_state_sod_reconciliations_total";

    private FixMetrics() {
    }
}
