package com.balh.oms.fix;

/**
 * Micrometer metric names for the FIX slice (per config-and-limits: no bare string literals at call sites).
 */
public final class FixMetrics {

    public static final String METRIC_NOS_SENT = "oms_fix_nos_sent_total";
    public static final String METRIC_INBOUND_ER = "oms_fix_inbound_execution_reports_total";
    public static final String TAG_DISPOSITION = "disposition";

    private FixMetrics() {
    }
}
