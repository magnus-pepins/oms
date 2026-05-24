package com.balh.oms.settlement;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.List;

/** Unified broker settlement file ingest counter (gap plan §5.18). */
public final class BrokerFileIngestMetrics {

    public static final String METRIC = "oms_broker_file_ingest_total";

    public static final String FILE_TRADE_CONFIRM = "trade_confirm";
    public static final String FILE_POSITION_SNAPSHOT = "position_snapshot";
    public static final String FILE_CASH_STATEMENT = "cash_statement";
    public static final String FILE_SETTLEMENT_FAIL = "settlement_fail";
    public static final String FILE_CORPORATE_ACTION = "corporate_action";

    private BrokerFileIngestMetrics() {}

    public static void record(MeterRegistry registry, String fileType, String status) {
        if (registry == null || fileType == null || status == null) {
            return;
        }
        registry.counter(METRIC, List.of(Tag.of("file_type", fileType), Tag.of("status", status))).increment();
    }
}
