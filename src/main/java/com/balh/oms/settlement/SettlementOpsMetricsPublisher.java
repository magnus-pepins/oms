package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Polls Postgres for settlement ops counters/gauges (gap plan §5.18 Slice 13a).
 *
 * <p>Label cardinality stays bounded: {@code break_type} uses the fixed CHECK constraint set;
 * aggregate gauges have no dynamic tags.
 */
@Component
public class SettlementOpsMetricsPublisher {

    private static final Logger log = LoggerFactory.getLogger(SettlementOpsMetricsPublisher.class);

    public static final String METRIC_BREAKS_OPEN = "oms_settlement_breaks_open";
    public static final String METRIC_BREAK_AGE_SECONDS = "oms_settlement_break_age_seconds";
    public static final String METRIC_STUCK_OUTBOX = "oms_ledger_settlement_outbox_stuck_total";
    public static final String METRIC_FAILS_OPEN = "oms_settlement_fails_open";
    public static final String METRIC_CA_PENDING = "oms_corporate_action_events_pending";
    public static final String METRIC_POSITION_BREAKS = "oms_position_reconciliation_breaks_total";
    public static final String METRIC_CASH_BREAKS = "oms_cash_reconciliation_breaks_total";
    public static final String METRIC_CUSTOMER_NOTIFICATION_PENDING =
            "oms_settlement_customer_notification_outbox_pending_total";
    public static final String METRIC_CUSTOMER_NOTIFICATION_STUCK =
            "oms_settlement_customer_notification_outbox_stuck_total";
    public static final String TAG_BREAK_TYPE = "break_type";

    /** Sentinel before the first successful poll. */
    public static final double NO_DATA = -1.0;

    private final OmsConfig config;
    private final SettlementOpsMetricsRepository metricsRepo;
    private final MeterRegistry meterRegistry;

    private final Map<String, AtomicLong> openBreaksByType = new ConcurrentHashMap<>();
    private final AtomicLong breakAgeSeconds = new AtomicLong(-1L);
    private final AtomicLong stuckOutboxTotal = new AtomicLong(-1L);
    private final AtomicLong openFailsTotal = new AtomicLong(-1L);
    private final AtomicLong pendingCorporateActions = new AtomicLong(-1L);
    private final AtomicLong positionBreaksTotal = new AtomicLong(-1L);
    private final AtomicLong cashBreaksTotal = new AtomicLong(-1L);
    private final AtomicLong customerNotificationPending = new AtomicLong(-1L);
    private final AtomicLong customerNotificationStuck = new AtomicLong(-1L);

    public SettlementOpsMetricsPublisher(
            OmsConfig config, SettlementOpsMetricsRepository metricsRepo, MeterRegistry meterRegistry) {
        this.config = config;
        this.metricsRepo = metricsRepo;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    void registerGauges() {
        for (String breakType : SettlementOpsMetricsRepository.boundedBreakTypes()) {
            AtomicLong holder = openBreaksByType.computeIfAbsent(breakType, k -> new AtomicLong(-1L));
            Gauge.builder(METRIC_BREAKS_OPEN, holder, AtomicLong::doubleValue)
                    .description("Open or investigating reconciliation breaks by type.")
                    .tag(TAG_BREAK_TYPE, breakType)
                    .register(meterRegistry);
        }
        Gauge.builder(METRIC_BREAK_AGE_SECONDS, breakAgeSeconds, AtomicLong::doubleValue)
                .description("Age in seconds of the oldest open/investigating reconciliation break.")
                .register(meterRegistry);
        Gauge.builder(METRIC_STUCK_OUTBOX, stuckOutboxTotal, AtomicLong::doubleValue)
                .description(
                        "Unposted ledger settlement outbox rows with attempts >= configured stuck threshold.")
                .register(meterRegistry);
        Gauge.builder(METRIC_FAILS_OPEN, openFailsTotal, AtomicLong::doubleValue)
                .description("Broker settlement fail rows not yet applied to OMS executions.")
                .register(meterRegistry);
        Gauge.builder(METRIC_CA_PENDING, pendingCorporateActions, AtomicLong::doubleValue)
                .description("Corporate action events awaiting processing.")
                .register(meterRegistry);
        Gauge.builder(METRIC_POSITION_BREAKS, positionBreaksTotal, AtomicLong::doubleValue)
                .description("Open position reconciliation breaks (position_mismatch).")
                .register(meterRegistry);
        Gauge.builder(METRIC_CASH_BREAKS, cashBreaksTotal, AtomicLong::doubleValue)
                .description("Open cash reconciliation breaks (cash_mismatch).")
                .register(meterRegistry);
        Gauge.builder("oms_settlement_confirm_late_total", metricsRepo, SettlementOpsMetricsRepository::countLateSettlementExecutions)
                .description("Executions past expected settlement date not settled/failed.")
                .register(meterRegistry);
        Gauge.builder("oms_broker_file_late_total", metricsRepo, SettlementOpsMetricsRepository::countLateBrokerFileBatches)
                .description("Broker EOD file batches past business date not applied.")
                .register(meterRegistry);
        Gauge.builder(METRIC_CUSTOMER_NOTIFICATION_PENDING, customerNotificationPending, AtomicLong::doubleValue)
                .description("Unpublished settlement customer notification outbox rows.")
                .register(meterRegistry);
        Gauge.builder(METRIC_CUSTOMER_NOTIFICATION_STUCK, customerNotificationStuck, AtomicLong::doubleValue)
                .description(
                        "Unpublished customer notification outbox rows with attempts >= configured stuck threshold.")
                .register(meterRegistry);
        log.info("Registered settlement ops metrics gauges (§5.18)");
    }

    @Scheduled(fixedDelayString = "${oms.settlement.ops-metrics-poll-interval-ms:30000}")
    public void pollMetrics() {
        if (!config.getSettlement().isOpsMetricsPublisherEnabled()) {
            return;
        }
        try {
            Map<String, Integer> byType = metricsRepo.countOpenBreaksByType();
            for (String breakType : SettlementOpsMetricsRepository.boundedBreakTypes()) {
                openBreaksByType
                        .computeIfAbsent(breakType, k -> new AtomicLong(-1L))
                        .set(byType.getOrDefault(breakType, 0));
            }
            breakAgeSeconds.set(metricsRepo.maxOpenBreakAgeSeconds());
            stuckOutboxTotal.set(metricsRepo.countStuckOutboxRows(config.getSettlement().getStuckOutboxMinAttempts()));
            openFailsTotal.set(metricsRepo.countOpenSettlementFails());
            pendingCorporateActions.set(metricsRepo.countPendingCorporateActionEvents());
            positionBreaksTotal.set(metricsRepo.countPositionReconciliationBreaks());
            cashBreaksTotal.set(metricsRepo.countCashReconciliationBreaks());
            customerNotificationPending.set(metricsRepo.countPendingCustomerNotifications());
            customerNotificationStuck.set(
                    metricsRepo.countStuckCustomerNotificationOutboxRows(
                            config.getSettlement().getStuckOutboxMinAttempts()));
        } catch (RuntimeException e) {
            log.warn("Settlement ops metrics poll failed: {}", e.toString());
        }
    }

    /** Test hook — current cached value for a break type gauge. */
    long openBreakCount(String breakType) {
        AtomicLong holder = openBreaksByType.get(breakType);
        return holder == null ? -1L : holder.get();
    }

    long breakAgeSeconds() {
        return breakAgeSeconds.get();
    }

    long stuckOutboxTotal() {
        return stuckOutboxTotal.get();
    }

    long openFailsTotal() {
        return openFailsTotal.get();
    }

    long pendingCorporateActions() {
        return pendingCorporateActions.get();
    }
}
