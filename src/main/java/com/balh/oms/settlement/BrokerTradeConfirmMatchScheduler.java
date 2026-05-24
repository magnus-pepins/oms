package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Optional drain of parsed {@code broker_confirm_batch} rows through the economic matcher
 * (off by default). Separate from {@link BrokerSettlementConfirmScheduler}, which drains
 * the v1 {@code broker_settlement_confirm} queue after rows are matched.
 */
@Component
public class BrokerTradeConfirmMatchScheduler {

    private static final Logger log = LoggerFactory.getLogger(BrokerTradeConfirmMatchScheduler.class);

    private final OmsConfig config;
    private final BrokerTradeConfirmBatchLifecycleService batchLifecycle;

    public BrokerTradeConfirmMatchScheduler(
            OmsConfig config, BrokerTradeConfirmBatchLifecycleService batchLifecycle) {
        this.config = config;
        this.batchLifecycle = batchLifecycle;
    }

    @Scheduled(fixedDelayString = "${oms.settlement.broker-trade-confirm-matcher-interval-ms:10000}")
    public void matchParsedBatches() {
        if (!config.getSettlement().isBrokerTradeConfirmMatcherSchedulerEnabled()) {
            return;
        }
        try {
            int n = batchLifecycle.processAllParsedBatches();
            if (n > 0) {
                log.info("Applied economic match to {} broker confirm batch(es)", n);
            }
        } catch (Exception e) {
            log.warn("Broker trade confirm matcher scheduler failed", e);
        }
    }
}
