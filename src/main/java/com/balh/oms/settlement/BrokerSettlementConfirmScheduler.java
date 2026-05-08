package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Optional drain of {@code broker_settlement_confirm} (off by default).
 */
@Component
public class BrokerSettlementConfirmScheduler {

    private static final Logger log = LoggerFactory.getLogger(BrokerSettlementConfirmScheduler.class);

    private final OmsConfig config;
    private final SettlementConfirmProcessor processor;

    public BrokerSettlementConfirmScheduler(OmsConfig config, SettlementConfirmProcessor processor) {
        this.config = config;
        this.processor = processor;
    }

    @Scheduled(fixedDelayString = "${oms.settlement.broker-confirm-reconciler-interval-ms:10000}")
    public void drainPendingBrokerConfirms() {
        if (!config.getSettlement().isBrokerConfirmReconcilerEnabled()) {
            return;
        }
        try {
            int n = processor.processPendingBatch(config.getSettlement().getBrokerConfirmReconcilerBatchSize());
            if (n > 0) {
                log.info("Processed {} broker settlement confirm row(s)", n);
            }
        } catch (Exception e) {
            log.warn("Broker settlement confirm drain failed", e);
        }
    }
}
