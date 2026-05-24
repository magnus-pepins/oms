package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Configurable post-EOD cron that runs {@link SettlementDailyCloseOrchestrator} (gap plan Phase C).
 */
@Component
public class SettlementDailyCloseJob {

    private static final Logger log = LoggerFactory.getLogger(SettlementDailyCloseJob.class);

    private final OmsConfig config;
    private final SettlementDailyCloseOrchestrator orchestrator;

    public SettlementDailyCloseJob(OmsConfig config, SettlementDailyCloseOrchestrator orchestrator) {
        this.config = config;
        this.orchestrator = orchestrator;
    }

    @Scheduled(cron = "${oms.settlement.daily-close-cron:0 0 22 * * *}")
    public void runDailyClose() {
        if (!config.getSettlement().isDailyCloseEnabled()) {
            return;
        }
        log.info("Settlement daily close job starting");
        try {
            orchestrator.runDailyClose();
        } catch (Exception e) {
            log.warn("Settlement daily close job failed", e);
        }
        log.info("Settlement daily close job finished");
    }
}
