package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Closes expired open netting buckets so treasury can hedge closed windows. */
@Component
public class FxCustomerFlowNettingWindowJob {

    private static final Logger log = LoggerFactory.getLogger(FxCustomerFlowNettingWindowJob.class);

    private final FxCustomerFlowNettingService netting;
    private final OmsConfig omsConfig;

    public FxCustomerFlowNettingWindowJob(FxCustomerFlowNettingService netting, OmsConfig omsConfig) {
        this.netting = netting;
        this.omsConfig = omsConfig;
    }

    @Scheduled(
            fixedDelayString = "${oms.fx.customer-flow-netting.close-interval-ms:60000}",
            initialDelayString = "${oms.fx.customer-flow-netting.close-interval-ms:60000}")
    public void closeExpiredWindows() {
        if (!omsConfig.getFx().isModuleEnabled() || !omsConfig.getFx().isCustomerFlowNettingEnabled()) {
            return;
        }
        int closed = netting.closeExpiredWindows();
        if (closed > 0) {
            log.info("fx_netting closed {} expired open bucket(s)", closed);
        }
    }
}
