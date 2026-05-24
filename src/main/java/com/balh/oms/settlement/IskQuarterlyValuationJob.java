package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import java.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Scheduled quarterly ISK valuation capture (gap plan §5.10 / Phase E). */
@Component
public class IskQuarterlyValuationJob {

    private static final Logger log = LoggerFactory.getLogger(IskQuarterlyValuationJob.class);

    private final OmsConfig config;
    private final IskValuationSnapshotService valuation;

    public IskQuarterlyValuationJob(OmsConfig config, IskValuationSnapshotService valuation) {
        this.config = config;
        this.valuation = valuation;
    }

    @Scheduled(cron = "${oms.isk-tax.quarterly-valuation-cron:0 0 6 1 1,4,7,10 *}")
    public void captureQuarterEndValuations() {
        if (!config.getIskTax().isQuarterlyValuationJobEnabled()) {
            return;
        }
        LocalDate asOf = LocalDate.now();
        int count = valuation.captureQuarter(asOf);
        log.info("ISK quarterly valuation captured asOf={} accounts={}", asOf, count);
    }
}
