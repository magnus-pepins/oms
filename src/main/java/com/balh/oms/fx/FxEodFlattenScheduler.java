package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * M3 Track 5: EOD flatten placeholder (finance-gated; no Ledger postings until product enables flow).
 */
@Component
@ConditionalOnProperty(prefix = "oms.fx", name = "eod-flatten-enabled", havingValue = "true")
public class FxEodFlattenScheduler {

    private static final Logger log = LoggerFactory.getLogger(FxEodFlattenScheduler.class);

    private final OmsConfig omsConfig;

    public FxEodFlattenScheduler(OmsConfig omsConfig) {
        this.omsConfig = omsConfig;
    }

    @Scheduled(fixedDelayString = "${oms.fx.eod-flatten-interval-ms}")
    public void tick() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled() || !fx.isEodFlattenEnabled()) {
            return;
        }
        log.info("event=fx_eod_flatten_tick finance_gated=true module=stub");
    }
}
