package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

/** Active nostro correspondent selection with failover metadata (§11.5.3). */
@Service
public class FxNostroCorrespondentService {

    private final FxNostroCorrespondentRepository correspondents;
    private final OmsConfig omsConfig;
    private final Clock clock;

    public FxNostroCorrespondentService(
            FxNostroCorrespondentRepository correspondents, OmsConfig omsConfig, Clock clock) {
        this.correspondents = correspondents;
        this.omsConfig = omsConfig;
        this.clock = clock;
    }

    public Map<String, Object> status() {
        var fx = omsConfig.getFx();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", Instant.now(clock).toString());
        body.put("nettingWindowMs", fx.getNettingWindowMs());
        body.put("configuredNostroBalanceIds", fx.nostroBalanceIds());
        body.put("correspondents", correspondents.listAll());
        return body;
    }

    public String resolveActiveBalanceId(String currency) {
        return correspondents
                .findActivePrimary(currency)
                .map(FxNostroCorrespondentRepository.Row::ledgerBalanceId)
                .orElse(null);
    }
}
