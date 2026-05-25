package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

/** Active nostro correspondent selection with failover metadata (§11.5.3). */
@Service
public class FxNostroCorrespondentService {

    private final FxNostroCorrespondentRepository correspondents;
    private final ObjectProvider<FxCustomerFlowNettingService> customerFlowNetting;
    private final OmsConfig omsConfig;
    private final Clock clock;

    public FxNostroCorrespondentService(
            FxNostroCorrespondentRepository correspondents,
            ObjectProvider<FxCustomerFlowNettingService> customerFlowNetting,
            OmsConfig omsConfig,
            Clock clock) {
        this.correspondents = correspondents;
        this.customerFlowNetting = customerFlowNetting;
        this.omsConfig = omsConfig;
        this.clock = clock;
    }

    public Map<String, Object> status() {
        var fx = omsConfig.getFx();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", Instant.now(clock).toString());
        body.put("nettingWindowMs", fx.getNettingWindowMs());
        body.put("customerFlowNettingEnabled", fx.isCustomerFlowNettingEnabled());
        body.put("configuredNostroBalanceIds", fx.nostroBalanceIds());
        body.put("correspondents", correspondents.listAll().stream().map(this::toMap).toList());
        FxCustomerFlowNettingService netting = customerFlowNetting.getIfAvailable();
        body.put(
                "openNettingBuckets",
                netting == null ? List.of() : netting.openBucketSummaries());
        return body;
    }

    private Map<String, Object> toMap(FxNostroCorrespondentRepository.Row row) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", row.id());
        m.put("currency", row.currency());
        m.put("correspondentCode", row.correspondentCode());
        m.put("ledgerBalanceId", row.ledgerBalanceId());
        m.put("priority", row.priority());
        m.put("status", row.status());
        return m;
    }

    public String resolveActiveBalanceId(String currency) {
        return correspondents
                .findActivePrimary(currency)
                .map(FxNostroCorrespondentRepository.Row::ledgerBalanceId)
                .orElse(null);
    }
}
