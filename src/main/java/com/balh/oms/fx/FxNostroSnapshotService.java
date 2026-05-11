package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.LedgerBalanceReadModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * FX M3 Track 2: aggregates configured Ledger balances for nostro-style read panels (no second book).
 */
@Service
public class FxNostroSnapshotService {

    private final OmsConfig omsConfig;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalanceClient;

    public FxNostroSnapshotService(OmsConfig omsConfig, ObjectProvider<LedgerBalanceClient> ledgerBalanceClient) {
        this.omsConfig = omsConfig;
        this.ledgerBalanceClient = ledgerBalanceClient;
    }

    public Map<String, Object> buildSnapshot() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled() || !fx.isNostroReadEnabled()) {
            throw new IllegalStateException("fx_nostro_read_disabled");
        }
        LedgerBalanceClient client = ledgerBalanceClient.getIfAvailable();
        if (client == null) {
            throw new IllegalStateException("ledger_client_unavailable");
        }
        List<String> ids = fx.nostroBalanceIds();
        if (ids.isEmpty()) {
            throw new IllegalStateException("fx_nostro_balance_ids_empty");
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String id : ids) {
            try {
                LedgerBalanceReadModel m = client.fetchBalanceReadModel(id);
                rows.add(rowOk(m));
            } catch (LedgerBalanceClient.LedgerServiceException e) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("balanceId", id);
                err.put("error", e.getMessage());
                rows.add(err);
            }
        }
        return Map.of("asOf", Instant.now().toString(), "source", "ledger", "balances", rows);
    }

    private static Map<String, Object> rowOk(LedgerBalanceReadModel m) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("balanceId", m.balanceId());
        row.put("availableBalance", m.availableBalance().toPlainString());
        row.put("bookedBalance", m.bookedBalance().toPlainString());
        row.put("currency", m.currency());
        row.put("identityId", m.identityId());
        return row;
    }
}
