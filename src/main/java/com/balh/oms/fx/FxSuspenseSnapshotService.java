package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerBalanceClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read model for open FX exposure on the {@code @FX-Suspense-<CCY>} balances.
 *
 * <p>Each cross-currency customer cash leg ({@code LEG_CASH_BASE} /
 * {@code LEG_CASH_QUOTE} in settlement) and each hedge action moves cash
 * through {@code @FX-Suspense-<ccy>}. The net of those flows per currency
 * is the open position the hedger is chasing. The auto-hedger only reads
 * <em>nostro</em> balances (see {@link FxNostroSnapshotService}) so before
 * this snapshot landed the operator had no surface to inspect the
 * suspense side — which is exactly where hedge submissions fail when
 * cumulative customer flow has drained suspense beyond the ledger's
 * overdraft tolerance.
 *
 * <p>Configured via {@code oms.fx.suspense.currencies-csv} (env
 * {@code OMS_FX_SUSPENSE_CURRENCIES_CSV}, e.g. {@code USD,EUR,GBP}).
 * Each currency is looked up at indicator {@code @FX-Suspense-<CCY>}
 * via the Ledger {@code GET /balances/indicator/.../currency/...} path
 * ({@link LedgerBalanceClient#fetchAvailableBalanceByIndicator}). Missing
 * indicators surface per-row as {@code error} strings without failing
 * the whole snapshot.
 */
@Service
public class FxSuspenseSnapshotService {

    /** Ledger indicator prefix for FX suspense balances (matches seed-fx-nostros.sh). */
    private static final String SUSPENSE_INDICATOR_PREFIX = "@FX-Suspense-";

    private final OmsConfig omsConfig;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalanceClient;

    public FxSuspenseSnapshotService(
            OmsConfig omsConfig, ObjectProvider<LedgerBalanceClient> ledgerBalanceClient) {
        this.omsConfig = omsConfig;
        this.ledgerBalanceClient = ledgerBalanceClient;
    }

    public Map<String, Object> buildSnapshot() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled() || !fx.isNostroReadEnabled()) {
            throw new IllegalStateException("fx_suspense_read_disabled");
        }
        List<String> currencies = fx.getSuspense().currencies();
        if (currencies.isEmpty()) {
            throw new IllegalStateException("fx_suspense_currencies_empty");
        }
        LedgerBalanceClient client = ledgerBalanceClient.getIfAvailable();
        if (client == null) {
            throw new IllegalStateException("ledger_client_unavailable");
        }
        Map<String, BigDecimal> limits = fx.getSuspense().maxAbsByCurrency();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String ccy : currencies) {
            String indicator = SUSPENSE_INDICATOR_PREFIX + ccy;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("currency", ccy);
            row.put("indicator", indicator);
            BigDecimal limit = limits.get(ccy);
            row.put("maxAbsLimit", limit == null ? null : limit.toPlainString());
            try {
                BigDecimal available = client.fetchAvailableBalanceByIndicator(indicator, ccy);
                row.put("availableBalance", available.toPlainString());
                row.put("absAvailable", available.abs().toPlainString());
                row.put("overLimit", limit != null && available.abs().compareTo(limit) > 0);
            } catch (LedgerBalanceClient.LedgerServiceException e) {
                row.put("error", e.getMessage());
                row.put("overLimit", false);
            }
            rows.add(row);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", Instant.now().toString());
        body.put("source", "ledger");
        body.put("balances", rows);
        return body;
    }
}
