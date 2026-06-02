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
 * Read model for the retail FX conversion pools on the plain
 * {@code @Nostro-<CCY>} balances (no {@code -Bank} suffix).
 *
 * <p>The customer-app "move money" cross-currency path posts the two
 * customer cash legs through these pools: a EUR→GBP transfer credits
 * {@code @Nostro-EUR} (platform receives EUR) and debits
 * {@code @Nostro-GBP} (platform pays GBP out), so the signed pool balance
 * per currency is the net <em>retail</em> open FX position. This is a
 * distinct book from:
 * <ul>
 *   <li>{@code @FX-Suspense-<CCY>} — OMS-routed cross-currency invest flow
 *       + desk hedge legs ({@link FxSuspenseSnapshotService}); and</li>
 *   <li>{@code @Nostro-<CCY>-Bank} — correspondent-cash inventory the desk
 *       hedges into ({@link FxNostroSnapshotService}).</li>
 * </ul>
 *
 * <p>Before this snapshot landed nothing read the retail pools, so retail
 * conversion exposure was invisible to the desk and un-hedgeable. It is
 * now surfaced to the Treasury panel and used as the drift source for
 * auto-hedger policies whose {@code exposure_source='retail'}.
 *
 * <p>Configured via {@code oms.fx.retail-nostro.currencies-csv} (env
 * {@code OMS_FX_RETAIL_NOSTRO_CURRENCIES_CSV}, e.g. {@code USD,EUR,GBP,SEK}).
 * Each currency is looked up at indicator {@code @Nostro-<CCY>} via the
 * Ledger {@code GET /balances/indicator/.../currency/...} path
 * ({@link LedgerBalanceClient#fetchAvailableBalanceByIndicator}). Missing
 * indicators surface per-row as {@code error} strings without failing the
 * whole snapshot.
 */
@Service
public class FxRetailNostroSnapshotService {

    /**
     * Ledger indicator prefix for retail FX conversion pools. Deliberately
     * the bare {@code @Nostro-} so it never collides with the correspondent
     * inventory balances which carry the {@code -Bank} suffix.
     */
    static final String RETAIL_NOSTRO_INDICATOR_PREFIX = "@Nostro-";

    private final OmsConfig omsConfig;
    private final ObjectProvider<LedgerBalanceClient> ledgerBalanceClient;

    public FxRetailNostroSnapshotService(
            OmsConfig omsConfig, ObjectProvider<LedgerBalanceClient> ledgerBalanceClient) {
        this.omsConfig = omsConfig;
        this.ledgerBalanceClient = ledgerBalanceClient;
    }

    public Map<String, Object> buildSnapshot() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled() || !fx.isNostroReadEnabled()) {
            throw new IllegalStateException("fx_retail_nostro_read_disabled");
        }
        List<String> currencies = fx.getRetailNostro().currencies();
        if (currencies.isEmpty()) {
            throw new IllegalStateException("fx_retail_nostro_currencies_empty");
        }
        LedgerBalanceClient client = ledgerBalanceClient.getIfAvailable();
        if (client == null) {
            throw new IllegalStateException("ledger_client_unavailable");
        }
        Map<String, BigDecimal> limits = fx.getRetailNostro().maxAbsByCurrency();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String ccy : currencies) {
            String indicator = RETAIL_NOSTRO_INDICATOR_PREFIX + ccy;
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
