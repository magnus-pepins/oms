package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
import com.balh.oms.ledger.LedgerBalanceClient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Slice-1.5 BUY buying-power gate: compares Ledger {@code availableBalance} to
 * {@code quantity * limitPrice} when {@code oms.ledger.enabled} and a balance id is present.
 */
@Component
public class BuyingPowerAdmission {

    private static final String METRIC_SELL_LEDGER_SKIP = "oms_buying_power_sell_ledger_skip_total";

    private final OmsConfig config;
    private final ObjectProvider<LedgerBalanceClient> ledger;
    private final Counter sellLedgerSkip;

    public BuyingPowerAdmission(
            OmsConfig config,
            ObjectProvider<LedgerBalanceClient> ledger,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.ledger = ledger;
        this.sellLedgerSkip = Counter.builder(METRIC_SELL_LEDGER_SKIP)
                .description("SELL orders skip Ledger buying-power evaluation (no remote call)")
                .register(meterRegistry);
    }

    public enum Outcome {
        PROCEED,
        REJECT_INSUFFICIENT,
        REJECT_LEDGER_UNAVAILABLE
    }

    /**
     * @param order authoritative row (same version as the control event expects)
     */
    public Outcome evaluate(Order order) {
        if (!config.getLedger().isEnabled()) {
            return Outcome.PROCEED;
        }
        LedgerBalanceClient client = ledger.getIfAvailable();
        if (client == null) {
            return Outcome.REJECT_LEDGER_UNAVAILABLE;
        }
        if (order.side() != Side.BUY) {
            if (order.side() == Side.SELL) {
                sellLedgerSkip.increment();
            }
            return Outcome.PROCEED;
        }
        if (order.ledgerBalanceId() == null || order.ledgerBalanceId().isBlank()) {
            return Outcome.PROCEED;
        }
        if (order.limitPrice() == null) {
            return Outcome.PROCEED;
        }
        BigDecimal required = order.quantity().multiply(order.limitPrice());
        try {
            BigDecimal available = client.fetchAvailableBalance(order.ledgerBalanceId());
            if (available.compareTo(required) < 0) {
                return Outcome.REJECT_INSUFFICIENT;
            }
            return Outcome.PROCEED;
        } catch (LedgerBalanceClient.LedgerServiceException e) {
            return Outcome.REJECT_LEDGER_UNAVAILABLE;
        }
    }
}
