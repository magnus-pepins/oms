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
import java.util.Optional;

/**
 * Slice-1.5 BUY buying-power gate: compares Ledger {@code availableBalance} to
 * notional + estimated commission ({@link BuyFundsRequirement}) when {@code oms.ledger.enabled}
 * and a balance id is present. BUY orders without a positive reference/limit price reject.
 *
 * <p>Pop! PREDMKT bench: when ingress already ran the pre-admit ledger hold path
 * ({@code order.ledgerBalanceId()} set) and the projector passes
 * {@code skipPassControlDecisionAudit=true} (see {@link ControlRiskEvaluator#ENV_SKIP_VENUE_CONTROL_PASS_AUDIT}),
 * {@link #shouldSkipVenueBenchBuyingPowerEval} may skip a redundant projector-tier
 * {@link #evaluate(Order)} remote {@code GET /balances} — ingress hold already reserved funds.
 */
@Component
public class BuyingPowerAdmission {

    /**
     * Bench-only env gate (Pop! PREDMKT soak). When {@code true}, together with
     * {@link ControlRiskEvaluator#ENV_SKIP_VENUE_CONTROL_PASS_AUDIT} venue-prefix admits that carry a
     * {@code ledgerBalanceId}, the projector skips {@link #evaluate(Order)}.
     * When this env is unset, {@link ControlRiskEvaluator#ENV_SKIP_VENUE_CONTROL_PASS_AUDIT} is consulted
     * so a single flag on pop covers both PASS-audit omission and buying-power skip.
     */
    public static final String ENV_SKIP_VENUE_CONTROL_BUYING_POWER_EVAL =
            "OMS_PROJECTOR_SKIP_VENUE_CONTROL_BUYING_POWER_EVAL";

    private static volatile Boolean skipVenueControlBuyingPowerEvalOverride;

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
     * Pop! PREDMKT bench: skip redundant projector buying-power when ingress pre-admit hold already ran.
     *
     * @param skipPassControlDecisionAudit {@code true} when
     *     {@link ControlRiskEvaluator#ENV_SKIP_VENUE_CONTROL_PASS_AUDIT} is enabled and the symbol matches the
     *     configured venue prefix (see {@link com.balh.oms.projector.OmsPostgresProjector})
     * @param order admitted order row handed to {@link com.balh.oms.tailer.OrderControlAdmission}
     */
    public static boolean shouldSkipVenueBenchBuyingPowerEval(boolean skipPassControlDecisionAudit, Order order) {
        if (!skipPassControlDecisionAudit || order == null) {
            return false;
        }
        String balanceId = order.ledgerBalanceId();
        if (balanceId == null || balanceId.isBlank()) {
            return false;
        }
        return isVenueControlBuyingPowerEvalSkipEnabled();
    }

    static boolean isVenueControlBuyingPowerEvalSkipEnabled() {
        if (skipVenueControlBuyingPowerEvalOverride != null) {
            return skipVenueControlBuyingPowerEvalOverride;
        }
        String dedicated = System.getenv(ENV_SKIP_VENUE_CONTROL_BUYING_POWER_EVAL);
        if (dedicated != null) {
            return Boolean.parseBoolean(dedicated);
        }
        return Boolean.parseBoolean(java.util.Objects.requireNonNullElse(
                System.getenv(ControlRiskEvaluator.ENV_SKIP_VENUE_CONTROL_PASS_AUDIT), "false"));
    }

    /** Visible for unit tests that pin the bench skip gate without env mutation. */
    public static void setSkipVenueControlBuyingPowerEvalForTesting(Boolean enabled) {
        skipVenueControlBuyingPowerEvalOverride = enabled;
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
        Optional<java.math.BigDecimal> requiredOpt = BuyFundsRequirement.requiredBuyFunds(order, config);
        if (requiredOpt.isEmpty()) {
            return Outcome.REJECT_INSUFFICIENT;
        }
        BigDecimal required = requiredOpt.get();
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
