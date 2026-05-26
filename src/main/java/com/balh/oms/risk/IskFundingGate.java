package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.settlement.OmsAccountTaxWrapperRepository;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * BUY funding gate for ISK tax-wrapper accounts (gap plan §5.10 / I3).
 *
 * <p>Rejects an order with {@link RejectCode#RISK_ISK_FUNDING_MISMATCH} when:
 * <ul>
 *   <li>{@code oms.risk.isk-funding-check-enabled=true}, and
 *   <li>the order's account is mapped to {@code tax_wrapper=isk} in
 *       {@code oms_account_tax_wrapper}, and
 *   <li>{@link Order#side()} is {@link Side#BUY} (SELL generates cash; it does not
 *       fund, so the wrapper match is not material), and
 *   <li>{@link Order#ledgerBalanceId()} does not equal the ISK's own ledger
 *       balance recorded in {@code oms_account_tax_wrapper.ledger_balance_id}.
 * </ul>
 *
 * <p>Defensive failures (treated as mismatch):
 * <ul>
 *   <li>Order's {@code ledgerBalanceId} is null/blank — the BFF must always emit
 *       the ISK SEK cash balance on ISK BUYs; missing it means the picker is broken.
 *   <li>Wrapper row's {@code ledger_balance_id} is null/blank — ISK provisioning
 *       is incomplete; we cannot prove a match, so we fail loud instead of letting
 *       cash route silently.
 * </ul>
 *
 * <p>Non-ISK accounts (no wrapper row, or {@code tax_wrapper != isk}) pass through
 * untouched — the gate has nothing to say about non-ISK funding.
 *
 * <p>Mirrors the shape of {@link IskInstrumentEligibilityGate} so {@link ControlRiskEvaluator}
 * can wire it adjacent to the instrument-eligibility check; the two are independent (one
 * rejects on instrument, the other on funding source), but both rely on the ISK wrapper
 * lookup so co-locating them keeps the failure modes legible in code review.
 */
@Component
public class IskFundingGate {

    private final OmsConfig omsConfig;
    private final OmsAccountTaxWrapperRepository accountTaxWrapper;

    public IskFundingGate(
            OmsConfig omsConfig, OmsAccountTaxWrapperRepository accountTaxWrapper) {
        this.omsConfig = omsConfig;
        this.accountTaxWrapper = accountTaxWrapper;
    }

    public Optional<RejectCode> evaluate(Order order) {
        if (!omsConfig.getRisk().isIskFundingCheckEnabled()) {
            return Optional.empty();
        }
        if (order == null || order.accountId() == null) {
            return Optional.empty();
        }
        if (order.side() != Side.BUY) {
            return Optional.empty();
        }
        Optional<OmsAccountTaxWrapperRepository.AccountTaxWrapperRow> wrapper =
                accountTaxWrapper.findByAccountId(order.accountId());
        if (wrapper.isEmpty()
                || !OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK.equals(wrapper.get().taxWrapper())) {
            return Optional.empty();
        }
        String iskBalance = wrapper.get().ledgerBalanceId();
        if (iskBalance == null || iskBalance.isBlank()) {
            return Optional.of(RejectCode.RISK_ISK_FUNDING_MISMATCH);
        }
        String orderBalance = order.ledgerBalanceId();
        if (orderBalance == null || orderBalance.isBlank()) {
            return Optional.of(RejectCode.RISK_ISK_FUNDING_MISMATCH);
        }
        if (!iskBalance.equals(orderBalance)) {
            return Optional.of(RejectCode.RISK_ISK_FUNDING_MISMATCH);
        }
        return Optional.empty();
    }
}
