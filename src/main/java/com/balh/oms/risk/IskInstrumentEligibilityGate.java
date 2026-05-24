package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.settlement.InstrumentSettlementProfileRepository;
import com.balh.oms.settlement.OmsAccountTaxWrapperRepository;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Optional;
import org.springframework.stereotype.Component;

/** Rejects ISK-account orders on non-{@code isk_eligible} instruments (gap plan §5.10). */
@Component
public class IskInstrumentEligibilityGate {

    private final OmsConfig omsConfig;
    private final OmsAccountTaxWrapperRepository accountTaxWrapper;
    private final InstrumentSettlementProfileRepository settlementProfiles;

    public IskInstrumentEligibilityGate(
            OmsConfig omsConfig,
            OmsAccountTaxWrapperRepository accountTaxWrapper,
            InstrumentSettlementProfileRepository settlementProfiles) {
        this.omsConfig = omsConfig;
        this.accountTaxWrapper = accountTaxWrapper;
        this.settlementProfiles = settlementProfiles;
    }

    public Optional<RejectCode> evaluate(Order order) {
        if (!omsConfig.getRisk().isIskInstrumentEligibilityCheckEnabled()) {
            return Optional.empty();
        }
        if (order == null || order.accountId() == null) {
            return Optional.empty();
        }
        Optional<OmsAccountTaxWrapperRepository.AccountTaxWrapperRow> wrapper =
                accountTaxWrapper.findByAccountId(order.accountId());
        if (wrapper.isEmpty()
                || !OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK.equals(wrapper.get().taxWrapper())) {
            return Optional.empty();
        }
        String symbol = normalizedSymbol(order.instrumentSymbol());
        if (symbol.isEmpty()) {
            return Optional.of(RejectCode.RISK_ISK_INSTRUMENT_NOT_ELIGIBLE);
        }
        LocalDate asOf = order.receivedAt() == null
                ? LocalDate.now(ZoneOffset.UTC)
                : order.receivedAt().atZone(ZoneOffset.UTC).toLocalDate();
        boolean eligible = settlementProfiles
                .findActiveBySymbol(symbol, asOf)
                .map(p -> p.iskEligible())
                .orElse(false);
        return eligible ? Optional.empty() : Optional.of(RejectCode.RISK_ISK_INSTRUMENT_NOT_ELIGIBLE);
    }

    private static String normalizedSymbol(String symbol) {
        if (symbol == null) {
            return "";
        }
        return symbol.trim().toUpperCase(Locale.ROOT);
    }
}
