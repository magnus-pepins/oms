package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.persistence.ControlRuntimeFlagsRepository;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Stateless pre-trade checks that run in {@link com.balh.oms.tailer.ControlTailer}
 * before Ledger buying-power admission (when enabled) and before WORKING CAS.
 *
 * <p>All thresholds are bound from {@link OmsConfig#getRisk()} — no bare numeric
 * literals in business logic (ecosystem config rule).
 */
@Component
public class ControlRiskEvaluator {

    public static final String STAGE_CONTROL = "CONTROL";

    private final OmsConfig omsConfig;
    private final ControlRuntimeFlagsRepository runtimeFlags;

    public ControlRiskEvaluator(OmsConfig omsConfig, ControlRuntimeFlagsRepository runtimeFlags) {
        this.omsConfig = omsConfig;
        this.runtimeFlags = runtimeFlags;
    }

    /**
     * @return empty if all checks pass; otherwise the first failing {@link RejectCode}.
     */
    public Optional<RejectCode> evaluate(Order order) {
        if (runtimeFlags.isGlobalHalt()) {
            return Optional.of(RejectCode.RISK_KILL_SWITCH);
        }
        var risk = omsConfig.getRisk();
        if (risk.isInstrumentAllowlistEnabled()) {
            Set<String> allowed = risk.allowedInstrumentSymbolSet();
            String sym = order.instrumentSymbol() == null
                    ? ""
                    : order.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
            if (sym.isEmpty() || !allowed.contains(sym)) {
                return Optional.of(RejectCode.RISK_INVALID_INSTRUMENT);
            }
        }
        BigDecimal maxLimitPx = risk.getFatFingerMaxLimitPrice();
        if (maxLimitPx.compareTo(BigDecimal.ZERO) > 0
                && order.limitPrice() != null
                && order.limitPrice().compareTo(maxLimitPx) > 0) {
            return Optional.of(RejectCode.RISK_FAT_FINGER_PRICE);
        }
        BigDecimal maxQty = risk.getFatFingerMaxOrderQuantity();
        if (maxQty.compareTo(BigDecimal.ZERO) > 0
                && order.quantity() != null
                && order.quantity().compareTo(maxQty) > 0) {
            return Optional.of(RejectCode.RISK_FAT_FINGER_SIZE);
        }
        BigDecimal maxNotional = risk.getMaxOrderNotional();
        if (maxNotional.compareTo(BigDecimal.ZERO) > 0
                && order.limitPrice() != null
                && order.quantity() != null) {
            BigDecimal notional = order.quantity().multiply(order.limitPrice());
            if (notional.compareTo(maxNotional) > 0) {
                return Optional.of(RejectCode.RISK_NOTIONAL_CAP);
            }
        }
        return Optional.empty();
    }
}
