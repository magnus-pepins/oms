package com.balh.oms.settlement;

import com.balh.oms.domain.Side;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** Adds ISK tax-wrapper fields to Ledger settlement outbox payloads (gap plan §5.10). */
@Service
public class IskSettlementMetadataService {

    private final OmsAccountTaxWrapperRepository accountTaxWrapper;

    public IskSettlementMetadataService(OmsAccountTaxWrapperRepository accountTaxWrapper) {
        this.accountTaxWrapper = accountTaxWrapper;
    }

    public void enrich(ObjectNode payload, UUID accountId, String side, String legKind) {
        accountTaxWrapper.findByAccountId(accountId).ifPresent(w -> {
            if (!OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK.equals(w.taxWrapper())) {
                return;
            }
            payload.put("taxWrapper", OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK);
            if (w.iskAccountId() != null) {
                payload.put("iskAccountId", w.iskAccountId().toString());
            }
            if (w.ledgerBalanceId() != null && !w.ledgerBalanceId().isBlank()) {
                payload.put("ledgerBalanceId", w.ledgerBalanceId());
            }
            String depositClass = depositClassFor(side, legKind);
            if (depositClass != null) {
                payload.put("iskDepositClass", depositClass);
            }
        });
    }

    static String depositClassFor(String side, String legKind) {
        if (LedgerSettlementOutboxRepository.LEG_FEE.equals(legKind)) {
            return IskDepositClass.COMMISSION;
        }
        if (LedgerSettlementOutboxRepository.LEG_CASH_BASE.equals(legKind)
                || LedgerSettlementOutboxRepository.LEG_CASH_QUOTE.equals(legKind)) {
            return IskDepositClass.FX_CONVERSION;
        }
        if (LedgerSettlementOutboxRepository.LEG_CASH.equals(legKind)) {
            if (side != null && Side.SELL.name().equalsIgnoreCase(side)) {
                return IskDepositClass.SALE_PROCEEDS_EXCLUDED;
            }
            return IskDepositClass.TRADE_FUNDING;
        }
        return null;
    }
}
