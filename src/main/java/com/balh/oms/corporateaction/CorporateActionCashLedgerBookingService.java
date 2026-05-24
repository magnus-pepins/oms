package com.balh.oms.corporateaction;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.settlement.IskDepositClass;
import com.balh.oms.settlement.OmsAccountTaxWrapperRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Enqueues payable-date dividend + withholding legs (gap plan §5.9). */
@Service
public class CorporateActionCashLedgerBookingService {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionCashLedgerBookingService.class);

    public static final String OUTBOX_STATUS_CA_DIVIDEND = "ca_dividend";
    public static final String LEG_DIVIDEND = "dividend";
    public static final String LEG_WITHHOLDING = "dividend-withholding";

    private final CorporateActionLedgerOutboxRepository outbox;
    private final CorporateActionCashImpactRepository cashImpacts;
    private final OmsAccountTaxWrapperRepository taxWrapper;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;

    public CorporateActionCashLedgerBookingService(
            CorporateActionLedgerOutboxRepository outbox,
            CorporateActionCashImpactRepository cashImpacts,
            OmsAccountTaxWrapperRepository taxWrapper,
            ObjectMapper objectMapper,
            OmsConfig config) {
        this.outbox = outbox;
        this.cashImpacts = cashImpacts;
        this.taxWrapper = taxWrapper;
        this.objectMapper = objectMapper;
        this.config = config;
    }

    /** @return number of new outbox rows inserted (0–2) */
    public int enqueueIfDue(CorporateActionCashImpactRepository.DueCashImpactRow row) {
        if (!config.getLedger().isSettlementOutboxEnabled()) {
            return 0;
        }
        if (row.ledgerOutboxEnqueued()) {
            return 0;
        }
        LocalDate payable = row.payableDate();
        if (payable != null && payable.isAfter(LocalDate.now())) {
            return 0;
        }
        int inserted = 0;
        inserted += enqueueNetDividend(row);
        inserted += enqueueWithholding(row);
        if (inserted > 0) {
            cashImpacts.markLedgerOutboxEnqueued(row.id());
        }
        return inserted;
    }

    private int enqueueNetDividend(CorporateActionCashImpactRepository.DueCashImpactRow row) {
        if (row.netAmount() == null || row.netAmount().signum() <= 0) {
            return 0;
        }
        try {
            ObjectNode payload = basePayload(row);
            payload.put("leg", LEG_DIVIDEND);
            payload.put("netAmount", row.netAmount().toPlainString());
            payload.put("currency", row.currency());
            payload.put("nostroIndicator", "@Nostro-" + row.currency().trim().toUpperCase() + "-Bank");
            enrichIsk(payload, row.accountId(), row.actionType());
            return outbox.insertIgnore(row.id(), LEG_DIVIDEND, objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("serialize CA dividend payload cashImpactId=" + row.id(), e);
        }
    }

    private int enqueueWithholding(CorporateActionCashImpactRepository.DueCashImpactRow row) {
        if (row.withholdingAmount() == null || row.withholdingAmount().signum() <= 0) {
            return 0;
        }
        try {
            ObjectNode payload = basePayload(row);
            payload.put("leg", LEG_WITHHOLDING);
            payload.put("withholdingAmount", row.withholdingAmount().toPlainString());
            payload.put("currency", row.currency());
            payload.put("withholdingIndicator", "@Withholding-Tax-" + row.currency().trim().toUpperCase());
            payload.put("nostroIndicator", "@Nostro-" + row.currency().trim().toUpperCase() + "-Bank");
            return outbox.insertIgnore(row.id(), LEG_WITHHOLDING, objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("serialize CA withholding payload cashImpactId=" + row.id(), e);
        }
    }

    private ObjectNode basePayload(CorporateActionCashImpactRepository.DueCashImpactRow row) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("schemaVersion", 1);
        payload.put("event", "CORPORATE_ACTION_CASH_DIVIDEND");
        payload.put("cashImpactId", row.id());
        payload.put("corporateActionEventId", row.corporateActionEventId());
        payload.put("accountId", row.accountId().toString());
        payload.put("grossAmount", row.grossAmount().toPlainString());
        payload.put("bookedAt", Instant.now().toString());
        return payload;
    }

    private void enrichIsk(ObjectNode payload, UUID accountId, String actionType) {
        taxWrapper.findByAccountId(accountId).ifPresent(w -> {
            if (!OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK.equals(w.taxWrapper())) {
                return;
            }
            payload.put("taxWrapper", w.taxWrapper());
            if (w.iskAccountId() != null) {
                payload.put("iskAccountId", w.iskAccountId().toString());
            }
            if (w.ledgerBalanceId() != null) {
                payload.put("ledgerBalanceId", w.ledgerBalanceId());
            }
            payload.put("iskDepositClass", iskDepositClassFor(actionType));
        });
    }

    static String iskDepositClassFor(String actionType) {
        if (CorporateActionProcessingService.ACTION_TENDER_OFFER.equalsIgnoreCase(actionType)) {
            return IskDepositClass.SALE_PROCEEDS_EXCLUDED;
        }
        return IskDepositClass.DIVIDEND;
    }
}
