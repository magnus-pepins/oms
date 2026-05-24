package com.balh.oms.corporateaction;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Books CA cash impacts to Ledger outbox on or after {@code payable_date} (§5.9). */
@Component
@ConditionalOnProperty(name = "oms.corporate-action.payable-date-ledger-enabled", havingValue = "true")
public class CorporateActionPayableDateJob {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionPayableDateJob.class);

    private final CorporateActionCashImpactRepository cashImpacts;
    private final CorporateActionCashLedgerBookingService ledgerBooking;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;

    public CorporateActionPayableDateJob(
            CorporateActionCashImpactRepository cashImpacts,
            CorporateActionCashLedgerBookingService ledgerBooking,
            OmsConfig config,
            org.springframework.transaction.PlatformTransactionManager transactionManager) {
        this.cashImpacts = cashImpacts;
        this.ledgerBooking = ledgerBooking;
        this.config = config;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${oms.corporate-action.payable-date-ledger-interval-ms:60000}")
    public void enqueueDueRows() {
        int batch = config.getCorporateAction().getPayableDateLedgerBatchSize();
        transactionTemplate.executeWithoutResult(
                status -> {
                    for (CorporateActionCashImpactRepository.DueCashImpactRow row :
                            cashImpacts.listDueForLedgerBooking(batch)) {
                        int n = ledgerBooking.enqueueIfDue(row);
                        if (n > 0) {
                            log.info(
                                    "corporate_action payable-date ledger outbox cashImpactId={} legs={}",
                                    row.id(),
                                    n);
                        }
                    }
                });
    }
}
