package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerMetadataClient;
import com.balh.oms.persistence.PositionsRepository;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Syncs OMS pending position count to Ledger ISK balance metadata (gap plan §5.10). */
@Component
public class IskPendingPositionCountSyncJob {

    private static final Logger log = LoggerFactory.getLogger(IskPendingPositionCountSyncJob.class);

    private final OmsConfig config;
    private final OmsAccountTaxWrapperRepository taxWrappers;
    private final PositionsRepository positions;
    private final ObjectProvider<LedgerMetadataClient> ledgerMetadata;

    public IskPendingPositionCountSyncJob(
            OmsConfig config,
            OmsAccountTaxWrapperRepository taxWrappers,
            PositionsRepository positions,
            ObjectProvider<LedgerMetadataClient> ledgerMetadata) {
        this.config = config;
        this.taxWrappers = taxWrappers;
        this.positions = positions;
        this.ledgerMetadata = ledgerMetadata;
    }

    @Scheduled(fixedDelayString = "${oms.isk-tax.pending-position-count-sync-interval-ms:300000}")
    public void syncPendingPositionCounts() {
        if (!config.getIskTax().isPendingPositionCountSyncEnabled()) {
            return;
        }
        LedgerMetadataClient client = ledgerMetadata.getIfAvailable();
        if (client == null) {
            return;
        }
        for (OmsAccountTaxWrapperRepository.AccountTaxWrapperRow row : taxWrappers.listIskAccounts()) {
            if (row.ledgerBalanceId() == null || row.ledgerBalanceId().isBlank()) {
                continue;
            }
            int pendingCount = positions.countPositionsWithPendingSettlement(row.accountId());
            try {
                client.patchBalanceMetadata(
                        row.ledgerBalanceId(), Map.of("pending_position_count", pendingCount, "taxWrapper", "isk"));
            } catch (LedgerMetadataClient.LedgerMetadataException e) {
                log.debug(
                        "ISK pending_position_count sync skipped accountId={}: {}",
                        row.accountId(),
                        e.getMessage());
            }
        }
    }
}
