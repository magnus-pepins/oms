package com.balh.oms.corporateaction;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.PositionsRepository;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Resolves settled holders on corporate-action record date (gap plan §5.9).
 *
 * <p>Prefers persisted {@code corporate_action_record_date_snapshot} rows captured on record date.
 * When missing, captures from live positions with source {@code live_fallback}.
 */
@Service
public class CorporateActionRecordDateSnapshotService {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionRecordDateSnapshotService.class);

    public static final String SOURCE_RECORD_DATE_JOB = "record_date_job";
    public static final String SOURCE_LIVE_FALLBACK = "live_fallback";

    private final CorporateActionRecordDateSnapshotRepository snapshots;
    private final PositionsRepository positions;
    private final OmsConfig config;

    public CorporateActionRecordDateSnapshotService(
            CorporateActionRecordDateSnapshotRepository snapshots,
            PositionsRepository positions,
            OmsConfig config) {
        this.snapshots = snapshots;
        this.positions = positions;
        this.config = config;
    }

    public record ResolvedHolder(UUID accountId, BigDecimal quantitySettled) {}

    /** Capture snapshot rows for an event on its record date from current settled positions. */
    public int captureForEvent(long eventId, String instrumentSymbol, LocalDate recordDate) {
        UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
        String symbol = instrumentSymbol.trim().toUpperCase();
        int count = 0;
        for (PositionsRepository.SettledHolder holder : positions.listSettledHolders(symbol, custody)) {
            snapshots.upsert(
                    eventId,
                    holder.accountId(),
                    symbol,
                    holder.quantitySettled(),
                    recordDate,
                    SOURCE_RECORD_DATE_JOB);
            count++;
        }
        log.info(
                "corporate_action record_date snapshot captured eventId={} symbol={} recordDate={} holders={}",
                eventId,
                symbol,
                recordDate,
                count);
        return count;
    }

    /** Resolve holders for entitlement calculation; ensures snapshot exists when record_date is set. */
    public List<ResolvedHolder> resolveHolders(CorporateActionEventRepository.ProcessingRow row) {
        UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
        String symbol = row.instrumentSymbol().trim().toUpperCase();
        LocalDate recordDate = row.recordDate();
        if (recordDate == null) {
            return positions.listSettledHolders(symbol, custody).stream()
                    .map(h -> new ResolvedHolder(h.accountId(), h.quantitySettled()))
                    .toList();
        }
        if (!snapshots.existsForEvent(row.id())) {
            log.warn(
                    "corporate_action missing record_date snapshot eventId={} recordDate={} — capturing live fallback",
                    row.id(),
                    recordDate);
            captureForEventLiveFallback(row.id(), symbol, recordDate, custody);
        }
        return snapshots.listByEvent(row.id()).stream()
                .map(h -> new ResolvedHolder(h.accountId(), h.quantitySettled()))
                .toList();
    }

    private void captureForEventLiveFallback(
            long eventId, String symbol, LocalDate recordDate, UUID custody) {
        for (PositionsRepository.SettledHolder holder : positions.listSettledHolders(symbol, custody)) {
            snapshots.upsert(
                    eventId,
                    holder.accountId(),
                    symbol,
                    holder.quantitySettled(),
                    recordDate,
                    SOURCE_LIVE_FALLBACK);
        }
    }
}
