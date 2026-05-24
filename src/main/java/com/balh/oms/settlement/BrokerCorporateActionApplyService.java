package com.balh.oms.settlement;

import com.balh.oms.corporateaction.CorporateActionEventRepository;
import com.balh.oms.corporateaction.CorporateActionProcessorJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Applies parsed broker corporate-action rows into {@code corporate_action_event} (gap plan §5.9).
 */
@Service
public class BrokerCorporateActionApplyService {

    private static final String METRIC_APPLY = "oms_corporate_action_file_apply_total";

    public record Result(
            long batchId,
            String status,
            int eventRowCount,
            int insertedCount,
            int duplicateCount,
            int skippedAlreadyApplied) {}

    private final BrokerCorporateActionBatchRepository batches;
    private final BrokerCorporateActionRowRepository rows;
    private final CorporateActionEventRepository corporateActionEvents;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public BrokerCorporateActionApplyService(
            BrokerCorporateActionBatchRepository batches,
            BrokerCorporateActionRowRepository rows,
            CorporateActionEventRepository corporateActionEvents,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.batches = batches;
        this.rows = rows;
        this.corporateActionEvents = corporateActionEvents;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @Transactional
    public Optional<Result> apply(long batchId) {
        Optional<BrokerCorporateActionBatchRepository.BatchRow> batchOpt = batches.findById(batchId);
        if (batchOpt.isEmpty()) {
            return Optional.empty();
        }
        BrokerCorporateActionBatchRepository.BatchRow batch = batchOpt.get();
        if (!"parsed".equals(batch.status()) && !"applied".equals(batch.status())) {
            throw new IllegalStateException("batch not parsed: status=" + batch.status());
        }

        List<BrokerCorporateActionRowRepository.NoticeRow> noticeRows = rows.listByBatch(batchId);
        int inserted = 0;
        int duplicate = 0;
        int skipped = 0;

        for (BrokerCorporateActionRowRepository.NoticeRow row : noticeRows) {
            if (row.appliedAt() != null) {
                skipped++;
                continue;
            }
            CorporateActionProcessorJob.BrokerDates dates =
                    CorporateActionProcessorJob.parseBrokerDates(row.rawRowJson(), objectMapper);
            Optional<Long> eventId = corporateActionEvents.insertFromBrokerIfNew(
                    row.brokerId(),
                    row.brokerEventId(),
                    row.instrumentSymbol(),
                    row.actionType(),
                    row.effectiveDate(),
                    dates.recordDate(),
                    dates.payableDate(),
                    row.rawRowJson());
            if (eventId.isPresent()) {
                rows.markApplyResult(row.id(), eventId.get(), null);
                inserted++;
                recordApply("inserted");
            } else {
                rows.markApplyResult(row.id(), null, "duplicate broker event");
                duplicate++;
                recordApply("duplicate");
            }
        }

        if (!"applied".equals(batch.status())) {
            batches.updateStatus(batchId, "applied", batch.eventCount(), null);
        }

        return Optional.of(new Result(
                batchId, "applied", noticeRows.size(), inserted, duplicate, skipped));
    }

    private void recordApply(String outcome) {
        meterRegistry.counter(METRIC_APPLY, List.of(Tag.of("outcome", outcome))).increment();
    }
}
