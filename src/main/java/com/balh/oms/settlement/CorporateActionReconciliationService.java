package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Compares broker CA batch rows to OMS {@code corporate_action_event} after apply (gap plan §5.9).
 */
@Service
public class CorporateActionReconciliationService {

    private static final ObjectMapper JSON = new ObjectMapper();

    public record Result(
            long reportId,
            long batchId,
            String status,
            int eventCount,
            int matchedCount,
            int mismatchCount,
            int unmatchedCount) {}

    private record PendingDetail(
            String outcome,
            String brokerEventId,
            Long eventId,
            String actionType,
            String symbol,
            String diffJson,
            Long breakId) {}

    private final BrokerCorporateActionBatchRepository batches;
    private final BrokerCorporateActionRowRepository rows;
    private final CorporateActionReconciliationReportRepository reports;
    private final CorporateActionReconciliationLookupRepository lookup;
    private final ReconciliationBreakRepository breaks;

    public CorporateActionReconciliationService(
            BrokerCorporateActionBatchRepository batches,
            BrokerCorporateActionRowRepository rows,
            CorporateActionReconciliationReportRepository reports,
            CorporateActionReconciliationLookupRepository lookup,
            ReconciliationBreakRepository breaks) {
        this.batches = batches;
        this.rows = rows;
        this.reports = reports;
        this.lookup = lookup;
        this.breaks = breaks;
    }

    @Transactional
    public Optional<Result> reconcile(long batchId) {
        Optional<BrokerCorporateActionBatchRepository.BatchRow> batchOpt = batches.findById(batchId);
        if (batchOpt.isEmpty()) {
            return Optional.empty();
        }
        BrokerCorporateActionBatchRepository.BatchRow batch = batchOpt.get();
        if (!"parsed".equals(batch.status()) && !"applied".equals(batch.status())) {
            throw new IllegalStateException("batch " + batchId + " not parsed/applied: " + batch.status());
        }

        List<BrokerCorporateActionRowRepository.NoticeRow> noticeRows = rows.listByBatch(batchId);
        int matched = 0;
        int mismatch = 0;
        int unmatched = 0;
        List<PendingDetail> details = new ArrayList<>();

        for (BrokerCorporateActionRowRepository.NoticeRow row : noticeRows) {
            Optional<CorporateActionReconciliationLookupRepository.OmsEventRow> omsOpt =
                    lookup.findByBrokerEvent(row.brokerId(), row.brokerEventId());
            if (omsOpt.isEmpty()) {
                unmatched++;
                String diffJson = diff("unmatched", "no OMS corporate_action_event", row, null);
                long breakId = openBreak(batch, diffJson);
                details.add(new PendingDetail(
                        "unmatched", row.brokerEventId(), null, row.actionType(), row.instrumentSymbol(), diffJson, breakId));
                continue;
            }
            CorporateActionReconciliationLookupRepository.OmsEventRow oms = omsOpt.get();
            if (fieldsMatch(row, oms)) {
                matched++;
                details.add(new PendingDetail(
                        "matched",
                        row.brokerEventId(),
                        oms.id(),
                        row.actionType(),
                        row.instrumentSymbol(),
                        diff("matched", null, row, oms),
                        null));
            } else {
                mismatch++;
                String diffJson = diff("mismatch", "broker vs OMS field drift", row, oms);
                long breakId = openBreak(batch, diffJson);
                details.add(new PendingDetail(
                        "mismatch",
                        row.brokerEventId(),
                        oms.id(),
                        row.actionType(),
                        row.instrumentSymbol(),
                        diffJson,
                        breakId));
            }
        }

        long reportId =
                reports.insertReport(new CorporateActionReconciliationReportRepository.ReportInsert(
                        batchId,
                        batch.brokerId(),
                        batch.businessDate(),
                        "completed",
                        noticeRows.size(),
                        matched,
                        mismatch,
                        unmatched));

        for (PendingDetail d : details) {
            reports.insertDetail(new CorporateActionReconciliationReportRepository.DetailInsert(
                    reportId,
                    d.outcome(),
                    d.brokerEventId(),
                    d.eventId(),
                    d.actionType(),
                    d.symbol(),
                    d.diffJson(),
                    d.breakId()));
        }

        return Optional.of(new Result(reportId, batchId, "completed", noticeRows.size(), matched, mismatch, unmatched));
    }

    private static boolean fieldsMatch(
            BrokerCorporateActionRowRepository.NoticeRow broker,
            CorporateActionReconciliationLookupRepository.OmsEventRow oms) {
        String brokerAction = norm(broker.actionType());
        String omsAction = norm(oms.actionType());
        if ("REVERSE_SPLIT".equals(brokerAction)) {
            brokerAction = "STOCK_SPLIT";
        }
        if ("REVERSE_SPLIT".equals(omsAction)) {
            omsAction = "STOCK_SPLIT";
        }
        return brokerAction.equals(omsAction)
                && norm(broker.instrumentSymbol()).equals(norm(oms.instrumentSymbol()))
                && broker.effectiveDate().equals(oms.effectiveDate());
    }

    private static String norm(String s) {
        return s == null ? "" : s.trim().toUpperCase(Locale.ROOT);
    }

    private long openBreak(BrokerCorporateActionBatchRepository.BatchRow batch, String diffJson) {
        return breaks.insert(new ReconciliationBreakRepository.InsertCommand(
                ReconciliationBreakRepository.BREAK_CORPORATE_ACTION_MISMATCH,
                ReconciliationBreakRepository.SEVERITY_HIGH,
                ReconciliationBreakRepository.SOURCE_BROKER,
                null,
                null,
                null,
                batch.businessDate(),
                diffJson,
                "system"));
    }

    private static String diff(
            String outcome,
            String reason,
            BrokerCorporateActionRowRepository.NoticeRow broker,
            CorporateActionReconciliationLookupRepository.OmsEventRow oms) {
        ObjectNode root = JSON.createObjectNode();
        root.put("outcome", outcome);
        if (reason != null) {
            root.put("reason", reason);
        }
        ObjectNode b = root.putObject("broker");
        b.put("brokerEventId", broker.brokerEventId());
        b.put("actionType", broker.actionType());
        b.put("instrumentSymbol", broker.instrumentSymbol());
        b.put("effectiveDate", broker.effectiveDate().toString());
        if (oms != null) {
            ObjectNode o = root.putObject("oms");
            o.put("eventId", oms.id());
            o.put("actionType", oms.actionType());
            o.put("instrumentSymbol", oms.instrumentSymbol());
            o.put("effectiveDate", oms.effectiveDate().toString());
        }
        try {
            return JSON.writeValueAsString(root);
        } catch (Exception e) {
            return "{}";
        }
    }
}
