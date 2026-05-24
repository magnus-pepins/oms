package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

@Repository
public class CorporateActionReconciliationReportRepository {

    public record ReportRow(
            long id,
            long batchId,
            String brokerId,
            LocalDate businessDate,
            String status,
            int eventCount,
            int matchedCount,
            int mismatchCount,
            int unmatchedCount,
            Instant createdAt) {}

    public record ReportDetailRow(
            long id,
            long reportId,
            String outcome,
            String brokerEventId,
            Long corporateActionEventId,
            String actionType,
            String instrumentSymbol,
            String diffJson,
            Long breakId) {}

    private static final String INSERT_REPORT =
            """
                    INSERT INTO corporate_action_reconciliation_report (
                        batch_id, broker_id, business_date, status,
                        event_count, matched_count, mismatch_count, unmatched_count
                    ) VALUES (
                        :batchId, :brokerId, :businessDate, :status,
                        :eventCount, :matched, :mismatch, :unmatched
                    )
                    """;

    private static final String INSERT_DETAIL =
            """
                    INSERT INTO corporate_action_reconciliation_report_row (
                        report_id, outcome, broker_event_id, corporate_action_event_id,
                        action_type, instrument_symbol, diff_json, break_id
                    ) VALUES (
                        :reportId, :outcome, :brokerEventId, :eventId,
                        :actionType, :symbol, CAST(:diffJson AS JSONB), :breakId
                    )
                    """;

    private static final String LIST_REPORTS =
            """
                    SELECT id, batch_id, broker_id, business_date, status,
                           event_count, matched_count, mismatch_count, unmatched_count, created_at
                    FROM corporate_action_reconciliation_report
                    ORDER BY created_at DESC, id DESC
                    LIMIT :lim OFFSET :off
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public CorporateActionReconciliationReportRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long insertReport(ReportInsert insert) {
        var kh = new GeneratedKeyHolder();
        jdbc.update(
                INSERT_REPORT,
                new MapSqlParameterSource()
                        .addValue("batchId", insert.batchId())
                        .addValue("brokerId", insert.brokerId())
                        .addValue("businessDate", Date.valueOf(insert.businessDate()))
                        .addValue("status", insert.status())
                        .addValue("eventCount", insert.eventCount())
                        .addValue("matched", insert.matchedCount())
                        .addValue("mismatch", insert.mismatchCount())
                        .addValue("unmatched", insert.unmatchedCount()),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        if (key == null) {
            throw new IllegalStateException("corporate_action_reconciliation_report insert returned no id");
        }
        return key.longValue();
    }

    public void insertDetail(DetailInsert insert) {
        jdbc.update(
                INSERT_DETAIL,
                new MapSqlParameterSource()
                        .addValue("reportId", insert.reportId())
                        .addValue("outcome", insert.outcome())
                        .addValue("brokerEventId", insert.brokerEventId())
                        .addValue("eventId", insert.corporateActionEventId())
                        .addValue("actionType", insert.actionType())
                        .addValue("symbol", insert.instrumentSymbol())
                        .addValue("diffJson", insert.diffJson())
                        .addValue("breakId", insert.breakId()));
    }

    public List<ReportRow> listRecent(int limit, int offset) {
        return jdbc.query(
                LIST_REPORTS,
                new MapSqlParameterSource().addValue("lim", limit).addValue("off", offset),
                (rs, rowNum) ->
                        new ReportRow(
                                rs.getLong("id"),
                                rs.getLong("batch_id"),
                                rs.getString("broker_id"),
                                rs.getDate("business_date").toLocalDate(),
                                rs.getString("status"),
                                rs.getInt("event_count"),
                                rs.getInt("matched_count"),
                                rs.getInt("mismatch_count"),
                                rs.getInt("unmatched_count"),
                                rs.getTimestamp("created_at").toInstant()));
    }

    public record ReportInsert(
            long batchId,
            String brokerId,
            LocalDate businessDate,
            String status,
            int eventCount,
            int matchedCount,
            int mismatchCount,
            int unmatchedCount) {}

    public record DetailInsert(
            long reportId,
            String outcome,
            String brokerEventId,
            Long corporateActionEventId,
            String actionType,
            String instrumentSymbol,
            String diffJson,
            Long breakId) {}
}
