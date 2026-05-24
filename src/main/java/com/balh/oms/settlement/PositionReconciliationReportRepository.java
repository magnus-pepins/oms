package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@Repository
public class PositionReconciliationReportRepository {

    public record ReportRow(
            long id,
            long batchId,
            String brokerId,
            LocalDate businessDate,
            String status,
            int rowCount,
            int matchedCount,
            int mismatchCount,
            int missingInOmsCount,
            int missingInBrokerCount,
            Instant createdAt) {}

    public record ReportDetailRow(
            long id,
            long reportId,
            String outcome,
            UUID accountId,
            String instrumentSymbol,
            UUID custodyAccountId,
            BigDecimal brokerQuantityTotal,
            BigDecimal omsQuantityTotal,
            String diffJson,
            Long breakId) {}

    private static final String INSERT_REPORT =
            """
                    INSERT INTO position_reconciliation_report (
                        batch_id, broker_id, business_date, status,
                        row_count, matched_count, mismatch_count,
                        missing_in_oms_count, missing_in_broker_count
                    ) VALUES (
                        :batchId, :brokerId, :businessDate, :status,
                        :rowCount, :matched, :mismatch, :missingOms, :missingBroker
                    )
                    """;

    private static final String INSERT_DETAIL =
            """
                    INSERT INTO position_reconciliation_report_row (
                        report_id, outcome, account_id, instrument_symbol, custody_account_id,
                        broker_quantity_total, oms_quantity_total, diff_json, break_id
                    ) VALUES (
                        :reportId, :outcome, :accountId, :instrumentSymbol, :custodyAccountId,
                        :brokerQty, :omsQty, CAST(:diffJson AS JSONB), :breakId
                    )
                    """;

    private static final String LIST_REPORTS =
            """
                    SELECT id, batch_id, broker_id, business_date, status,
                           row_count, matched_count, mismatch_count,
                           missing_in_oms_count, missing_in_broker_count, created_at
                    FROM position_reconciliation_report
                    ORDER BY created_at DESC, id DESC
                    LIMIT :lim OFFSET :off
                    """;

    private static final String LIST_DETAILS =
            """
                    SELECT id, report_id, outcome, account_id, instrument_symbol, custody_account_id,
                           broker_quantity_total, oms_quantity_total, diff_json::text AS diff_json, break_id
                    FROM position_reconciliation_report_row
                    WHERE report_id = :reportId
                    ORDER BY id
                    LIMIT :lim
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public PositionReconciliationReportRepository(NamedParameterJdbcTemplate jdbc) {
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
                        .addValue("rowCount", insert.rowCount())
                        .addValue("matched", insert.matchedCount())
                        .addValue("mismatch", insert.mismatchCount())
                        .addValue("missingOms", insert.missingInOmsCount())
                        .addValue("missingBroker", insert.missingInBrokerCount()),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        if (key == null) {
            throw new IllegalStateException("position_reconciliation_report insert returned no id");
        }
        return key.longValue();
    }

    public void insertDetail(DetailInsert insert) {
        jdbc.update(
                INSERT_DETAIL,
                new MapSqlParameterSource()
                        .addValue("reportId", insert.reportId())
                        .addValue("outcome", insert.outcome())
                        .addValue("accountId", insert.accountId())
                        .addValue("instrumentSymbol", insert.instrumentSymbol())
                        .addValue("custodyAccountId", insert.custodyAccountId())
                        .addValue("brokerQty", insert.brokerQuantityTotal())
                        .addValue("omsQty", insert.omsQuantityTotal())
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
                                rs.getInt("row_count"),
                                rs.getInt("matched_count"),
                                rs.getInt("mismatch_count"),
                                rs.getInt("missing_in_oms_count"),
                                rs.getInt("missing_in_broker_count"),
                                rs.getTimestamp("created_at").toInstant()));
    }

    public List<ReportDetailRow> listDetails(long reportId, int limit) {
        return jdbc.query(
                LIST_DETAILS,
                new MapSqlParameterSource().addValue("reportId", reportId).addValue("lim", limit),
                (rs, rowNum) ->
                        new ReportDetailRow(
                                rs.getLong("id"),
                                rs.getLong("report_id"),
                                rs.getString("outcome"),
                                (UUID) rs.getObject("account_id"),
                                rs.getString("instrument_symbol"),
                                (UUID) rs.getObject("custody_account_id"),
                                rs.getBigDecimal("broker_quantity_total"),
                                rs.getBigDecimal("oms_quantity_total"),
                                rs.getString("diff_json"),
                                (Long) rs.getObject("break_id")));
    }

    public record ReportInsert(
            long batchId,
            String brokerId,
            LocalDate businessDate,
            String status,
            int rowCount,
            int matchedCount,
            int mismatchCount,
            int missingInOmsCount,
            int missingInBrokerCount) {}

    public record DetailInsert(
            long reportId,
            String outcome,
            UUID accountId,
            String instrumentSymbol,
            UUID custodyAccountId,
            BigDecimal brokerQuantityTotal,
            BigDecimal omsQuantityTotal,
            String diffJson,
            Long breakId) {}
}
