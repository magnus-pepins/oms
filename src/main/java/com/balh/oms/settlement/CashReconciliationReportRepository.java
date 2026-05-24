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
public class CashReconciliationReportRepository {

    public record ReportRow(
            long id,
            long batchId,
            String brokerId,
            LocalDate businessDate,
            String currency,
            String status,
            int movementCount,
            int matchedCount,
            int mismatchCount,
            int unmatchedCount,
            int missingInBrokerCount,
            boolean balanceMismatch,
            Instant createdAt) {}

    public record ReportDetailRow(
            long id,
            long reportId,
            String outcome,
            String brokerMovementId,
            String executionRef,
            Long executionId,
            UUID accountId,
            BigDecimal brokerAmount,
            BigDecimal omsAmount,
            String diffJson,
            Long breakId) {}

    private static final String INSERT_REPORT =
            """
                    INSERT INTO cash_reconciliation_report (
                        batch_id, broker_id, business_date, currency, status,
                        movement_count, matched_count, mismatch_count,
                        unmatched_count, missing_in_broker_count, balance_mismatch
                    ) VALUES (
                        :batchId, :brokerId, :businessDate, :currency, :status,
                        :movementCount, :matched, :mismatch, :unmatched, :missingBroker, :balanceMismatch
                    )
                    """;

    private static final String INSERT_DETAIL =
            """
                    INSERT INTO cash_reconciliation_report_row (
                        report_id, outcome, broker_movement_id, execution_ref, execution_id,
                        account_id, broker_amount, oms_amount, diff_json, break_id
                    ) VALUES (
                        :reportId, :outcome, :brokerMovementId, :executionRef, :executionId,
                        :accountId, :brokerAmount, :omsAmount, CAST(:diffJson AS JSONB), :breakId
                    )
                    """;

    private static final String LIST_REPORTS =
            """
                    SELECT id, batch_id, broker_id, business_date, currency, status,
                           movement_count, matched_count, mismatch_count,
                           unmatched_count, missing_in_broker_count, balance_mismatch, created_at
                    FROM cash_reconciliation_report
                    ORDER BY created_at DESC, id DESC
                    LIMIT :lim OFFSET :off
                    """;

    private static final String LIST_DETAILS =
            """
                    SELECT id, report_id, outcome, broker_movement_id, execution_ref, execution_id,
                           account_id, broker_amount, oms_amount, diff_json::text AS diff_json, break_id
                    FROM cash_reconciliation_report_row
                    WHERE report_id = :reportId
                    ORDER BY id
                    LIMIT :lim
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public CashReconciliationReportRepository(NamedParameterJdbcTemplate jdbc) {
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
                        .addValue("currency", insert.currency())
                        .addValue("status", insert.status())
                        .addValue("movementCount", insert.movementCount())
                        .addValue("matched", insert.matchedCount())
                        .addValue("mismatch", insert.mismatchCount())
                        .addValue("unmatched", insert.unmatchedCount())
                        .addValue("missingBroker", insert.missingInBrokerCount())
                        .addValue("balanceMismatch", insert.balanceMismatch()),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        if (key == null) {
            throw new IllegalStateException("cash_reconciliation_report insert returned no id");
        }
        return key.longValue();
    }

    public void insertDetail(DetailInsert insert) {
        jdbc.update(
                INSERT_DETAIL,
                new MapSqlParameterSource()
                        .addValue("reportId", insert.reportId())
                        .addValue("outcome", insert.outcome())
                        .addValue("brokerMovementId", insert.brokerMovementId())
                        .addValue("executionRef", insert.executionRef())
                        .addValue("executionId", insert.executionId())
                        .addValue("accountId", insert.accountId())
                        .addValue("brokerAmount", insert.brokerAmount())
                        .addValue("omsAmount", insert.omsAmount())
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
                                rs.getString("currency"),
                                rs.getString("status"),
                                rs.getInt("movement_count"),
                                rs.getInt("matched_count"),
                                rs.getInt("mismatch_count"),
                                rs.getInt("unmatched_count"),
                                rs.getInt("missing_in_broker_count"),
                                rs.getBoolean("balance_mismatch"),
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
                                rs.getString("broker_movement_id"),
                                rs.getString("execution_ref"),
                                (Long) rs.getObject("execution_id"),
                                (UUID) rs.getObject("account_id"),
                                rs.getBigDecimal("broker_amount"),
                                rs.getBigDecimal("oms_amount"),
                                rs.getString("diff_json"),
                                (Long) rs.getObject("break_id")));
    }

    public record ReportInsert(
            long batchId,
            String brokerId,
            LocalDate businessDate,
            String currency,
            String status,
            int movementCount,
            int matchedCount,
            int mismatchCount,
            int unmatchedCount,
            int missingInBrokerCount,
            boolean balanceMismatch) {}

    public record DetailInsert(
            long reportId,
            String outcome,
            String brokerMovementId,
            String executionRef,
            Long executionId,
            UUID accountId,
            BigDecimal brokerAmount,
            BigDecimal omsAmount,
            String diffJson,
            Long breakId) {}
}
