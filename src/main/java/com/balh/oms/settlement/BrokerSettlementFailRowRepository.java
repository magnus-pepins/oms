package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

@Repository
public class BrokerSettlementFailRowRepository {

    public record FailRow(
            long id,
            long batchId,
            String brokerId,
            String brokerFailId,
            String brokerTradeId,
            String executionRef,
            String instrumentSymbol,
            String side,
            BigDecimal failedQuantity,
            LocalDate intendedSettlementDate,
            String failReason,
            LocalDate expectedResolutionDate,
            BigDecimal penaltyAmount,
            String penaltyCurrency,
            String resolutionStatus,
            Long executionId,
            Long lotId,
            java.time.Instant appliedAt,
            String applyError,
            java.time.Instant penaltyBookedAt) {}

    private static final String INSERT_IGNORE =
            """
                    INSERT INTO broker_settlement_fail_row (
                        batch_id, broker_id, broker_fail_id, broker_trade_id, execution_ref,
                        instrument_symbol, side, failed_quantity, intended_settlement_date,
                        fail_reason, expected_resolution_date, penalty_amount, penalty_currency,
                        resolution_status, raw_row_json
                    ) VALUES (
                        :batchId, :brokerId, :brokerFailId, :brokerTradeId, :executionRef,
                        :instrumentSymbol, :side, :failedQuantity, :intendedSettlementDate,
                        :failReason, :expectedResolutionDate, :penaltyAmount, :penaltyCurrency,
                        :resolutionStatus, CAST(:rawRowJson AS JSONB)
                    )
                    ON CONFLICT DO NOTHING
                    """;

    private static final String LIST_BY_BATCH =
            """
                    SELECT id, batch_id, broker_id, broker_fail_id, broker_trade_id, execution_ref,
                           instrument_symbol, side, failed_quantity, intended_settlement_date,
                           fail_reason, expected_resolution_date, penalty_amount, penalty_currency,
                           resolution_status, execution_id, lot_id, applied_at, apply_error,
                           penalty_booked_at
                    FROM broker_settlement_fail_row
                    WHERE batch_id = :batchId
                    ORDER BY id
                    """;

    private static final String MARK_APPLY =
            """
                    UPDATE broker_settlement_fail_row
                    SET execution_id = :executionId,
                        lot_id = :lotId,
                        applied_at = NOW(),
                        apply_error = :applyError
                    WHERE id = :id AND applied_at IS NULL
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerSettlementFailRowRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Long insertIgnore(InsertCommand cmd) {
        var kh = new GeneratedKeyHolder();
        jdbc.update(
                INSERT_IGNORE,
                new MapSqlParameterSource()
                        .addValue("batchId", cmd.batchId())
                        .addValue("brokerId", cmd.brokerId())
                        .addValue("brokerFailId", cmd.brokerFailId())
                        .addValue("brokerTradeId", cmd.brokerTradeId())
                        .addValue("executionRef", cmd.executionRef())
                        .addValue("instrumentSymbol", cmd.instrumentSymbol())
                        .addValue("side", cmd.side())
                        .addValue("failedQuantity", cmd.failedQuantity())
                        .addValue(
                                "intendedSettlementDate",
                                Date.valueOf(cmd.intendedSettlementDate()))
                        .addValue("failReason", cmd.failReason())
                        .addValue(
                                "expectedResolutionDate",
                                cmd.expectedResolutionDate() == null
                                        ? null
                                        : Date.valueOf(cmd.expectedResolutionDate()))
                        .addValue("penaltyAmount", cmd.penaltyAmount())
                        .addValue("penaltyCurrency", cmd.penaltyCurrency())
                        .addValue("resolutionStatus", cmd.resolutionStatus())
                        .addValue("rawRowJson", cmd.rawRowJson()),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        return key == null ? null : key.longValue();
    }

    public List<FailRow> listByBatch(long batchId) {
        return jdbc.query(LIST_BY_BATCH, new MapSqlParameterSource("batchId", batchId), (rs, rowNum) -> {
            java.sql.Timestamp applied = rs.getTimestamp("applied_at");
            return new FailRow(
                    rs.getLong("id"),
                    rs.getLong("batch_id"),
                    rs.getString("broker_id"),
                    rs.getString("broker_fail_id"),
                    rs.getString("broker_trade_id"),
                    rs.getString("execution_ref"),
                    rs.getString("instrument_symbol"),
                    rs.getString("side"),
                    rs.getBigDecimal("failed_quantity"),
                    rs.getDate("intended_settlement_date").toLocalDate(),
                    rs.getString("fail_reason"),
                    rs.getDate("expected_resolution_date") == null
                            ? null
                            : rs.getDate("expected_resolution_date").toLocalDate(),
                    rs.getBigDecimal("penalty_amount"),
                    rs.getString("penalty_currency"),
                    rs.getString("resolution_status"),
                    (Long) rs.getObject("execution_id"),
                    (Long) rs.getObject("lot_id"),
                    applied == null ? null : applied.toInstant(),
                    rs.getString("apply_error"),
                    rs.getTimestamp("penalty_booked_at") == null
                            ? null
                            : rs.getTimestamp("penalty_booked_at").toInstant());
        });
    }

    public void markPenaltyBooked(long failRowId) {
        jdbc.update(
                """
                        UPDATE broker_settlement_fail_row
                        SET penalty_booked_at = NOW()
                        WHERE id = :id AND penalty_booked_at IS NULL
                        """,
                new MapSqlParameterSource("id", failRowId));
    }

    public void markApplyResult(long failRowId, Long executionId, Long lotId, String applyError) {
        jdbc.update(
                MARK_APPLY,
                new MapSqlParameterSource()
                        .addValue("id", failRowId)
                        .addValue("executionId", executionId)
                        .addValue("lotId", lotId)
                        .addValue("applyError", applyError));
    }

    public record InsertCommand(
            long batchId,
            String brokerId,
            String brokerFailId,
            String brokerTradeId,
            String executionRef,
            String instrumentSymbol,
            String side,
            BigDecimal failedQuantity,
            LocalDate intendedSettlementDate,
            String failReason,
            LocalDate expectedResolutionDate,
            BigDecimal penaltyAmount,
            String penaltyCurrency,
            String resolutionStatus,
            String rawRowJson) {}
}
