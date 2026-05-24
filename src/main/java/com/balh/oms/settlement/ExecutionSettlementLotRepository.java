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
public class ExecutionSettlementLotRepository {

    public record LotRow(
            long id,
            long executionId,
            BigDecimal quantity,
            LocalDate intendedSettlementDate,
            LocalDate actualSettlementDate,
            String status,
            String brokerFailReason,
            String brokerRef,
            Long brokerSettlementFailRowId) {}

    private static final String INSERT =
            """
                    INSERT INTO execution_settlement_lot (
                        execution_id, quantity, intended_settlement_date, actual_settlement_date,
                        status, broker_fail_reason, broker_ref, broker_settlement_fail_row_id
                    ) VALUES (
                        :executionId, :quantity, :intendedSettlementDate, :actualSettlementDate,
                        :status, :brokerFailReason, :brokerRef, :brokerFailRowId
                    )
                    """;

    private static final String LIST_BY_EXECUTION =
            """
                    SELECT id, execution_id, quantity, intended_settlement_date, actual_settlement_date,
                           status, broker_fail_reason, broker_ref, broker_settlement_fail_row_id
                    FROM execution_settlement_lot
                    WHERE execution_id = :executionId
                    ORDER BY id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public ExecutionSettlementLotRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long insert(InsertCommand cmd) {
        var kh = new GeneratedKeyHolder();
        jdbc.update(
                INSERT,
                new MapSqlParameterSource()
                        .addValue("executionId", cmd.executionId())
                        .addValue("quantity", cmd.quantity())
                        .addValue("intendedSettlementDate", Date.valueOf(cmd.intendedSettlementDate()))
                        .addValue(
                                "actualSettlementDate",
                                cmd.actualSettlementDate() == null
                                        ? null
                                        : Date.valueOf(cmd.actualSettlementDate()))
                        .addValue("status", cmd.status())
                        .addValue("brokerFailReason", cmd.brokerFailReason())
                        .addValue("brokerRef", cmd.brokerRef())
                        .addValue("brokerFailRowId", cmd.brokerSettlementFailRowId()),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        if (key == null) {
            throw new IllegalStateException("execution_settlement_lot insert returned no id");
        }
        return key.longValue();
    }

    public List<LotRow> listByExecution(long executionId) {
        return jdbc.query(
                LIST_BY_EXECUTION,
                new MapSqlParameterSource("executionId", executionId),
                (rs, rowNum) ->
                        new LotRow(
                                rs.getLong("id"),
                                rs.getLong("execution_id"),
                                rs.getBigDecimal("quantity"),
                                rs.getDate("intended_settlement_date").toLocalDate(),
                                rs.getDate("actual_settlement_date") == null
                                        ? null
                                        : rs.getDate("actual_settlement_date").toLocalDate(),
                                rs.getString("status"),
                                rs.getString("broker_fail_reason"),
                                rs.getString("broker_ref"),
                                (Long) rs.getObject("broker_settlement_fail_row_id")));
    }

    public record InsertCommand(
            long executionId,
            BigDecimal quantity,
            LocalDate intendedSettlementDate,
            LocalDate actualSettlementDate,
            String status,
            String brokerFailReason,
            String brokerRef,
            Long brokerSettlementFailRowId) {}
}
