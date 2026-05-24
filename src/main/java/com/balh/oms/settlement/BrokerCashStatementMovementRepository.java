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
public class BrokerCashStatementMovementRepository {

    public record MovementRow(
            long id,
            long batchId,
            String brokerId,
            String brokerMovementId,
            String movementType,
            String executionRef,
            BigDecimal amount,
            String currency,
            LocalDate valueDate) {}

    private static final String INSERT_IGNORE =
            """
                    INSERT INTO broker_cash_statement_movement (
                        batch_id, broker_id, broker_movement_id, movement_type, execution_ref,
                        amount, currency, value_date, raw_row_json
                    ) VALUES (
                        :batchId, :brokerId, :brokerMovementId, :movementType, :executionRef,
                        :amount, :currency, :valueDate, CAST(:rawRowJson AS JSONB)
                    )
                    ON CONFLICT DO NOTHING
                    """;

    private static final String LIST_BY_BATCH =
            """
                    SELECT id, batch_id, broker_id, broker_movement_id, movement_type, execution_ref,
                           amount, currency, value_date
                    FROM broker_cash_statement_movement
                    WHERE batch_id = :batchId
                    ORDER BY id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerCashStatementMovementRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Long insertIgnore(InsertCommand cmd) {
        var kh = new GeneratedKeyHolder();
        jdbc.update(
                INSERT_IGNORE,
                new MapSqlParameterSource()
                        .addValue("batchId", cmd.batchId())
                        .addValue("brokerId", cmd.brokerId())
                        .addValue("brokerMovementId", cmd.brokerMovementId())
                        .addValue("movementType", cmd.movementType())
                        .addValue("executionRef", cmd.executionRef())
                        .addValue("amount", cmd.amount())
                        .addValue("currency", cmd.currency())
                        .addValue("valueDate", cmd.valueDate() == null ? null : Date.valueOf(cmd.valueDate()))
                        .addValue("rawRowJson", cmd.rawRowJson()),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        return key == null ? null : key.longValue();
    }

    public List<MovementRow> listByBatch(long batchId) {
        return jdbc.query(LIST_BY_BATCH, new MapSqlParameterSource("batchId", batchId), (rs, rowNum) ->
                new MovementRow(
                        rs.getLong("id"),
                        rs.getLong("batch_id"),
                        rs.getString("broker_id"),
                        rs.getString("broker_movement_id"),
                        rs.getString("movement_type"),
                        rs.getString("execution_ref"),
                        rs.getBigDecimal("amount"),
                        rs.getString("currency"),
                        rs.getDate("value_date") == null
                                ? null
                                : rs.getDate("value_date").toLocalDate()));
    }

    public record InsertCommand(
            long batchId,
            String brokerId,
            String brokerMovementId,
            String movementType,
            String executionRef,
            BigDecimal amount,
            String currency,
            LocalDate valueDate,
            String rawRowJson) {}
}
