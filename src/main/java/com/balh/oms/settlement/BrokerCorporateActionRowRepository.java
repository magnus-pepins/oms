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
public class BrokerCorporateActionRowRepository {

    public record NoticeRow(
            long id,
            long batchId,
            String brokerId,
            String brokerEventId,
            String instrumentSymbol,
            String actionType,
            LocalDate effectiveDate,
            String rawRowJson,
            Long corporateActionEventId,
            Instant appliedAt,
            String applyError) {}

    private static final String INSERT_IGNORE =
            """
                    INSERT INTO broker_corporate_action_row (
                        batch_id, broker_id, broker_event_id, instrument_symbol, action_type,
                        effective_date, raw_row_json
                    ) VALUES (
                        :batchId, :brokerId, :brokerEventId, :instrumentSymbol, :actionType,
                        :effectiveDate, CAST(:rawRowJson AS JSONB)
                    )
                    ON CONFLICT DO NOTHING
                    """;

    private static final String LIST_BY_BATCH =
            """
                    SELECT id, batch_id, broker_id, broker_event_id, instrument_symbol, action_type,
                           effective_date, raw_row_json::text AS raw_row_json,
                           corporate_action_event_id, applied_at, apply_error
                    FROM broker_corporate_action_row
                    WHERE batch_id = :batchId
                    ORDER BY id
                    """;

    private static final String MARK_APPLY =
            """
                    UPDATE broker_corporate_action_row
                    SET corporate_action_event_id = :eventId,
                        applied_at = NOW(),
                        apply_error = :applyError
                    WHERE id = :id AND applied_at IS NULL
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerCorporateActionRowRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Long insertIgnore(InsertCommand cmd) {
        var kh = new GeneratedKeyHolder();
        jdbc.update(
                INSERT_IGNORE,
                new MapSqlParameterSource()
                        .addValue("batchId", cmd.batchId())
                        .addValue("brokerId", cmd.brokerId())
                        .addValue("brokerEventId", cmd.brokerEventId())
                        .addValue("instrumentSymbol", cmd.instrumentSymbol())
                        .addValue("actionType", cmd.actionType())
                        .addValue("effectiveDate", Date.valueOf(cmd.effectiveDate()))
                        .addValue("rawRowJson", cmd.rawRowJson()),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        return key == null ? null : key.longValue();
    }

    public List<NoticeRow> listByBatch(long batchId) {
        return jdbc.query(LIST_BY_BATCH, new MapSqlParameterSource("batchId", batchId), (rs, rowNum) -> {
            java.sql.Timestamp applied = rs.getTimestamp("applied_at");
            return new NoticeRow(
                    rs.getLong("id"),
                    rs.getLong("batch_id"),
                    rs.getString("broker_id"),
                    rs.getString("broker_event_id"),
                    rs.getString("instrument_symbol"),
                    rs.getString("action_type"),
                    rs.getDate("effective_date").toLocalDate(),
                    rs.getString("raw_row_json"),
                    (Long) rs.getObject("corporate_action_event_id"),
                    applied == null ? null : applied.toInstant(),
                    rs.getString("apply_error"));
        });
    }

    public void markApplyResult(long rowId, Long corporateActionEventId, String applyError) {
        jdbc.update(
                MARK_APPLY,
                new MapSqlParameterSource()
                        .addValue("id", rowId)
                        .addValue("eventId", corporateActionEventId)
                        .addValue("applyError", applyError));
    }

    public record InsertCommand(
            long batchId,
            String brokerId,
            String brokerEventId,
            String instrumentSymbol,
            String actionType,
            LocalDate effectiveDate,
            String rawRowJson) {}
}
