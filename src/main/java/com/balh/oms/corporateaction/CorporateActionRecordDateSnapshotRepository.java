package com.balh.oms.corporateaction;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class CorporateActionRecordDateSnapshotRepository {

    public record SnapshotHolder(UUID accountId, BigDecimal quantitySettled) {}

    private static final String UPSERT =
            """
                    INSERT INTO corporate_action_record_date_snapshot (
                        corporate_action_event_id, account_id, instrument_symbol,
                        quantity_settled, record_date, snapshot_source
                    ) VALUES (
                        :eventId, :accountId, :symbol, :qty, CAST(:recordDate AS DATE), :source
                    )
                    ON CONFLICT (corporate_action_event_id, account_id) DO UPDATE SET
                        quantity_settled = EXCLUDED.quantity_settled,
                        instrument_symbol = EXCLUDED.instrument_symbol,
                        record_date = EXCLUDED.record_date,
                        snapshot_source = EXCLUDED.snapshot_source,
                        captured_at = NOW()
                    """;

    private static final String LIST_BY_EVENT =
            """
                    SELECT account_id, quantity_settled
                    FROM corporate_action_record_date_snapshot
                    WHERE corporate_action_event_id = :eventId
                      AND quantity_settled > 0
                    ORDER BY account_id
                    """;

    private static final String EXISTS_FOR_EVENT =
            """
                    SELECT EXISTS(
                        SELECT 1 FROM corporate_action_record_date_snapshot
                        WHERE corporate_action_event_id = :eventId
                    )
                    """;

    private static final String SELECT_EVENTS_NEEDING_CAPTURE =
            """
                    SELECT id, instrument_symbol, record_date
                    FROM corporate_action_event
                    WHERE record_date = CAST(:recordDate AS DATE)
                      AND processed_at IS NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM corporate_action_record_date_snapshot s
                          WHERE s.corporate_action_event_id = corporate_action_event.id
                      )
                    ORDER BY id ASC
                    LIMIT :limit
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public CorporateActionRecordDateSnapshotRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void upsert(
            long eventId,
            UUID accountId,
            String instrumentSymbol,
            BigDecimal quantitySettled,
            LocalDate recordDate,
            String snapshotSource) {
        jdbc.update(
                UPSERT,
                new MapSqlParameterSource()
                        .addValue("eventId", eventId)
                        .addValue("accountId", accountId)
                        .addValue("symbol", instrumentSymbol)
                        .addValue("qty", quantitySettled)
                        .addValue("recordDate", recordDate.toString())
                        .addValue("source", snapshotSource));
    }

    public List<SnapshotHolder> listByEvent(long eventId) {
        return jdbc.query(
                LIST_BY_EVENT,
                new MapSqlParameterSource("eventId", eventId),
                (rs, rowNum) ->
                        new SnapshotHolder(
                                (UUID) rs.getObject("account_id"), rs.getBigDecimal("quantity_settled")));
    }

    public boolean existsForEvent(long eventId) {
        Boolean exists =
                jdbc.queryForObject(
                        EXISTS_FOR_EVENT, new MapSqlParameterSource("eventId", eventId), Boolean.class);
        return Boolean.TRUE.equals(exists);
    }

    public List<EventCaptureRow> listEventsNeedingCapture(LocalDate recordDate, int limit) {
        return jdbc.query(
                SELECT_EVENTS_NEEDING_CAPTURE,
                new MapSqlParameterSource()
                        .addValue("recordDate", recordDate.toString())
                        .addValue("limit", limit),
                (rs, rowNum) ->
                        new EventCaptureRow(
                                rs.getLong("id"),
                                rs.getString("instrument_symbol"),
                                rs.getDate("record_date").toLocalDate()));
    }

    public record EventCaptureRow(long eventId, String instrumentSymbol, LocalDate recordDate) {}
}
