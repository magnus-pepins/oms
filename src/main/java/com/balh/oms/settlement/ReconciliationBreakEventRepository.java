package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public class ReconciliationBreakEventRepository {

    public record EventRow(long id, long breakId, String eventType, String actor, String payloadJson, Instant createdAt) {}

    private static final String INSERT =
            """
                    INSERT INTO reconciliation_break_events (break_id, event_type, actor, payload_json)
                    VALUES (:breakId, :eventType, :actor, CAST(:payloadJson AS JSONB))
                    """;

    private static final String LIST_BY_BREAK =
            """
                    SELECT id, break_id, event_type, actor, payload_json::text AS payload_json, created_at
                    FROM reconciliation_break_events
                    WHERE break_id = :breakId
                    ORDER BY created_at ASC, id ASC
                    LIMIT :lim
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public ReconciliationBreakEventRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(long breakId, String eventType, String actor, String payloadJson) {
        jdbc.update(
                INSERT,
                new MapSqlParameterSource()
                        .addValue("breakId", breakId)
                        .addValue("eventType", eventType)
                        .addValue("actor", actor)
                        .addValue("payloadJson", payloadJson == null ? "{}" : payloadJson));
    }

    public List<EventRow> listByBreakId(long breakId, int limit) {
        return jdbc.query(
                LIST_BY_BREAK,
                new MapSqlParameterSource().addValue("breakId", breakId).addValue("lim", limit),
                (rs, rowNum) ->
                        new EventRow(
                                rs.getLong("id"),
                                rs.getLong("break_id"),
                                rs.getString("event_type"),
                                rs.getString("actor"),
                                rs.getString("payload_json"),
                                rs.getTimestamp("created_at").toInstant()));
    }
}
