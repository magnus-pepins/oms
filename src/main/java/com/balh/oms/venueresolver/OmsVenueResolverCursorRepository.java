package com.balh.oms.venueresolver;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/** Cursor for {@link OmsVenueResolverService} replay of venue cluster events. */
@Repository
public class OmsVenueResolverCursorRepository {

    public record RecordedCursor(long recordingId, long position) {}

    private static final String SELECT_CURSOR =
            """
                    SELECT last_applied_recording_id, last_applied_position
                    FROM oms_venue_resolver_cursor
                    WHERE resolver_id = :resolver_id
                      AND venue_events_stream_id = :stream_id
                    """;

    private static final String UPSERT_CURSOR =
            """
                    INSERT INTO oms_venue_resolver_cursor (
                        resolver_id, venue_events_stream_id,
                        last_applied_recording_id, last_applied_position, last_applied_at
                    ) VALUES (
                        :resolver_id, :stream_id, :recording_id, :position, NOW()
                    )
                    ON CONFLICT (resolver_id, venue_events_stream_id) DO UPDATE
                       SET last_applied_recording_id = EXCLUDED.last_applied_recording_id,
                           last_applied_position = EXCLUDED.last_applied_position,
                           last_applied_at = EXCLUDED.last_applied_at
                     WHERE oms_venue_resolver_cursor.last_applied_recording_id IS NULL
                        OR EXCLUDED.last_applied_recording_id > oms_venue_resolver_cursor.last_applied_recording_id
                        OR (
                             EXCLUDED.last_applied_recording_id
                                 = oms_venue_resolver_cursor.last_applied_recording_id
                             AND EXCLUDED.last_applied_position
                                 > oms_venue_resolver_cursor.last_applied_position
                           )
                    """;

    private static final String RESET_CURSOR =
            """
                    INSERT INTO oms_venue_resolver_cursor (
                        resolver_id, venue_events_stream_id,
                        last_applied_recording_id, last_applied_position, last_applied_at
                    ) VALUES (
                        :resolver_id, :stream_id, :recording_id, :position, NOW()
                    )
                    ON CONFLICT (resolver_id, venue_events_stream_id) DO UPDATE
                       SET last_applied_recording_id = EXCLUDED.last_applied_recording_id,
                           last_applied_position = EXCLUDED.last_applied_position,
                           last_applied_at = EXCLUDED.last_applied_at
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public OmsVenueResolverCursorRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<RecordedCursor> findCursor(String resolverId, int venueEventsStreamId) {
        try {
            return Optional.ofNullable(
                    jdbc.queryForObject(
                            SELECT_CURSOR,
                            new MapSqlParameterSource()
                                    .addValue("resolver_id", resolverId)
                                    .addValue("stream_id", venueEventsStreamId),
                            (rs, rowNum) -> {
                                long recordingId = rs.getLong(1);
                                if (rs.wasNull()) {
                                    return null;
                                }
                                return new RecordedCursor(recordingId, rs.getLong(2));
                            }));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public void resetCursor(String resolverId, int venueEventsStreamId, long recordingId, long position) {
        jdbc.update(
                RESET_CURSOR,
                new MapSqlParameterSource()
                        .addValue("resolver_id", resolverId)
                        .addValue("stream_id", venueEventsStreamId)
                        .addValue("recording_id", recordingId)
                        .addValue("position", position));
    }

    public void advance(String resolverId, int venueEventsStreamId, long recordingId, long position) {
        jdbc.update(
                UPSERT_CURSOR,
                new MapSqlParameterSource()
                        .addValue("resolver_id", resolverId)
                        .addValue("stream_id", venueEventsStreamId)
                        .addValue("recording_id", recordingId)
                        .addValue("position", position));
    }
}
