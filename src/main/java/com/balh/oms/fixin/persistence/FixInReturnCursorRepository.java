package com.balh.oms.fixin.persistence;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class FixInReturnCursorRepository {

    public record RecordedCursor(long recordingId, long position) {}

    private static final String SELECT = """
            SELECT last_applied_recording_id, last_applied_position
              FROM oms_fix_in_return_cursor
             WHERE egress_id = :egress_id
            """;

    private static final String UPSERT = """
            INSERT INTO oms_fix_in_return_cursor (
                egress_id, replay_stream_id, last_applied_recording_id, last_applied_position, updated_at)
            VALUES (:egress_id, :replay_stream_id, :recording_id, :position, NOW())
            ON CONFLICT (egress_id) DO UPDATE
               SET last_applied_recording_id = EXCLUDED.last_applied_recording_id,
                   last_applied_position = EXCLUDED.last_applied_position,
                   updated_at = NOW()
             WHERE oms_fix_in_return_cursor.last_applied_recording_id IS NULL
                OR EXCLUDED.last_applied_recording_id > oms_fix_in_return_cursor.last_applied_recording_id
                OR (EXCLUDED.last_applied_recording_id = oms_fix_in_return_cursor.last_applied_recording_id
                    AND EXCLUDED.last_applied_position > oms_fix_in_return_cursor.last_applied_position)
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixInReturnCursorRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<RecordedCursor> find(String egressId) {
        try {
            return Optional.of(jdbc.queryForObject(
                    SELECT,
                    new MapSqlParameterSource("egress_id", egressId),
                    (rs, rowNum) -> new RecordedCursor(
                            rs.getLong("last_applied_recording_id"),
                            rs.getLong("last_applied_position"))));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public void advanceWithRecording(String egressId, int replayStreamId, long recordingId, long position) {
        jdbc.update(
                UPSERT,
                new MapSqlParameterSource()
                        .addValue("egress_id", egressId)
                        .addValue("replay_stream_id", replayStreamId)
                        .addValue("recording_id", recordingId)
                        .addValue("position", position));
    }
}
