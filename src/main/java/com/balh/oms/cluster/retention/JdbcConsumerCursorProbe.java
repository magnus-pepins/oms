package com.balh.oms.cluster.retention;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Reads the minimum projector cursor from Postgres (optional purge-floor input for the events
 * recordings). The OMS {@code aeron_projector_cursor} schema is recording-id qualified (V55), so
 * the cursor unambiguously names which events recording the position belongs to. Rows with a NULL
 * {@code last_applied_recording_id} are the V55 poison value (pre-migration ambiguity) and map to
 * empty — the regulator then skips events purging entirely.
 */
public final class JdbcConsumerCursorProbe implements AutoCloseable {

    /** Consumer cursor; {@code recordingId} is null when the cursor schema does not track it. */
    public record ConsumerCursor(Long recordingId, long position) {}

    private static final String CURSOR_SQL =
            "SELECT last_applied_recording_id AS rec_id, last_applied_position AS pos"
                    + " FROM aeron_projector_cursor WHERE projector_id = ?"
                    + " ORDER BY last_applied_recording_id ASC NULLS FIRST, last_applied_position ASC"
                    + " LIMIT 1";

    private final Connection connection;
    private final String projectorId;

    public JdbcConsumerCursorProbe(String jdbcUrl, String user, String password, String projectorId)
            throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl, user, password);
        this.projectorId = projectorId;
    }

    public Optional<ConsumerCursor> minConsumerCursor() throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(CURSOR_SQL)) {
            stmt.setString(1, projectorId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                long recordingId = rs.getLong("rec_id");
                boolean recordingIdIsNull = rs.wasNull();
                long position = rs.getLong("pos");
                if (recordingIdIsNull) {
                    return Optional.empty();
                }
                return Optional.of(new ConsumerCursor(recordingId, position));
            }
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
