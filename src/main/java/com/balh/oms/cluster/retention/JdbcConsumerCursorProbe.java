package com.balh.oms.cluster.retention;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.OptionalLong;

/** Reads minimum projector cursor position from Postgres (optional purge floor input). */
public final class JdbcConsumerCursorProbe implements AutoCloseable {

    private static final String CURSOR_SQL =
            "SELECT MIN(last_applied_position) AS min_pos FROM aeron_projector_cursor WHERE projector_id = ?";

    private final Connection connection;
    private final String projectorId;

    public JdbcConsumerCursorProbe(String jdbcUrl, String user, String password, String projectorId)
            throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl, user, password);
        this.projectorId = projectorId;
    }

    public OptionalLong minConsumerPosition() throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(CURSOR_SQL)) {
            stmt.setString(1, projectorId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return OptionalLong.empty();
                }
                long value = rs.getLong("min_pos");
                if (rs.wasNull()) {
                    return OptionalLong.empty();
                }
                return OptionalLong.of(value);
            }
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
