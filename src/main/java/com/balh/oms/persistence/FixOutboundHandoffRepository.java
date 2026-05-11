package com.balh.oms.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * Postgres-backed queue for FIX outbound: producers {@link #enqueue(UUID)}; consumer
 * {@link #popNextOrderId()} removes one row using {@code FOR UPDATE SKIP LOCKED} so only one
 * session wins each row (multiple control replicas may enqueue; one FIX drain path consumes).
 */
@Repository
public class FixOutboundHandoffRepository {

    private static final String ENQUEUE_SQL = """
            INSERT INTO fix_outbound_handoff (order_id)
            VALUES (:order_id)
            ON CONFLICT (order_id) DO NOTHING
            """;

    private static final String POP_SQL = """
            DELETE FROM fix_outbound_handoff f
            USING (
                SELECT id FROM fix_outbound_handoff
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            ) AS c
            WHERE f.id = c.id
            RETURNING f.order_id
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixOutboundHandoffRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public int enqueue(UUID orderId) {
        var params = new MapSqlParameterSource().addValue("order_id", orderId);
        return jdbc.update(ENQUEUE_SQL, params);
    }

    /**
     * Removes and returns the next pending order id, or empty if none. Caller must run inside a transaction
     * so {@code FOR UPDATE SKIP LOCKED} is scoped correctly (see {@link com.balh.oms.fix.PostgresFixOutboundOrderDequeue}).
     */
    public Optional<UUID> popNextOrderId() {
        return jdbc.query(
                POP_SQL,
                new MapSqlParameterSource(),
                rs -> {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of((UUID) rs.getObject("order_id"));
                });
    }
}
