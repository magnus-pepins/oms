package com.balh.oms.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Best-ex evidence stub: ensures a {@code market_context} row exists (slice 3).
 */
@Repository
public class MarketContextRepository {

    private static final String UPSERT_STUB_SQL = """
            INSERT INTO market_context (order_id, snapshot_json)
            VALUES (:order_id, CAST(:snapshot AS JSONB))
            ON CONFLICT (order_id) DO NOTHING
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public MarketContextRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void ensureStubSnapshot(UUID orderId, String snapshotJson) {
        jdbc.update(
                UPSERT_STUB_SQL,
                new MapSqlParameterSource()
                        .addValue("order_id", orderId)
                        .addValue("snapshot", snapshotJson));
    }
}
