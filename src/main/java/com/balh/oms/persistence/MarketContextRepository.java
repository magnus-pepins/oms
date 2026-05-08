package com.balh.oms.persistence;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.UUID;

/**
 * Best-ex evidence: {@code market_context.snapshot_json} merges a config stub with venue-attested fields from each
 * applied trade (slice 5 prep; venue ER fields today, NBBO/marketdata later).
 */
@Repository
public class MarketContextRepository {

    private static final String UPSERT_STUB_SQL = """
            INSERT INTO market_context (order_id, snapshot_json)
            VALUES (:order_id, CAST(:snapshot AS JSONB))
            ON CONFLICT (order_id) DO NOTHING
            """;

    private static final String UPSERT_MERGE_VENUE_FILL_SQL = """
            INSERT INTO market_context (order_id, decided_at, snapshot_json)
            VALUES (
                :order_id,
                :decided_at,
                COALESCE(CAST(:stub AS JSONB), '{}'::JSONB) || CAST(:patch AS JSONB)
            )
            ON CONFLICT (order_id) DO UPDATE SET
                snapshot_json = market_context.snapshot_json || CAST(:patch AS JSONB),
                decided_at = GREATEST(market_context.decided_at, EXCLUDED.decided_at)
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public MarketContextRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Insert stub-only row, or no-op if the row exists (e.g. venue reject before any trade).
     */
    public void ensureStubSnapshot(UUID orderId, String snapshotJson) {
        jdbc.update(
                UPSERT_STUB_SQL,
                new MapSqlParameterSource()
                        .addValue("order_id", orderId)
                        .addValue("snapshot", snapshotJson));
    }

    /**
     * Merges {@code stubJson} (first insert only) and {@code evidencePatchJson} (every apply) into {@code snapshot_json};
     * bumps {@code decided_at} to the latest venue timestamp applied.
     */
    public void mergeVenueFillEvidence(UUID orderId, Instant decidedAt, String stubJson, String evidencePatchJson) {
        jdbc.update(
                UPSERT_MERGE_VENUE_FILL_SQL,
                new MapSqlParameterSource()
                        .addValue("order_id", orderId)
                        .addValue("decided_at", java.sql.Timestamp.from(decidedAt))
                        .addValue("stub", stubJson == null || stubJson.isBlank() ? "{}" : stubJson)
                        .addValue("patch", evidencePatchJson));
    }
}
