package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Phase B: venue contract resolution header row + dispute window. */
@Repository
public class VenueContractResolutionRepository {

    private static final String INSERT =
            """
                    INSERT INTO venue_contract_resolution (
                        contract_symbol, outcome, resolution_source, resolution_timestamp,
                        evidence_hash, venue_id, dispute_until, posting_paused, orders_resolved_count
                    ) VALUES (
                        :symbol, :outcome, :source, :resolvedAt, :hash, :venueId,
                        :disputeUntil, FALSE, :ordersResolved
                    )
                    ON CONFLICT (contract_symbol, evidence_hash) DO NOTHING
                    RETURNING id
                    """;

    private static final String FIND_ID =
            """
                    SELECT id FROM venue_contract_resolution
                    WHERE contract_symbol = :symbol AND evidence_hash = :hash
                    """;

    private static final String SET_POSTING_PAUSED =
            """
                    UPDATE venue_contract_resolution
                    SET posting_paused = :paused
                    WHERE id = :id
                    """;

    private static final String EXISTS_BY_ID =
            "SELECT EXISTS(SELECT 1 FROM venue_contract_resolution WHERE id = :id)";

    private static final String LIST_RECENT =
            """
                    SELECT r.id,
                           r.contract_symbol,
                           r.outcome,
                           r.resolution_source,
                           r.resolution_timestamp,
                           r.evidence_hash,
                           r.venue_id,
                           r.dispute_until,
                           r.posting_paused,
                           r.orders_resolved_count,
                           r.created_at,
                           COUNT(o.id) FILTER (WHERE o.posted_at IS NOT NULL)     AS posted_legs,
                           COUNT(o.id) FILTER (WHERE o.posted_at IS NULL
                               AND o.skipped_at IS NULL)                         AS pending_legs,
                           COUNT(o.id) FILTER (WHERE o.skipped_at IS NOT NULL) AS skipped_legs
                    FROM venue_contract_resolution r
                    LEFT JOIN prediction_market_ledger_outbox o ON o.resolution_id = r.id
                    GROUP BY r.id
                    ORDER BY r.created_at DESC
                    LIMIT :lim
                    """;

    private static final String LIST_RECENT_BY_SYMBOL =
            """
                    SELECT r.id,
                           r.contract_symbol,
                           r.outcome,
                           r.resolution_source,
                           r.resolution_timestamp,
                           r.evidence_hash,
                           r.venue_id,
                           r.dispute_until,
                           r.posting_paused,
                           r.orders_resolved_count,
                           r.created_at,
                           COUNT(o.id) FILTER (WHERE o.posted_at IS NOT NULL)     AS posted_legs,
                           COUNT(o.id) FILTER (WHERE o.posted_at IS NULL
                               AND o.skipped_at IS NULL)                         AS pending_legs,
                           COUNT(o.id) FILTER (WHERE o.skipped_at IS NOT NULL) AS skipped_legs
                    FROM venue_contract_resolution r
                    LEFT JOIN prediction_market_ledger_outbox o ON o.resolution_id = r.id
                    WHERE r.contract_symbol = :symbol
                    GROUP BY r.id
                    ORDER BY r.created_at DESC
                    LIMIT :lim
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public VenueContractResolutionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Long> insertIgnoreReturningId(
            String contractSymbol,
            String outcome,
            String resolutionSource,
            Instant resolutionTimestamp,
            String evidenceHash,
            String venueId,
            Instant disputeUntil,
            int ordersResolvedCount) {
        List<Long> ids =
                jdbc.query(
                        INSERT,
                        new MapSqlParameterSource()
                                .addValue("symbol", contractSymbol)
                                .addValue("outcome", outcome)
                                .addValue("source", resolutionSource)
                                .addValue("resolvedAt", Timestamp.from(resolutionTimestamp))
                                .addValue("hash", evidenceHash)
                                .addValue("venueId", venueId)
                                .addValue("disputeUntil", Timestamp.from(disputeUntil))
                                .addValue("ordersResolved", ordersResolvedCount),
                        (rs, rowNum) -> rs.getLong("id"));
        if (!ids.isEmpty()) {
            return Optional.of(ids.getFirst());
        }
        return findId(contractSymbol, evidenceHash);
    }

    public Optional<Long> findId(String contractSymbol, String evidenceHash) {
        List<Long> ids =
                jdbc.query(
                        FIND_ID,
                        new MapSqlParameterSource()
                                .addValue("symbol", contractSymbol)
                                .addValue("hash", evidenceHash),
                        (rs, rowNum) -> rs.getLong("id"));
        return ids.isEmpty() ? Optional.empty() : Optional.of(ids.getFirst());
    }

    public boolean existsById(long resolutionId) {
        Boolean exists =
                jdbc.queryForObject(
                        EXISTS_BY_ID,
                        new MapSqlParameterSource("id", resolutionId),
                        Boolean.class);
        return Boolean.TRUE.equals(exists);
    }

    public boolean setPostingPaused(long resolutionId, boolean paused) {
        return jdbc.update(
                        SET_POSTING_PAUSED,
                        new MapSqlParameterSource().addValue("id", resolutionId).addValue("paused", paused))
                > 0;
    }

    public record ResolutionListRow(
            long id,
            String contractSymbol,
            String outcome,
            String resolutionSource,
            Instant resolutionTimestamp,
            String evidenceHash,
            String venueId,
            Instant disputeUntil,
            boolean postingPaused,
            int ordersResolvedCount,
            Instant createdAt,
            int postedLegs,
            int pendingLegs,
            int skippedLegs) {}

    public List<ResolutionListRow> listRecent(int limit, String contractSymbol) {
        String symbol =
                contractSymbol == null || contractSymbol.isBlank()
                        ? null
                        : contractSymbol.trim().toUpperCase();
        int lim = Math.max(1, Math.min(limit, 200));
        String sql = symbol == null ? LIST_RECENT : LIST_RECENT_BY_SYMBOL;
        MapSqlParameterSource params = new MapSqlParameterSource("lim", lim);
        if (symbol != null) {
            params.addValue("symbol", symbol);
        }
        return jdbc.query(
                sql,
                params,
                (rs, rowNum) ->
                        new ResolutionListRow(
                                rs.getLong("id"),
                                rs.getString("contract_symbol"),
                                rs.getString("outcome"),
                                rs.getString("resolution_source"),
                                rs.getTimestamp("resolution_timestamp").toInstant(),
                                rs.getString("evidence_hash"),
                                rs.getString("venue_id"),
                                rs.getTimestamp("dispute_until").toInstant(),
                                rs.getBoolean("posting_paused"),
                                rs.getInt("orders_resolved_count"),
                                rs.getTimestamp("created_at").toInstant(),
                                rs.getInt("posted_legs"),
                                rs.getInt("pending_legs"),
                                rs.getInt("skipped_legs")));
    }
}
