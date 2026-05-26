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
}
