package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Phase B: Ledger payout legs for {@code prediction_market_binary_resolution}. */
@Repository
public class PredictionMarketLedgerOutboxRepository {

    public static final String LEG_PREDICTION_PAYOUT = "prediction-payout";

    public record OutboxRow(
            long id,
            long resolutionId,
            UUID accountId,
            String legKind,
            String payloadJson,
            int attempts) {}

    private static final String INSERT =
            """
                    INSERT INTO prediction_market_ledger_outbox (resolution_id, account_id, leg_kind, payload_json)
                    VALUES (:resolutionId, :accountId, :legKind, CAST(:payload AS JSONB))
                    ON CONFLICT (resolution_id, account_id, leg_kind) DO NOTHING
                    """;

    private static final String LOCK_ELIGIBLE =
            """
                    SELECT o.id, o.resolution_id, o.account_id, o.leg_kind, o.payload_json::text AS payload_json,
                           o.attempts
                    FROM prediction_market_ledger_outbox o
                    JOIN venue_contract_resolution r ON r.id = o.resolution_id
                    WHERE o.posted_at IS NULL
                      AND o.skipped_at IS NULL
                      AND r.posting_paused = FALSE
                      AND r.dispute_until <= :now
                    ORDER BY o.id
                    LIMIT :lim
                    FOR UPDATE SKIP LOCKED
                    """;

    private static final String MARK_POSTED =
            "UPDATE prediction_market_ledger_outbox SET posted_at = :ts WHERE id = :id";

    private static final String MARK_FAILED =
            """
                    UPDATE prediction_market_ledger_outbox
                    SET attempts = attempts + 1,
                        last_error_text = :err,
                        last_attempt_at = :ts
                    WHERE id = :id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public PredictionMarketLedgerOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public int insertIgnore(long resolutionId, UUID accountId, String legKind, String payloadJson) {
        return jdbc.update(
                INSERT,
                new MapSqlParameterSource()
                        .addValue("resolutionId", resolutionId)
                        .addValue("accountId", accountId)
                        .addValue("legKind", legKind)
                        .addValue("payload", payloadJson));
    }

    public List<OutboxRow> lockEligible(Instant now, int limit) {
        return jdbc.query(
                LOCK_ELIGIBLE,
                new MapSqlParameterSource().addValue("now", Timestamp.from(now)).addValue("lim", limit),
                (rs, rowNum) ->
                        new OutboxRow(
                                rs.getLong("id"),
                                rs.getLong("resolution_id"),
                                (UUID) rs.getObject("account_id"),
                                rs.getString("leg_kind"),
                                rs.getString("payload_json"),
                                rs.getInt("attempts")));
    }

    public void markPosted(long id, Instant ts) {
        jdbc.update(MARK_POSTED, new MapSqlParameterSource("id", id).addValue("ts", Timestamp.from(ts)));
    }

    public void recordAttempt(long id, String error, Instant ts) {
        jdbc.update(
                MARK_FAILED,
                new MapSqlParameterSource().addValue("id", id).addValue("err", error).addValue("ts", Timestamp.from(ts)));
    }
}
