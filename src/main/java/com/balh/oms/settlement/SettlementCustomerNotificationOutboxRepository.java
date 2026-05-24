package com.balh.oms.settlement;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SettlementCustomerNotificationOutboxRepository {

    private static final int MAX_STORED_ERROR_CHARS = 4000;

    private static final String INSERT =
            """
                    INSERT INTO settlement_customer_notification_outbox (
                        notification_type, account_id, execution_id, fail_row_id, cash_impact_id, envelope_json
                    ) VALUES (
                        :type, :accountId, :executionId, :failRowId, :cashImpactId, CAST(:envelope AS JSONB)
                    )
                    """;

    private static final String INSERT_CA_IGNORE =
            """
                    INSERT INTO settlement_customer_notification_outbox (
                        notification_type, account_id, cash_impact_id, envelope_json
                    ) VALUES (
                        :type, :accountId, :cashImpactId, CAST(:envelope AS JSONB)
                    )
                    ON CONFLICT (notification_type, cash_impact_id)
                    WHERE cash_impact_id IS NOT NULL
                    DO NOTHING
                    """;

    private static final String FETCH_PENDING =
            """
                    SELECT id, notification_type, account_id, envelope_json::text AS envelope_json, attempts
                    FROM settlement_customer_notification_outbox
                    WHERE published_at IS NULL
                      AND created_at <= :older_than
                    ORDER BY id
                    LIMIT :batch_size
                    FOR UPDATE SKIP LOCKED
                    """;

    private static final String MARK_PUBLISHED =
            """
                    UPDATE settlement_customer_notification_outbox
                    SET published_at = :published_at
                    WHERE id = :id
                    """;

    private static final String MARK_FAILED =
            """
                    UPDATE settlement_customer_notification_outbox
                    SET attempts = attempts + 1, last_error = :error
                    WHERE id = :id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public SettlementCustomerNotificationOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record OutboxRow(long id, String notificationType, UUID accountId, String envelopeJson, int attempts) {}

    public int insertIgnore(
            String notificationType,
            UUID accountId,
            Long executionId,
            Long failRowId,
            String envelopeJson) {
        return jdbc.update(
                INSERT,
                new MapSqlParameterSource()
                        .addValue("type", notificationType)
                        .addValue("accountId", accountId)
                        .addValue("executionId", executionId)
                        .addValue("failRowId", failRowId)
                        .addValue("cashImpactId", null)
                        .addValue("envelope", envelopeJson));
    }

    /** Idempotent CA dividend row — unique on (notification_type, cash_impact_id). */
    public int insertCorporateActionIgnore(
            String notificationType, UUID accountId, long cashImpactId, String envelopeJson) {
        return jdbc.update(
                INSERT_CA_IGNORE,
                new MapSqlParameterSource()
                        .addValue("type", notificationType)
                        .addValue("accountId", accountId)
                        .addValue("cashImpactId", cashImpactId)
                        .addValue("envelope", envelopeJson));
    }

    public List<OutboxRow> fetchPendingOlderThan(Instant olderThan, int batchSize) {
        return jdbc.query(
                FETCH_PENDING,
                new MapSqlParameterSource()
                        .addValue("older_than", Timestamp.from(olderThan))
                        .addValue("batch_size", batchSize),
                ROW_MAPPER);
    }

    public void markPublished(long id, Instant publishedAt) {
        jdbc.update(
                MARK_PUBLISHED,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("published_at", Timestamp.from(publishedAt)));
    }

    public void markFailed(long id, String error) {
        jdbc.update(
                MARK_FAILED,
                new MapSqlParameterSource().addValue("id", id).addValue("error", truncateError(error)));
    }

    private static String truncateError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_STORED_ERROR_CHARS ? error : error.substring(0, MAX_STORED_ERROR_CHARS);
    }

    private static final RowMapper<OutboxRow> ROW_MAPPER =
            (rs, rowNum) ->
                    new OutboxRow(
                            rs.getLong("id"),
                            rs.getString("notification_type"),
                            UUID.fromString(rs.getString("account_id")),
                            rs.getString("envelope_json"),
                            rs.getInt("attempts"));
}
