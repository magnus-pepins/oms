package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Persistence for {@code broker_confirm_batch} (gap plan §5.1 / Flyway V54).
 *
 * <p>Idempotency: the ingest service relies on file-byte SHA-256 and on the
 * broker-provided {@code (broker_id, broker_file_id)} key. ON CONFLICT DO NOTHING
 * (no target) lets either unique index reject the row; the service then looks the
 * existing row up by SHA first, then by {@code (broker_id, broker_file_id)}.
 */
@Repository
public class BrokerConfirmBatchRepository {

    public record BatchRow(
            long id,
            String brokerId,
            String brokerFileId,
            LocalDate businessDate,
            int schemaVersion,
            String fileSha256Hex,
            String source,
            String fileName,
            Instant generatedAt,
            Instant receivedAt,
            Instant parsedAt,
            Instant appliedAt,
            String status,
            Integer rowCount,
            Integer matchedRowCount,
            Integer breakRowCount,
            String errorSummary) {}

    private static final String INSERT_RECEIVED =
            """
                    INSERT INTO broker_confirm_batch (
                        broker_id, broker_file_id, business_date, schema_version,
                        file_sha256_hex, source, file_name, generated_at, status
                    ) VALUES (
                        :brokerId, :brokerFileId, :businessDate, :schemaVersion,
                        :sha, :source, :fileName, :generatedAt, 'received'
                    )
                    ON CONFLICT DO NOTHING
                    """;

    private static final String COMMON_SELECT =
            """
                    SELECT id, broker_id, broker_file_id, business_date, schema_version,
                           file_sha256_hex, source, file_name, generated_at, received_at,
                           parsed_at, applied_at,
                           status, row_count, matched_row_count, break_row_count, error_summary
                    FROM broker_confirm_batch
                    """;

    private static final String SELECT_BY_SHA = COMMON_SELECT + " WHERE file_sha256_hex = :sha LIMIT 1";

    private static final String SELECT_BY_BROKER_FILE =
            COMMON_SELECT + " WHERE broker_id = :brokerId AND broker_file_id = :brokerFileId LIMIT 1";

    private static final String SELECT_BY_ID = COMMON_SELECT + " WHERE id = :id LIMIT 1";

    private static final String LIST_RECENT =
            COMMON_SELECT + " ORDER BY received_at DESC, id DESC LIMIT :lim OFFSET :off";

    private static final String UPDATE_MATCH_OUTCOMES =
            """
                    UPDATE broker_confirm_batch
                    SET matched_row_count = :matched,
                        break_row_count = :breaks
                    WHERE id = :id
                    """;

    private static final String UPDATE_STATUS =
            """
                    UPDATE broker_confirm_batch
                    SET status = :st,
                        row_count = COALESCE(:rc, row_count),
                        parsed_at = CASE WHEN :st IN ('parsed','matching','applied') AND parsed_at IS NULL
                                         THEN NOW() ELSE parsed_at END,
                        applied_at = CASE WHEN :st = 'applied' AND applied_at IS NULL
                                          THEN NOW() ELSE applied_at END,
                        error_summary = :es
                    WHERE id = :id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerConfirmBatchRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return new batch id when inserted, empty when either unique index rejected the row
     */
    public Optional<Long> insertReceivedIfNew(
            String brokerId,
            String brokerFileId,
            LocalDate businessDate,
            int schemaVersion,
            String fileSha256Hex,
            String source,
            String fileName,
            Instant generatedAt) {
        var params =
                new MapSqlParameterSource()
                        .addValue("brokerId", brokerId)
                        .addValue("brokerFileId", brokerFileId)
                        .addValue("businessDate", java.sql.Date.valueOf(businessDate))
                        .addValue("schemaVersion", schemaVersion)
                        .addValue("sha", fileSha256Hex)
                        .addValue("source", source)
                        .addValue("fileName", fileName)
                        .addValue("generatedAt", generatedAt == null ? null : Timestamp.from(generatedAt));
        var kh = new GeneratedKeyHolder();
        jdbc.update(INSERT_RECEIVED, params, kh, new String[] {"id"});
        Number key = kh.getKey();
        return key == null ? Optional.empty() : Optional.of(key.longValue());
    }

    public Optional<BatchRow> findBySha256Hex(String sha256Hex) {
        return queryOne(SELECT_BY_SHA, new MapSqlParameterSource("sha", sha256Hex));
    }

    public Optional<BatchRow> findByBrokerFile(String brokerId, String brokerFileId) {
        return queryOne(
                SELECT_BY_BROKER_FILE,
                new MapSqlParameterSource().addValue("brokerId", brokerId).addValue("brokerFileId", brokerFileId));
    }

    public Optional<BatchRow> findById(long id) {
        return queryOne(SELECT_BY_ID, new MapSqlParameterSource("id", id));
    }

    public List<BatchRow> listRecent(int limit, int offset) {
        return jdbc.query(
                LIST_RECENT,
                new MapSqlParameterSource().addValue("lim", limit).addValue("off", offset),
                BATCH_ROW_MAPPER);
    }

    public List<Long> findIdsByStatus(String status, int limit) {
        return jdbc.query(
                "SELECT id FROM broker_confirm_batch WHERE status = :status ORDER BY received_at, id LIMIT :lim",
                new MapSqlParameterSource().addValue("status", status).addValue("lim", limit),
                (rs, i) -> rs.getLong("id"));
    }

    public void updateMatchOutcomes(long id, int matchedRowCount, int breakRowCount) {
        jdbc.update(
                UPDATE_MATCH_OUTCOMES,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("matched", matchedRowCount)
                        .addValue("breaks", breakRowCount));
    }

    public void updateStatus(long id, String status, Integer rowCount, String errorSummary) {
        jdbc.update(
                UPDATE_STATUS,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("st", status)
                        .addValue("rc", rowCount)
                        .addValue("es", errorSummary));
    }

    private Optional<BatchRow> queryOne(String sql, MapSqlParameterSource params) {
        List<BatchRow> rows = jdbc.query(sql, params, BATCH_ROW_MAPPER);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    private static final org.springframework.jdbc.core.RowMapper<BatchRow> BATCH_ROW_MAPPER = (rs, i) -> {
        Timestamp generatedAt = rs.getTimestamp("generated_at");
        Timestamp parsedAt = rs.getTimestamp("parsed_at");
        Timestamp appliedAt = rs.getTimestamp("applied_at");
        return new BatchRow(
                rs.getLong("id"),
                rs.getString("broker_id"),
                rs.getString("broker_file_id"),
                rs.getDate("business_date").toLocalDate(),
                rs.getInt("schema_version"),
                rs.getString("file_sha256_hex"),
                rs.getString("source"),
                rs.getString("file_name"),
                generatedAt == null ? null : generatedAt.toInstant(),
                rs.getTimestamp("received_at").toInstant(),
                parsedAt == null ? null : parsedAt.toInstant(),
                appliedAt == null ? null : appliedAt.toInstant(),
                rs.getString("status"),
                (Integer) rs.getObject("row_count"),
                (Integer) rs.getObject("matched_row_count"),
                (Integer) rs.getObject("break_row_count"),
                rs.getString("error_summary"));
    };
}
