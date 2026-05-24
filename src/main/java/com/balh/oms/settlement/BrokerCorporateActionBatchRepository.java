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

@Repository
public class BrokerCorporateActionBatchRepository {

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
            String status,
            Integer eventCount,
            String errorSummary) {}

    private static final String INSERT_RECEIVED =
            """
                    INSERT INTO broker_corporate_action_batch (
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
                           parsed_at, status, event_count, error_summary
                    FROM broker_corporate_action_batch
                    """;

    private static final String UPDATE_STATUS =
            """
                    UPDATE broker_corporate_action_batch
                    SET status = :st,
                        event_count = COALESCE(:ec, event_count),
                        parsed_at = CASE WHEN :st = 'parsed' AND parsed_at IS NULL THEN NOW() ELSE parsed_at END,
                        error_summary = :es
                    WHERE id = :id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerCorporateActionBatchRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Long> insertReceivedIfNew(InsertReceived cmd) {
        var kh = new GeneratedKeyHolder();
        jdbc.update(
                INSERT_RECEIVED,
                new MapSqlParameterSource()
                        .addValue("brokerId", cmd.brokerId())
                        .addValue("brokerFileId", cmd.brokerFileId())
                        .addValue("businessDate", java.sql.Date.valueOf(cmd.businessDate()))
                        .addValue("schemaVersion", cmd.schemaVersion())
                        .addValue("sha", cmd.fileSha256Hex())
                        .addValue("source", cmd.source())
                        .addValue("fileName", cmd.fileName())
                        .addValue("generatedAt", cmd.generatedAt() == null ? null : Timestamp.from(cmd.generatedAt())),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        return key == null ? Optional.empty() : Optional.of(key.longValue());
    }

    public Optional<BatchRow> findBySha256Hex(String sha) {
        return findOne(COMMON_SELECT + " WHERE file_sha256_hex = :sha LIMIT 1", new MapSqlParameterSource("sha", sha));
    }

    public Optional<BatchRow> findByBrokerFile(String brokerId, String brokerFileId) {
        return findOne(
                COMMON_SELECT + " WHERE broker_id = :brokerId AND broker_file_id = :brokerFileId LIMIT 1",
                new MapSqlParameterSource().addValue("brokerId", brokerId).addValue("brokerFileId", brokerFileId));
    }

    public Optional<BatchRow> findById(long id) {
        return findOne(COMMON_SELECT + " WHERE id = :id LIMIT 1", new MapSqlParameterSource("id", id));
    }

    public List<BatchRow> listRecent(int limit, int offset) {
        return jdbc.query(
                COMMON_SELECT + " ORDER BY received_at DESC, id DESC LIMIT :lim OFFSET :off",
                new MapSqlParameterSource().addValue("lim", limit).addValue("off", offset),
                (rs, rowNum) -> mapRow(rs));
    }

    /** Parsed (or applied but not yet reconciled) CA batches in the lookback window. */
    public List<Long> listParsedSince(Instant since, int limit) {
        return jdbc.queryForList(
                """
                        SELECT b.id FROM broker_corporate_action_batch b
                        WHERE b.status IN ('parsed', 'applied')
                          AND b.received_at >= :since
                          AND NOT EXISTS (
                              SELECT 1 FROM corporate_action_reconciliation_report r WHERE r.batch_id = b.id
                          )
                        ORDER BY b.received_at ASC
                        LIMIT :lim
                        """,
                new MapSqlParameterSource().addValue("since", Timestamp.from(since)).addValue("lim", limit),
                Long.class);
    }

    public void updateStatus(long id, String status, Integer eventCount, String errorSummary) {
        jdbc.update(
                UPDATE_STATUS,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("st", status)
                        .addValue("ec", eventCount)
                        .addValue("es", errorSummary));
    }

    private Optional<BatchRow> findOne(String sql, MapSqlParameterSource params) {
        List<BatchRow> rows = jdbc.query(sql, params, (rs, rowNum) -> mapRow(rs));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    private static BatchRow mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        java.sql.Date bd = rs.getDate("business_date");
        Timestamp generated = rs.getTimestamp("generated_at");
        Timestamp received = rs.getTimestamp("received_at");
        Timestamp parsed = rs.getTimestamp("parsed_at");
        return new BatchRow(
                rs.getLong("id"),
                rs.getString("broker_id"),
                rs.getString("broker_file_id"),
                bd.toLocalDate(),
                rs.getInt("schema_version"),
                rs.getString("file_sha256_hex"),
                rs.getString("source"),
                rs.getString("file_name"),
                generated == null ? null : generated.toInstant(),
                received.toInstant(),
                parsed == null ? null : parsed.toInstant(),
                rs.getString("status"),
                (Integer) rs.getObject("event_count"),
                rs.getString("error_summary"));
    }

    public record InsertReceived(
            String brokerId,
            String brokerFileId,
            LocalDate businessDate,
            int schemaVersion,
            String fileSha256Hex,
            String source,
            String fileName,
            Instant generatedAt) {}
}
