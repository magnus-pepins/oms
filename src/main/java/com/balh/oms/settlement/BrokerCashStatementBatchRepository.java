package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository
public class BrokerCashStatementBatchRepository {

    public record BatchRow(
            long id,
            String brokerId,
            String brokerFileId,
            LocalDate businessDate,
            String currency,
            int schemaVersion,
            String fileSha256Hex,
            String source,
            String fileName,
            Instant generatedAt,
            Instant receivedAt,
            Instant parsedAt,
            BigDecimal openingBalance,
            BigDecimal closingBalance,
            String status,
            Integer movementCount,
            String errorSummary) {}

    private static final String INSERT_RECEIVED =
            """
                    INSERT INTO broker_cash_statement_batch (
                        broker_id, broker_file_id, business_date, currency, schema_version,
                        file_sha256_hex, source, file_name, generated_at,
                        opening_balance, closing_balance, status
                    ) VALUES (
                        :brokerId, :brokerFileId, :businessDate, :currency, :schemaVersion,
                        :sha, :source, :fileName, :generatedAt,
                        :opening, :closing, 'received'
                    )
                    ON CONFLICT DO NOTHING
                    """;

    private static final String COMMON_SELECT =
            """
                    SELECT id, broker_id, broker_file_id, business_date, currency, schema_version,
                           file_sha256_hex, source, file_name, generated_at, received_at,
                           parsed_at, opening_balance, closing_balance, status, movement_count, error_summary
                    FROM broker_cash_statement_batch
                    """;

    private static final String UPDATE_STATUS =
            """
                    UPDATE broker_cash_statement_batch
                    SET status = :st,
                        movement_count = COALESCE(:mc, movement_count),
                        parsed_at = CASE WHEN :st = 'parsed' AND parsed_at IS NULL THEN NOW() ELSE parsed_at END,
                        error_summary = :es
                    WHERE id = :id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerCashStatementBatchRepository(NamedParameterJdbcTemplate jdbc) {
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
                        .addValue("currency", cmd.currency())
                        .addValue("schemaVersion", cmd.schemaVersion())
                        .addValue("sha", cmd.fileSha256Hex())
                        .addValue("source", cmd.source())
                        .addValue("fileName", cmd.fileName())
                        .addValue("generatedAt", cmd.generatedAt() == null ? null : Timestamp.from(cmd.generatedAt()))
                        .addValue("opening", cmd.openingBalance())
                        .addValue("closing", cmd.closingBalance()),
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

    public void updateStatus(long id, String status, Integer movementCount, String errorSummary) {
        jdbc.update(
                UPDATE_STATUS,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("st", status)
                        .addValue("mc", movementCount)
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
                rs.getString("currency"),
                rs.getInt("schema_version"),
                rs.getString("file_sha256_hex"),
                rs.getString("source"),
                rs.getString("file_name"),
                generated == null ? null : generated.toInstant(),
                received.toInstant(),
                parsed == null ? null : parsed.toInstant(),
                rs.getBigDecimal("opening_balance"),
                rs.getBigDecimal("closing_balance"),
                rs.getString("status"),
                (Integer) rs.getObject("movement_count"),
                rs.getString("error_summary"));
    }

    public record InsertReceived(
            String brokerId,
            String brokerFileId,
            LocalDate businessDate,
            String currency,
            int schemaVersion,
            String fileSha256Hex,
            String source,
            String fileName,
            Instant generatedAt,
            BigDecimal openingBalance,
            BigDecimal closingBalance) {}
}
