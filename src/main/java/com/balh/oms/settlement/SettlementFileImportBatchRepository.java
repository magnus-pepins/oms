package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class SettlementFileImportBatchRepository {

    public record BatchRow(long id, String status, Integer rowCount, String errorSummary) {}

    private static final String INSERT_RECEIVED =
            """
                    INSERT INTO settlement_file_import_batch (source, file_name, file_sha256_hex, status)
                    VALUES (:source, :fileName, :sha, 'received')
                    ON CONFLICT (file_sha256_hex) DO NOTHING
                    """;

    private static final String SELECT_BY_SHA =
            """
                    SELECT id, status, row_count, error_summary
                    FROM settlement_file_import_batch
                    WHERE file_sha256_hex = :sha
                    LIMIT 1
                    """;

    private static final String UPDATE =
            """
                    UPDATE settlement_file_import_batch
                    SET status = :st, row_count = :rc, error_summary = :es
                    WHERE id = :id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public SettlementFileImportBatchRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return new batch id when inserted, empty when SHA already existed
     */
    public Optional<Long> insertReceivedIfNew(String source, String fileName, String sha256Hex) {
        var params =
                new MapSqlParameterSource()
                        .addValue("source", source)
                        .addValue("fileName", fileName)
                        .addValue("sha", sha256Hex);
        var kh = new GeneratedKeyHolder();
        jdbc.update(INSERT_RECEIVED, params, kh, new String[] {"id"});
        Number key = kh.getKey();
        return key == null ? Optional.empty() : Optional.of(key.longValue());
    }

    public Optional<BatchRow> findBySha256Hex(String sha256Hex) {
        List<BatchRow> rows =
                jdbc.query(
                        SELECT_BY_SHA,
                        new MapSqlParameterSource("sha", sha256Hex),
                        (rs, i) ->
                                new BatchRow(
                                        rs.getLong("id"),
                                        rs.getString("status"),
                                        (Integer) rs.getObject("row_count"),
                                        rs.getString("error_summary")));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    private static final String LIST_RECENT =
            """
                    SELECT id, source, file_name, file_sha256_hex, received_at, status, row_count, error_summary
                    FROM settlement_file_import_batch
                    ORDER BY received_at DESC, id DESC
                    LIMIT :lim OFFSET :off
                    """;

    public record FileImportBatchListRow(
            long id,
            String source,
            String fileName,
            String fileSha256Hex,
            java.time.Instant receivedAt,
            String status,
            Integer rowCount,
            String errorSummary) {}

    private static final org.springframework.jdbc.core.RowMapper<FileImportBatchListRow> LIST_ROW_MAPPER =
            (rs, rowNum) ->
                    new FileImportBatchListRow(
                            rs.getLong("id"),
                            rs.getString("source"),
                            rs.getString("file_name"),
                            rs.getString("file_sha256_hex"),
                            rs.getTimestamp("received_at").toInstant(),
                            rs.getString("status"),
                            (Integer) rs.getObject("row_count"),
                            rs.getString("error_summary"));

    public List<FileImportBatchListRow> listRecentBatches(int limit, int offset) {
        return jdbc.query(
                LIST_RECENT,
                new MapSqlParameterSource().addValue("lim", limit).addValue("off", offset),
                LIST_ROW_MAPPER);
    }

    public void updateStatus(long id, String status, Integer rowCount, String errorSummary) {
        jdbc.update(
                UPDATE,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("st", status)
                        .addValue("rc", rowCount)
                        .addValue("es", errorSummary));
    }
}
