package com.balh.oms.corporateaction;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Inbox for {@code corporate_action_event} (Flyway V19). */
@Repository
public class CorporateActionEventRepository {

    private static final String INSERT_SQL = """
            INSERT INTO corporate_action_event (instrument_symbol, action_type, effective_date, payload_json)
            VALUES (:instrument_symbol, :action_type, CAST(:effective_date AS DATE), CAST(:payload_json AS JSONB))
            RETURNING id
            """;

    private static final String INSERT_FROM_BROKER_IF_NEW =
            """
                    INSERT INTO corporate_action_event (
                        broker_id, broker_event_id, instrument_symbol, action_type, effective_date,
                        record_date, payable_date, payload_json
                    ) VALUES (
                        :brokerId, :brokerEventId, :instrumentSymbol, :actionType,
                        CAST(:effectiveDate AS DATE), CAST(:recordDate AS DATE), CAST(:payableDate AS DATE),
                        CAST(:payloadJson AS JSONB)
                    )
                    ON CONFLICT (broker_id, broker_event_id) DO NOTHING
                    RETURNING id
                    """;

    private static final String SELECT_UNPROCESSED_FOR_PROCESSING =
            """
                    SELECT id, instrument_symbol, action_type, effective_date::text AS effective_date,
                           payload_json::text AS payload_json, record_date, payable_date
                    FROM corporate_action_event
                    WHERE processed_at IS NULL
                    ORDER BY id ASC
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                    """;

    private static final String MARK_PROCESSING_ERROR =
            """
                    UPDATE corporate_action_event
                    SET processing_error = :error
                    WHERE id = :id AND processed_at IS NULL
                    """;

    /**
     * {@code FOR UPDATE SKIP LOCKED} — callers must run inside a Spring transaction that spans this fetch and
     * {@link #markProcessedIfPending(long)} (see {@link CorporateActionProcessorJob}).
     */
    private static final String SELECT_UNPROCESSED_SQL = """
            SELECT id, instrument_symbol, action_type, effective_date::text AS effective_date
            FROM corporate_action_event
            WHERE processed_at IS NULL
            ORDER BY id ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_PROCESSED_SQL = """
            UPDATE corporate_action_event SET processed_at = NOW() WHERE id = :id AND processed_at IS NULL
            """;

    private static final String SELECT_PAGE_SQL = """
            SELECT id, instrument_symbol, action_type, effective_date::text AS effective_date,
                   created_at, processed_at
            FROM corporate_action_event
            ORDER BY id DESC
            LIMIT :limit OFFSET :offset
            """;

    private static final String SELECT_BY_ID_SQL = """
            SELECT id, instrument_symbol, action_type, effective_date::text AS effective_date,
                   payload_json::text AS payload_json, created_at, processed_at
            FROM corporate_action_event WHERE id = :id
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public CorporateActionEventRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long insert(String instrumentSymbol, String actionType, LocalDate effectiveDate, String payloadJson) {
        List<Long> ids =
                jdbc.query(
                        INSERT_SQL,
                        new MapSqlParameterSource()
                                .addValue("instrument_symbol", instrumentSymbol)
                                .addValue("action_type", actionType)
                                .addValue("effective_date", effectiveDate.toString())
                                .addValue("payload_json", payloadJson),
                        (rs, rowNum) -> rs.getLong("id"));
        if (ids.isEmpty()) {
            throw new IllegalStateException("insert corporate_action_event returned no id");
        }
        return ids.getFirst();
    }

    /** Idempotent broker-file apply (gap plan §5.9). Returns empty when broker event already ingested. */
    public Optional<Long> insertFromBrokerIfNew(
            String brokerId,
            String brokerEventId,
            String instrumentSymbol,
            String actionType,
            LocalDate effectiveDate,
            LocalDate recordDate,
            LocalDate payableDate,
            String payloadJson) {
        List<Long> ids =
                jdbc.query(
                        INSERT_FROM_BROKER_IF_NEW,
                        new MapSqlParameterSource()
                                .addValue("brokerId", brokerId)
                                .addValue("brokerEventId", brokerEventId)
                                .addValue("instrumentSymbol", instrumentSymbol)
                                .addValue("actionType", actionType)
                                .addValue("effectiveDate", effectiveDate.toString())
                                .addValue(
                                        "recordDate", recordDate == null ? null : recordDate.toString())
                                .addValue(
                                        "payableDate", payableDate == null ? null : payableDate.toString())
                                .addValue("payloadJson", payloadJson),
                        (rs, rowNum) -> rs.getLong("id"));
        return ids.isEmpty() ? Optional.empty() : Optional.of(ids.getFirst());
    }

    /** Backwards-compatible broker insert without record/payable dates. */
    public Optional<Long> insertFromBrokerIfNew(
            String brokerId,
            String brokerEventId,
            String instrumentSymbol,
            String actionType,
            LocalDate effectiveDate,
            String payloadJson) {
        return insertFromBrokerIfNew(
                brokerId, brokerEventId, instrumentSymbol, actionType, effectiveDate, null, null, payloadJson);
    }

    public List<UnprocessedRow> findUnprocessedForUpdateSkipLocked(int limit) {
        return jdbc.query(
                SELECT_UNPROCESSED_SQL,
                new MapSqlParameterSource("limit", limit),
                (rs, rowNum) ->
                        new UnprocessedRow(
                                rs.getLong("id"),
                                rs.getString("instrument_symbol"),
                                rs.getString("action_type"),
                                LocalDate.parse(rs.getString("effective_date"))));
    }

    public List<ProcessingRow> findUnprocessedForProcessing(int limit) {
        return jdbc.query(
                SELECT_UNPROCESSED_FOR_PROCESSING,
                new MapSqlParameterSource("limit", limit),
                (rs, rowNum) -> {
                    java.sql.Date recordDate = rs.getDate("record_date");
                    java.sql.Date payableDate = rs.getDate("payable_date");
                    return new ProcessingRow(
                            rs.getLong("id"),
                            rs.getString("instrument_symbol"),
                            rs.getString("action_type"),
                            LocalDate.parse(rs.getString("effective_date")),
                            rs.getString("payload_json"),
                            recordDate == null ? null : recordDate.toLocalDate(),
                            payableDate == null ? null : payableDate.toLocalDate());
                });
    }

    public void markProcessingError(long id, String error) {
        String err = error;
        if (err != null && err.length() > 500) {
            err = err.substring(0, 500);
        }
        jdbc.update(MARK_PROCESSING_ERROR, new MapSqlParameterSource("id", id).addValue("error", err));
    }

    /**
     * @return 1 if the row was newly marked processed, 0 if already processed or missing.
     */
    public int markProcessedIfPending(long id) {
        return jdbc.update(MARK_PROCESSED_SQL, new MapSqlParameterSource("id", id));
    }

    public record ListRow(
            long id,
            String instrumentSymbol,
            String actionType,
            String effectiveDate,
            Instant createdAt,
            Instant processedAt) {}

    public record DetailRow(
            long id,
            String instrumentSymbol,
            String actionType,
            String effectiveDate,
            String payloadJson,
            Instant createdAt,
            Instant processedAt) {}

    public List<ListRow> listPage(int limit, int offset) {
        return jdbc.query(
                SELECT_PAGE_SQL,
                new MapSqlParameterSource("limit", limit).addValue("offset", offset),
                (rs, rowNum) -> {
                    var proc = rs.getTimestamp("processed_at");
                    return new ListRow(
                            rs.getLong("id"),
                            rs.getString("instrument_symbol"),
                            rs.getString("action_type"),
                            rs.getString("effective_date"),
                            rs.getTimestamp("created_at").toInstant(),
                            proc == null ? null : proc.toInstant());
                });
    }

    public Optional<DetailRow> findById(long id) {
        var rows =
                jdbc.query(
                        SELECT_BY_ID_SQL,
                        new MapSqlParameterSource("id", id),
                        (rs, rowNum) -> {
                            var proc = rs.getTimestamp("processed_at");
                            return new DetailRow(
                                    rs.getLong("id"),
                                    rs.getString("instrument_symbol"),
                                    rs.getString("action_type"),
                                    rs.getString("effective_date"),
                                    rs.getString("payload_json"),
                                    rs.getTimestamp("created_at").toInstant(),
                                    proc == null ? null : proc.toInstant());
                        });
        return rows.stream().findFirst();
    }

    public record UnprocessedRow(long id, String instrumentSymbol, String actionType, LocalDate effectiveDate) {}

    public record ProcessingRow(
            long id,
            String instrumentSymbol,
            String actionType,
            LocalDate effectiveDate,
            String payloadJson,
            LocalDate recordDate,
            LocalDate payableDate) {}
}
