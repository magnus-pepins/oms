package com.balh.oms.settlement;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Persistence for {@code reconciliation_breaks} (Flyway V57 / gap plan §5.2).
 *
 * <p>Single break-queue table for all reconciliation domains. v1 inserts come from
 * {@link BrokerTradeConfirmMatcher}; future reconcilers (positions, cash, corporate
 * actions) append into the same table for one beard-admin queue.
 */
@Repository
public class ReconciliationBreakRepository {

    public static final String SOURCE_BROKER = "broker";
    public static final String SEVERITY_HIGH = "high";
    public static final String SEVERITY_MEDIUM = "medium";
    public static final String STATUS_OPEN = "open";

    public static final String BREAK_TRADE_MISMATCH = "trade_mismatch";
    public static final String BREAK_UNRESOLVED_CONFIRM = "unresolved_confirm";
    /**
     * Gap plan §5.3 Slice 2a. Opened by {@link BrokerTradeConfirmMatcher} when the broker's
     * {@code settlement_date} disagrees with the OMS-computed
     * {@code executions.expected_settlement_date} on an otherwise-matched confirm. Severity
     * is {@link #SEVERITY_MEDIUM} because the broker is authoritative on actual settlement
     * date — the break exists for calendar / config drift visibility, not as a settle-stop.
     */
    public static final String BREAK_SETTLEMENT_DATE_MISMATCH = "settlement_date_mismatch";

    public record BreakRow(
            long id,
            String breakType,
            String severity,
            String sourceSystem,
            Long confirmId,
            Long executionId,
            UUID accountId,
            LocalDate businessDate,
            String diffJson,
            String status,
            Instant openedAt,
            String openedBy,
            Instant resolvedAt,
            String resolvedBy,
            String notes) {}

    private static final Set<String> ALLOWED_STATUS = Set.of("open", "investigating", "resolved", "waived");

    private static final String INSERT =
            """
                    INSERT INTO reconciliation_breaks (
                        break_type, severity, source_system,
                        confirm_id, execution_id, account_id, business_date,
                        diff_json, status, opened_by
                    ) VALUES (
                        :breakType, :severity, :sourceSystem,
                        :confirmId, :executionId, :accountId, :businessDate,
                        CAST(:diffJson AS JSONB), 'open', :openedBy
                    )
                    """;

    private static final String SELECT_COLUMNS =
            """
                    SELECT id, break_type, severity, source_system,
                           confirm_id, execution_id, account_id, business_date,
                           diff_json::text AS diff_json,
                           status, opened_at, opened_by, resolved_at, resolved_by, notes
                    """;

    private static final String LIST_BY_STATUS =
            SELECT_COLUMNS
                    + """
                    FROM reconciliation_breaks
                    WHERE status = :status
                    ORDER BY opened_at DESC, id DESC
                    LIMIT :lim OFFSET :off
                    """;

    private static final String FIND_BY_ID = SELECT_COLUMNS + " FROM reconciliation_breaks WHERE id = :id";

    private final NamedParameterJdbcTemplate jdbc;

    public ReconciliationBreakRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long insert(InsertCommand cmd) {
        var params = new MapSqlParameterSource()
                .addValue("breakType", cmd.breakType())
                .addValue("severity", cmd.severity() == null ? SEVERITY_HIGH : cmd.severity())
                .addValue("sourceSystem", cmd.sourceSystem() == null ? SOURCE_BROKER : cmd.sourceSystem())
                .addValue("confirmId", cmd.confirmId())
                .addValue("executionId", cmd.executionId())
                .addValue("accountId", cmd.accountId())
                .addValue("businessDate", cmd.businessDate() == null ? null : Date.valueOf(cmd.businessDate()))
                .addValue("diffJson", cmd.diffJson() == null ? "{}" : cmd.diffJson())
                .addValue("openedBy", cmd.openedBy() == null ? "system" : cmd.openedBy());
        var kh = new GeneratedKeyHolder();
        jdbc.update(INSERT, params, kh, new String[] {"id"});
        Number key = kh.getKey();
        if (key == null) {
            throw new IllegalStateException("reconciliation_breaks insert returned no id");
        }
        return key.longValue();
    }

    public List<BreakRow> listByStatus(String status, int limit, int offset) {
        if (!ALLOWED_STATUS.contains(status)) {
            throw new IllegalArgumentException("status must be one of " + ALLOWED_STATUS);
        }
        return jdbc.query(
                LIST_BY_STATUS,
                new MapSqlParameterSource()
                        .addValue("status", status)
                        .addValue("lim", limit)
                        .addValue("off", offset),
                ROW_MAPPER);
    }

    public java.util.Optional<BreakRow> findById(long id) {
        List<BreakRow> rows = jdbc.query(FIND_BY_ID, new MapSqlParameterSource("id", id), ROW_MAPPER);
        return rows.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(rows.getFirst());
    }

    public record InsertCommand(
            String breakType,
            String severity,
            String sourceSystem,
            Long confirmId,
            Long executionId,
            UUID accountId,
            LocalDate businessDate,
            String diffJson,
            String openedBy) {}

    private static final RowMapper<BreakRow> ROW_MAPPER = (rs, rowNum) -> {
        Object accountId = rs.getObject("account_id");
        Date bd = rs.getDate("business_date");
        Timestamp resolvedAt = rs.getTimestamp("resolved_at");
        return new BreakRow(
                rs.getLong("id"),
                rs.getString("break_type"),
                rs.getString("severity"),
                rs.getString("source_system"),
                (Long) rs.getObject("confirm_id"),
                (Long) rs.getObject("execution_id"),
                accountId == null ? null : (UUID) accountId,
                bd == null ? null : bd.toLocalDate(),
                rs.getString("diff_json"),
                rs.getString("status"),
                rs.getTimestamp("opened_at").toInstant(),
                rs.getString("opened_by"),
                resolvedAt == null ? null : resolvedAt.toInstant(),
                rs.getString("resolved_by"),
                rs.getString("notes"));
    };
}
