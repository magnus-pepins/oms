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
    /** Gap plan §5.6 — opened by {@link PositionReconciliationService}. */
    public static final String BREAK_POSITION_MISMATCH = "position_mismatch";
    /** Gap plan §5.7 — opened by broker cash statement reconciliation. */
    public static final String BREAK_CASH_MISMATCH = "cash_mismatch";
    /** Gap plan §5.8 — broker fail row could not resolve to an OMS execution. */
    public static final String BREAK_SETTLEMENT_FAIL_UNMATCHED = "settlement_fail_unmatched";
    /** Gap plan §5.8 — broker failed quantity exceeds OMS trade quantity. */
    public static final String BREAK_SETTLEMENT_FAIL_QUANTITY_MISMATCH = "settlement_fail_quantity_mismatch";

    private static final java.util.Set<String> ALLOWED_BREAK_TYPES = java.util.Set.of(
            BREAK_TRADE_MISMATCH,
            BREAK_UNRESOLVED_CONFIRM,
            BREAK_SETTLEMENT_DATE_MISMATCH,
            BREAK_POSITION_MISMATCH,
            BREAK_CASH_MISMATCH,
            BREAK_SETTLEMENT_FAIL_UNMATCHED,
            BREAK_SETTLEMENT_FAIL_QUANTITY_MISMATCH);

    private static final Set<String> ALLOWED_SEVERITIES = java.util.Set.of(SEVERITY_HIGH, SEVERITY_MEDIUM);

    public record BreakSummary(int total, java.util.Map<String, Integer> byBreakType, java.util.Map<String, Integer> bySeverity) {}

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
            String notes,
            String assignedTo,
            String resolutionCode,
            String resolutionNote) {}

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
                           status, opened_at, opened_by, resolved_at, resolved_by, notes,
                           assigned_to, resolution_code, resolution_note
                    """;

    private static final String LIST_BY_STATUS =
            SELECT_COLUMNS
                    + """
                    FROM reconciliation_breaks
                    WHERE status = :status
                    ORDER BY opened_at DESC, id DESC
                    LIMIT :lim OFFSET :off
                    """;

    private static final String LIST_FILTERED =
            SELECT_COLUMNS
                    + """
                    FROM reconciliation_breaks
                    WHERE status = :status
                      AND (CAST(:breakType AS TEXT) IS NULL OR break_type = CAST(:breakType AS TEXT))
                      AND (CAST(:severity AS TEXT) IS NULL OR severity = CAST(:severity AS TEXT))
                    ORDER BY opened_at DESC, id DESC
                    LIMIT :lim OFFSET :off
                    """;

    private static final String SUMMARY_BY_STATUS =
            """
                    SELECT break_type, severity, COUNT(*)::int AS cnt
                    FROM reconciliation_breaks
                    WHERE status = :status
                    GROUP BY break_type, severity
                    """;

    private static final String FIND_BY_ID = SELECT_COLUMNS + " FROM reconciliation_breaks WHERE id = :id";

    private static final String ASSIGN_INVESTIGATING =
            """
                    UPDATE reconciliation_breaks
                    SET status = 'investigating',
                        assigned_to = :assignedTo,
                        notes = COALESCE(CAST(:notes AS TEXT), notes)
                    WHERE id = :id AND status = 'open'
                    """;

    private static final String MARK_RESOLVED =
            """
                    UPDATE reconciliation_breaks
                    SET status = 'resolved',
                        resolution_code = :resolutionCode,
                        resolution_note = :resolutionNote,
                        resolved_at = NOW(),
                        resolved_by = :resolvedBy
                    WHERE id = :id AND status IN ('open', 'investigating')
                    """;

    private static final String MARK_WAIVED =
            """
                    UPDATE reconciliation_breaks
                    SET status = 'waived',
                        resolution_code = :resolutionCode,
                        resolution_note = :resolutionNote,
                        resolved_at = NOW(),
                        resolved_by = :resolvedBy
                    WHERE id = :id AND status IN ('open', 'investigating')
                    """;

    private static final String LIST_BY_EXECUTION =
            SELECT_COLUMNS
                    + """
                    FROM reconciliation_breaks
                    WHERE execution_id = :executionId
                    ORDER BY opened_at DESC, id DESC
                    LIMIT :lim
                    """;

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
        return listFiltered(status, null, null, limit, offset);
    }

    public List<BreakRow> listFiltered(String status, String breakType, String severity, int limit, int offset) {
        validateStatus(status);
        validateOptionalBreakType(breakType);
        validateOptionalSeverity(severity);
        String normalizedBreakType = normalizeOptional(breakType);
        String normalizedSeverity = normalizeOptional(severity);
        return jdbc.query(
                LIST_FILTERED,
                new MapSqlParameterSource()
                        .addValue("status", status)
                        .addValue("breakType", normalizedBreakType)
                        .addValue("severity", normalizedSeverity)
                        .addValue("lim", limit)
                        .addValue("off", offset),
                ROW_MAPPER);
    }

    public BreakSummary summarizeByStatus(String status) {
        validateStatus(status);
        java.util.Map<String, Integer> byBreakType = new java.util.LinkedHashMap<>();
        java.util.Map<String, Integer> bySeverity = new java.util.LinkedHashMap<>();
        int total = 0;
        jdbc.query(
                SUMMARY_BY_STATUS,
                new MapSqlParameterSource("status", status),
                (rs, rowNum) -> {
                    String bt = rs.getString("break_type");
                    String sev = rs.getString("severity");
                    int cnt = rs.getInt("cnt");
                    byBreakType.merge(bt, cnt, Integer::sum);
                    bySeverity.merge(sev, cnt, Integer::sum);
                    return cnt;
                });
        for (int n : byBreakType.values()) {
            total += n;
        }
        return new BreakSummary(total, java.util.Map.copyOf(byBreakType), java.util.Map.copyOf(bySeverity));
    }

    private static void validateStatus(String status) {
        if (!ALLOWED_STATUS.contains(status)) {
            throw new IllegalArgumentException("status must be one of " + ALLOWED_STATUS);
        }
    }

    private static void validateOptionalBreakType(String breakType) {
        if (breakType != null && !breakType.isBlank() && !ALLOWED_BREAK_TYPES.contains(breakType.trim())) {
            throw new IllegalArgumentException("breakType must be one of " + ALLOWED_BREAK_TYPES);
        }
    }

    private static void validateOptionalSeverity(String severity) {
        if (severity != null && !severity.isBlank() && !ALLOWED_SEVERITIES.contains(severity.trim())) {
            throw new IllegalArgumentException("severity must be one of " + ALLOWED_SEVERITIES);
        }
    }

    private static String normalizeOptional(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    public java.util.Optional<BreakRow> findById(long id) {
        List<BreakRow> rows = jdbc.query(FIND_BY_ID, new MapSqlParameterSource("id", id), ROW_MAPPER);
        return rows.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(rows.getFirst());
    }

    public List<BreakRow> listByExecutionId(long executionId) {
        return jdbc.query(
                LIST_BY_EXECUTION,
                new MapSqlParameterSource().addValue("executionId", executionId).addValue("lim", 50),
                ROW_MAPPER);
    }

    public int assignInvestigating(long id, String assignedTo, String notes) {
        return jdbc.update(
                ASSIGN_INVESTIGATING,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("assignedTo", assignedTo)
                        .addValue("notes", notes));
    }

    public int markResolved(long id, String resolutionCode, String resolutionNote, String resolvedBy) {
        return jdbc.update(
                MARK_RESOLVED,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("resolutionCode", resolutionCode)
                        .addValue("resolutionNote", resolutionNote)
                        .addValue("resolvedBy", resolvedBy));
    }

    public int markWaived(long id, String resolutionCode, String resolutionNote, String resolvedBy) {
        return jdbc.update(
                MARK_WAIVED,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("resolutionCode", resolutionCode)
                        .addValue("resolutionNote", resolutionNote)
                        .addValue("resolvedBy", resolvedBy));
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
                rs.getString("notes"),
                rs.getString("assigned_to"),
                rs.getString("resolution_code"),
                rs.getString("resolution_note"));
    };
}
