package com.balh.oms.settlement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Read-side counts for settlement ops Micrometer gauges (gap plan §5.18). */
@Repository
public class SettlementOpsMetricsRepository {

    private static final String COUNT_OPEN_BREAKS_BY_TYPE =
            """
                    SELECT break_type, COUNT(*)::int AS cnt
                    FROM reconciliation_breaks
                    WHERE status IN ('open', 'investigating')
                    GROUP BY break_type
                    """;

    private static final String MAX_OPEN_BREAK_AGE_SECONDS =
            """
                    SELECT COALESCE(
                        EXTRACT(EPOCH FROM (NOW() - MIN(opened_at)))::bigint,
                        0
                    )
                    FROM reconciliation_breaks
                    WHERE status IN ('open', 'investigating')
                    """;

    private static final String COUNT_STUCK_OUTBOX =
            """
                    SELECT COUNT(*)::int
                    FROM ledger_settlement_outbox
                    WHERE posted_at IS NULL
                      AND skipped_at IS NULL
                      AND attempts >= :minAttempts
                    """;

    private static final String COUNT_OPEN_SETTLEMENT_FAILS =
            """
                    SELECT COUNT(*)::int
                    FROM broker_settlement_fail_row
                    WHERE applied_at IS NULL
                    """;

    private static final String COUNT_PENDING_CORPORATE_ACTIONS =
            """
                    SELECT COUNT(*)::int
                    FROM corporate_action_event
                    WHERE processed_at IS NULL
                    """;

    private static final String COUNT_LATE_SETTLEMENT_EXECUTIONS =
            """
                    SELECT COUNT(*)::int
                    FROM executions
                    WHERE expected_settlement_date < CURRENT_DATE
                      AND settlement_status NOT IN (
                          CAST('settled' AS execution_settlement_status),
                          CAST('failed' AS execution_settlement_status))
                    """;

    private static final String COUNT_LATE_BROKER_FILE_BATCHES =
            """
                    SELECT COUNT(*)::int FROM (
                        SELECT id FROM broker_confirm_batch
                        WHERE business_date < CURRENT_DATE - 1 AND status NOT IN ('applied', 'reconciled')
                        UNION ALL
                        SELECT id FROM broker_cash_statement_batch
                        WHERE business_date < CURRENT_DATE - 1 AND status NOT IN ('applied', 'reconciled')
                        UNION ALL
                        SELECT id FROM broker_position_snapshot_batch
                        WHERE business_date < CURRENT_DATE - 1 AND status NOT IN ('applied', 'reconciled')
                        UNION ALL
                        SELECT id FROM broker_settlement_fail_batch
                        WHERE business_date < CURRENT_DATE - 1 AND status NOT IN ('applied', 'reconciled')
                        UNION ALL
                        SELECT id FROM broker_corporate_action_batch
                        WHERE business_date < CURRENT_DATE - 1 AND status NOT IN ('applied', 'reconciled')
                    ) late_batches
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public SettlementOpsMetricsRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Map<String, Integer> countOpenBreaksByType() {
        Map<String, Integer> counts = new HashMap<>();
        jdbc.query(
                COUNT_OPEN_BREAKS_BY_TYPE,
                new MapSqlParameterSource(),
                (rs, rowNum) -> counts.put(rs.getString("break_type"), rs.getInt("cnt")));
        return counts;
    }

    public long maxOpenBreakAgeSeconds() {
        Long age = jdbc.queryForObject(MAX_OPEN_BREAK_AGE_SECONDS, new MapSqlParameterSource(), Long.class);
        return age == null ? 0L : Math.max(0L, age);
    }

    public int countStuckOutboxRows(int minAttempts) {
        Integer n =
                jdbc.queryForObject(
                        COUNT_STUCK_OUTBOX,
                        new MapSqlParameterSource("minAttempts", Math.max(1, minAttempts)),
                        Integer.class);
        return n == null ? 0 : n;
    }

    public int countOpenSettlementFails() {
        Integer n = jdbc.queryForObject(COUNT_OPEN_SETTLEMENT_FAILS, new MapSqlParameterSource(), Integer.class);
        return n == null ? 0 : n;
    }

    public int countPendingCorporateActionEvents() {
        Integer n =
                jdbc.queryForObject(COUNT_PENDING_CORPORATE_ACTIONS, new MapSqlParameterSource(), Integer.class);
        return n == null ? 0 : n;
    }

    public int countPositionReconciliationBreaks() {
        return countOpenBreaksByType().getOrDefault(ReconciliationBreakRepository.BREAK_POSITION_MISMATCH, 0);
    }

    public int countCashReconciliationBreaks() {
        return countOpenBreaksByType().getOrDefault(ReconciliationBreakRepository.BREAK_CASH_MISMATCH, 0);
    }

    public int countLateSettlementExecutions() {
        Integer n =
                jdbc.queryForObject(COUNT_LATE_SETTLEMENT_EXECUTIONS, new MapSqlParameterSource(), Integer.class);
        return n == null ? 0 : n;
    }

    public int countLateBrokerFileBatches() {
        Integer n =
                jdbc.queryForObject(COUNT_LATE_BROKER_FILE_BATCHES, new MapSqlParameterSource(), Integer.class);
        return n == null ? 0 : n;
    }

    /** Bounded break-type label set for gauge registration (matches reconciliation_breaks CHECK). */
    public static List<String> boundedBreakTypes() {
        return List.of(
                ReconciliationBreakRepository.BREAK_TRADE_MISMATCH,
                ReconciliationBreakRepository.BREAK_UNRESOLVED_CONFIRM,
                "unmatched_execution",
                ReconciliationBreakRepository.BREAK_SETTLEMENT_DATE_MISMATCH,
                ReconciliationBreakRepository.BREAK_POSITION_MISMATCH,
                ReconciliationBreakRepository.BREAK_CASH_MISMATCH,
                "corporate_action_mismatch",
                ReconciliationBreakRepository.BREAK_SETTLEMENT_FAIL_UNMATCHED,
                ReconciliationBreakRepository.BREAK_SETTLEMENT_FAIL_QUANTITY_MISMATCH);
    }
}
