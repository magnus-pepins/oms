package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Read-side lookups for cash reconciliation (gap plan §5.7). */
@Repository
public class CashReconciliationLookupRepository {

    public record ExecutionCashExpectation(
            long executionId,
            UUID accountId,
            String venueExecRef,
            String side,
            BigDecimal expectedCashAmount,
            String currency) {}

    private static final String FIND_BY_BROKER_VENUE_REF =
            """
                    SELECT e.id AS execution_id,
                           e.account_id,
                           e.venue_exec_ref,
                           o.side,
                           (e.last_quantity * e.last_price) AS gross,
                           'SEK' AS currency
                    FROM executions e
                    JOIN orders o ON o.id = e.order_id
                    WHERE e.venue_exec_ref = :venueRef
                      AND e.exec_type = CAST('TRADE' AS execution_exec_type)
                      AND EXISTS (
                          SELECT 1
                          FROM positions p
                          JOIN custody_accounts ca ON ca.id = p.custody_account_id
                          WHERE p.account_id = e.account_id
                            AND ca.broker_id = :brokerId
                      )
                    LIMIT 2
                    """;

    private static final String FIND_SETTLED_MISSING_REFS =
            """
                    SELECT e.id AS execution_id,
                           e.account_id,
                           e.venue_exec_ref,
                           o.side,
                           (e.last_quantity * e.last_price) AS gross,
                           'SEK' AS currency
                    FROM executions e
                    JOIN orders o ON o.id = e.order_id
                    WHERE e.settlement_status = CAST('settled' AS execution_settlement_status)
                      AND e.exec_type = CAST('TRADE' AS execution_exec_type)
                      AND e.expected_settlement_date = :businessDate
                      AND e.venue_exec_ref IS NOT NULL
                      AND e.venue_exec_ref <> ''
                      AND (CAST(:currency AS TEXT) IS NULL OR CAST(:currency AS TEXT) = 'SEK')
                      AND EXISTS (
                          SELECT 1
                          FROM positions p
                          JOIN custody_accounts ca ON ca.id = p.custody_account_id
                          WHERE p.account_id = e.account_id
                            AND ca.broker_id = :brokerId
                      )
                      AND NOT (e.venue_exec_ref = ANY(:refs))
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public CashReconciliationLookupRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<ExecutionCashExpectation> findByBrokerAndVenueRef(String brokerId, String venueRef) {
        if (venueRef == null || venueRef.isBlank() || brokerId == null || brokerId.isBlank()) {
            return Optional.empty();
        }
        List<ExecutionCashExpectation> rows = jdbc.query(
                FIND_BY_BROKER_VENUE_REF,
                new MapSqlParameterSource()
                        .addValue("brokerId", brokerId)
                        .addValue("venueRef", venueRef.trim()),
                (rs, rowNum) -> mapRow(rs));
        if (rows.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(rows.getFirst());
    }

    public List<ExecutionCashExpectation> findSettledExecutionsMissingRefs(
            String brokerId, LocalDate businessDate, String currency, String[] refsInBatch) {
        return jdbc.query(
                FIND_SETTLED_MISSING_REFS,
                new MapSqlParameterSource()
                        .addValue("brokerId", brokerId)
                        .addValue("businessDate", java.sql.Date.valueOf(businessDate))
                        .addValue("currency", currency)
                        .addValue("refs", refsInBatch == null || refsInBatch.length == 0 ? new String[0] : refsInBatch),
                (rs, rowNum) -> mapRow(rs));
    }

    private static ExecutionCashExpectation mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        String side = rs.getString("side");
        BigDecimal gross = rs.getBigDecimal("gross");
        BigDecimal signed = signedCashAmount(side, gross);
        return new ExecutionCashExpectation(
                rs.getLong("execution_id"),
                (UUID) rs.getObject("account_id"),
                rs.getString("venue_exec_ref"),
                side,
                signed,
                rs.getString("currency"));
    }

    /** BUY settlement debits customer cash (negative); SELL credits (positive). */
    static BigDecimal signedCashAmount(String side, BigDecimal gross) {
        BigDecimal g = gross == null ? BigDecimal.ZERO : gross;
        if ("SELL".equalsIgnoreCase(side)) {
            return g;
        }
        return g.negate();
    }
}
