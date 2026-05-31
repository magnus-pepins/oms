package com.balh.oms.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Customer-facing invest read models (gap plan §5.4 / §5.13). Aggregates open
 * {@code TRADE} executions for cash-availability and settlement-event feeds.
 */
@Repository
public class CustomerInvestReadRepository {

    private static final String SUM_OPEN_TRADE_AMOUNTS =
            """
            SELECT
              COALESCE(SUM(
                CASE WHEN o.side = CAST('BUY' AS order_side)
                     THEN e.last_quantity * e.last_price ELSE 0 END), 0) AS pending_buy,
              COALESCE(SUM(
                CASE WHEN o.side = CAST('SELL' AS order_side)
                     THEN e.last_quantity * e.last_price ELSE 0 END), 0) AS pending_sell
            FROM executions e
            JOIN orders o ON o.id = e.order_id
            WHERE e.account_id = :account_id
              AND e.exec_type = CAST('TRADE' AS execution_exec_type)
              AND e.settlement_status NOT IN (
                  CAST('settled' AS execution_settlement_status),
                  CAST('failed' AS execution_settlement_status))
            """;

    private static final String LIST_SETTLEMENT_EVENTS =
            """
            SELECT e.id AS execution_id,
                   e.order_id,
                   o.instrument_symbol,
                   o.side::text AS side,
                   e.last_quantity,
                   e.last_price,
                   e.settlement_status::text AS settlement_status,
                   e.trade_date,
                   e.expected_settlement_date,
                   e.venue_ts
            FROM executions e
            JOIN orders o ON o.id = e.order_id
            WHERE e.account_id = :account_id
              AND e.exec_type = CAST('TRADE' AS execution_exec_type)
            ORDER BY e.venue_ts DESC NULLS LAST, e.id DESC
            LIMIT :lim
            """;

    /**
     * Full TRADE execution (fill) history for one account, optionally bounded by
     * {@code venue_ts} window. Ordered ascending so the BFF can replay fills to
     * reconstruct positions/equity over time (portfolio history + statements).
     * Unlike {@code LIST_SETTLEMENT_EVENTS} this supports a date range and a
     * larger row cap.
     */
    private static final String LIST_EXECUTIONS =
            """
            SELECT e.id AS execution_id,
                   e.order_id,
                   o.instrument_symbol,
                   o.side::text AS side,
                   e.last_quantity,
                   e.last_price,
                   e.settlement_status::text AS settlement_status,
                   e.trade_date,
                   e.expected_settlement_date,
                   e.venue_ts
            FROM executions e
            JOIN orders o ON o.id = e.order_id
            WHERE e.account_id = :account_id
              AND e.exec_type = CAST('TRADE' AS execution_exec_type)
              AND (CAST(:from_ts AS timestamptz) IS NULL OR e.venue_ts >= CAST(:from_ts AS timestamptz))
              AND (CAST(:to_ts AS timestamptz) IS NULL OR e.venue_ts <= CAST(:to_ts AS timestamptz))
            ORDER BY e.venue_ts ASC NULLS LAST, e.id ASC
            LIMIT :lim
            """;

    private static final String LIST_PENDING_VOLUNTARY_CA =
            """
            SELECT e.id AS event_id,
                   e.instrument_symbol,
                   e.action_type,
                   e.effective_date,
                   s.quantity_settled,
                   el.election_choice,
                   CASE
                     WHEN el.approved_at IS NOT NULL THEN 'approved'
                     WHEN el.id IS NOT NULL THEN 'pending_approval'
                     ELSE 'none'
                   END AS election_status
            FROM corporate_action_record_date_snapshot s
            JOIN corporate_action_event e ON e.id = s.corporate_action_event_id
            LEFT JOIN corporate_action_election el
              ON el.corporate_action_event_id = e.id AND el.account_id = s.account_id
            WHERE s.account_id = :account_id
              AND e.processed_at IS NULL
              AND e.action_type IN ('TENDER_OFFER', 'RIGHTS_ISSUE')
            ORDER BY e.effective_date ASC NULLS LAST, e.id ASC
            """;

    private static final String EXISTS_PENDING_VOLUNTARY_CA =
            """
            SELECT EXISTS (
              SELECT 1
              FROM corporate_action_record_date_snapshot s
              JOIN corporate_action_event e ON e.id = s.corporate_action_event_id
              WHERE s.account_id = :account_id
                AND e.id = :event_id
                AND e.processed_at IS NULL
                AND e.action_type IN ('TENDER_OFFER', 'RIGHTS_ISSUE')
            )
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public CustomerInvestReadRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record OpenTradeSettlementAmounts(BigDecimal pendingBuyDebits, BigDecimal pendingSellCredits) {}

    public record SettlementEventRow(
            long executionId,
            UUID orderId,
            String instrumentSymbol,
            String side,
            BigDecimal quantity,
            BigDecimal price,
            String settlementStatus,
            LocalDate tradeDate,
            LocalDate expectedSettlementDate,
            Instant venueTs) {}

    public record PendingVoluntaryCorporateActionRow(
            long eventId,
            String instrumentSymbol,
            String actionType,
            LocalDate effectiveDate,
            BigDecimal quantitySettled,
            String electionChoice,
            String electionStatus) {}

    public OpenTradeSettlementAmounts sumOpenTradeSettlementAmounts(UUID accountId) {
        return jdbc.queryForObject(
                SUM_OPEN_TRADE_AMOUNTS,
                new MapSqlParameterSource("account_id", accountId),
                (rs, rowNum) ->
                        new OpenTradeSettlementAmounts(
                                rs.getBigDecimal("pending_buy"),
                                rs.getBigDecimal("pending_sell")));
    }

    public List<SettlementEventRow> listSettlementEvents(UUID accountId, int limit) {
        return jdbc.query(
                LIST_SETTLEMENT_EVENTS,
                new MapSqlParameterSource("account_id", accountId).addValue("lim", limit),
                SETTLEMENT_EVENT_ROW_MAPPER);
    }

    /**
     * TRADE fills for one account in optional {@code [from, to]} {@code venue_ts}
     * window, ascending. {@code from}/{@code to} may be {@code null} for an
     * open-ended bound. Used by the BFF portfolio-history and statements engines.
     */
    public List<SettlementEventRow> listExecutions(UUID accountId, Instant from, Instant to, int limit) {
        return jdbc.query(
                LIST_EXECUTIONS,
                new MapSqlParameterSource("account_id", accountId)
                        .addValue("from_ts", from == null ? null : from.toString())
                        .addValue("to_ts", to == null ? null : to.toString())
                        .addValue("lim", limit),
                SETTLEMENT_EVENT_ROW_MAPPER);
    }

    private static final org.springframework.jdbc.core.RowMapper<SettlementEventRow> SETTLEMENT_EVENT_ROW_MAPPER =
            (rs, rowNum) -> {
                java.sql.Date td = rs.getDate("trade_date");
                java.sql.Date esd = rs.getDate("expected_settlement_date");
                var venueTs = rs.getTimestamp("venue_ts");
                return new SettlementEventRow(
                        rs.getLong("execution_id"),
                        rs.getObject("order_id", UUID.class),
                        rs.getString("instrument_symbol"),
                        rs.getString("side"),
                        rs.getBigDecimal("last_quantity"),
                        rs.getBigDecimal("last_price"),
                        rs.getString("settlement_status"),
                        td == null ? null : td.toLocalDate(),
                        esd == null ? null : esd.toLocalDate(),
                        venueTs == null ? null : venueTs.toInstant());
            };

    public List<PendingVoluntaryCorporateActionRow> listPendingVoluntaryCorporateActions(UUID accountId) {
        return jdbc.query(
                LIST_PENDING_VOLUNTARY_CA,
                new MapSqlParameterSource("account_id", accountId),
                (rs, rowNum) -> {
                    java.sql.Date ed = rs.getDate("effective_date");
                    return new PendingVoluntaryCorporateActionRow(
                            rs.getLong("event_id"),
                            rs.getString("instrument_symbol"),
                            rs.getString("action_type"),
                            ed == null ? null : ed.toLocalDate(),
                            rs.getBigDecimal("quantity_settled"),
                            rs.getString("election_choice"),
                            rs.getString("election_status"));
                });
    }

    public boolean accountHasPendingVoluntaryCorporateAction(UUID accountId, long eventId) {
        Boolean exists =
                jdbc.queryForObject(
                        EXISTS_PENDING_VOLUNTARY_CA,
                        new MapSqlParameterSource()
                                .addValue("account_id", accountId)
                                .addValue("event_id", eventId),
                        Boolean.class);
        return Boolean.TRUE.equals(exists);
    }
}
