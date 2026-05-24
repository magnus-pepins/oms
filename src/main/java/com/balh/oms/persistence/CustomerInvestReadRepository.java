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
                });
    }
}
