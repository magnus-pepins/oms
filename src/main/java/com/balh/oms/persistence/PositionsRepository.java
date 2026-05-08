package com.balh.oms.persistence;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Customer positions and history (slice 6 / §12.2). BUY fills increase total + pending buy settle;
 * SELL fills reduce pending buy first, then settled, then total, and add the fill qty to {@code
 * quantity_pending_sell_settle} until broker settlement clears it via {@link #recordSellSettled}.
 */
@Repository
public class PositionsRepository {

    private static final String UPSERT_BUY_SQL = """
            INSERT INTO positions (
                account_id, instrument_symbol, custody_account_id,
                quantity_total, quantity_pending_buy_settle, updated_at
            ) VALUES (
                :account_id, :symbol, :custody_id,
                :qty, :qty, NOW()
            )
            ON CONFLICT (account_id, instrument_symbol, custody_account_id) DO UPDATE SET
                quantity_total = positions.quantity_total + EXCLUDED.quantity_total,
                quantity_pending_buy_settle = positions.quantity_pending_buy_settle
                    + EXCLUDED.quantity_pending_buy_settle,
                updated_at = NOW()
            """;

    private static final String SELL_UPDATE_SQL = """
            UPDATE positions SET
                quantity_pending_buy_settle = quantity_pending_buy_settle
                    - LEAST(quantity_pending_buy_settle, :qty),
                quantity_settled = quantity_settled - LEAST(
                    GREATEST(0, quantity_settled),
                    :qty - LEAST(quantity_pending_buy_settle, :qty)
                ),
                quantity_total = quantity_total - :qty,
                quantity_pending_sell_settle = quantity_pending_sell_settle + :qty,
                updated_at = NOW()
            WHERE account_id = :account_id
              AND instrument_symbol = :symbol
              AND custody_account_id = :custody_id
              AND quantity_total >= :qty
            """;

    private static final String SETTLE_SELL_SQL = """
            UPDATE positions SET
                quantity_pending_sell_settle = quantity_pending_sell_settle - :qty,
                updated_at = NOW()
            WHERE account_id = :account_id
              AND instrument_symbol = :symbol
              AND custody_account_id = :custody_id
              AND quantity_pending_sell_settle >= :qty
            """;

    private static final String SETTLE_BUY_SQL = """
            UPDATE positions SET
                quantity_pending_buy_settle = quantity_pending_buy_settle - :qty,
                quantity_settled = quantity_settled + :qty,
                updated_at = NOW()
            WHERE account_id = :account_id
              AND instrument_symbol = :symbol
              AND custody_account_id = :custody_id
              AND quantity_pending_buy_settle >= :qty
            """;

    private static final String INSERT_HISTORY_SQL = """
            INSERT INTO position_history (
                account_id, instrument_symbol, custody_account_id,
                event_type, quantity_delta, execution_id, payload_json
            ) VALUES (
                :account_id, :symbol, :custody_id,
                :event_type, :delta, :execution_id, CAST(:payload AS JSONB)
            )
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public PositionsRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Applies a trade fill to {@code positions} + {@code position_history} (same txn as caller).
     */
    public void recordTradeFill(Order order, long executionId, BigDecimal fillQuantity, UUID custodyAccountId) {
        if (fillQuantity == null || fillQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            return;
        }
        String sym = order.instrumentSymbol() == null ? "" : order.instrumentSymbol().trim();
        if (sym.isEmpty()) {
            return;
        }
        if (order.side() == Side.BUY) {
            jdbc.update(
                    UPSERT_BUY_SQL,
                    new MapSqlParameterSource()
                            .addValue("account_id", order.accountId())
                            .addValue("symbol", sym)
                            .addValue("custody_id", custodyAccountId)
                            .addValue("qty", fillQuantity));
            insertHistory(
                    order.accountId(),
                    sym,
                    custodyAccountId,
                    "TRADE_BUY_FILL",
                    fillQuantity,
                    executionId);
        } else {
            int updated = jdbc.update(
                    SELL_UPDATE_SQL,
                    new MapSqlParameterSource()
                            .addValue("account_id", order.accountId())
                            .addValue("symbol", sym)
                            .addValue("custody_id", custodyAccountId)
                            .addValue("qty", fillQuantity));
            if (updated > 0) {
                insertHistory(
                        order.accountId(),
                        sym,
                        custodyAccountId,
                        "TRADE_SELL_FILL",
                        fillQuantity.negate(),
                        executionId);
            }
        }
    }

    /**
     * Moves {@code quantity_pending_buy_settle} → {@code quantity_settled} for a BUY leg (§12.3 terminal).
     */
    public void recordBuySettled(
            UUID accountId, String instrumentSymbol, UUID custodyAccountId, BigDecimal settleQuantity, long executionId) {
        if (settleQuantity == null || settleQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            return;
        }
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim();
        if (sym.isEmpty()) {
            return;
        }
        int updated = jdbc.update(
                SETTLE_BUY_SQL,
                new MapSqlParameterSource()
                        .addValue("account_id", accountId)
                        .addValue("symbol", sym)
                        .addValue("custody_id", custodyAccountId)
                        .addValue("qty", settleQuantity));
        if (updated != 1) {
            throw new IllegalStateException(
                    "buy settle expected 1 position row, got %s (account=%s symbol=%s execution=%s)"
                            .formatted(updated, accountId, sym, executionId));
        }
        insertHistory(accountId, sym, custodyAccountId, "SETTLEMENT_BUY_SETTLED", settleQuantity, executionId);
    }

    /**
     * Clears {@code quantity_pending_sell_settle} for a SELL leg after broker settlement pipeline reaches
     * {@code settled} (§12.3 terminal).
     */
    public void recordSellSettled(
            UUID accountId, String instrumentSymbol, UUID custodyAccountId, BigDecimal settleQuantity, long executionId) {
        if (settleQuantity == null || settleQuantity.compareTo(BigDecimal.ZERO) <= 0) {
            return;
        }
        String sym = instrumentSymbol == null ? "" : instrumentSymbol.trim();
        if (sym.isEmpty()) {
            return;
        }
        int updated = jdbc.update(
                SETTLE_SELL_SQL,
                new MapSqlParameterSource()
                        .addValue("account_id", accountId)
                        .addValue("symbol", sym)
                        .addValue("custody_id", custodyAccountId)
                        .addValue("qty", settleQuantity));
        if (updated != 1) {
            throw new IllegalStateException(
                    "sell settle expected 1 position row, got %s (account=%s symbol=%s execution=%s)"
                            .formatted(updated, accountId, sym, executionId));
        }
        insertHistory(accountId, sym, custodyAccountId, "SETTLEMENT_SELL_SETTLED", settleQuantity, executionId);
    }

    private void insertHistory(
            UUID accountId,
            String symbol,
            UUID custodyId,
            String eventType,
            BigDecimal delta,
            long executionId) {
        jdbc.update(
                INSERT_HISTORY_SQL,
                new MapSqlParameterSource()
                        .addValue("account_id", accountId)
                        .addValue("symbol", symbol)
                        .addValue("custody_id", custodyId)
                        .addValue("event_type", eventType)
                        .addValue("delta", delta)
                        .addValue("execution_id", executionId)
                        .addValue("payload", "{}"));
    }
}
