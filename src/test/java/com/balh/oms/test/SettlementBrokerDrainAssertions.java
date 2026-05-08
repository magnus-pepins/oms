package com.balh.oms.test;

import com.balh.oms.settlement.SettlementConfirmProcessor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * After TRADE fills, broker-confirm drain settles every TRADE execution. BUY assertions cover
 * {@code quantity_settled} / {@code quantity_pending_buy_settle}; SELL assertions cover
 * {@code quantity_pending_sell_settle} and {@code SETTLEMENT_SELL_SETTLED} history rows.
 */
public final class SettlementBrokerDrainAssertions {

    private static final UUID DEFAULT_CUSTODY =
            UUID.fromString("a0000001-0000-4000-8000-000000000001");

    private SettlementBrokerDrainAssertions() {}

    public static void assertFullBrokerLifecycleSettles(
            JdbcTemplate jdbc,
            SettlementConfirmProcessor proc,
            UUID orderId,
            int expectedTradeExecs,
            BigDecimal expectedSettledQty) {
        List<Long> ids = jdbc.query(
                """
                        SELECT id FROM executions
                        WHERE order_id = ? AND exec_type = CAST('TRADE' AS execution_exec_type)
                        ORDER BY id
                        """,
                (rs, rowNum) -> rs.getLong(1),
                orderId);
        assertThat(ids).hasSize(expectedTradeExecs);
        proc.registerAndDrain(ids, 100, 50);
        for (Long id : ids) {
            assertThat(jdbc.queryForObject(
                            "SELECT settlement_status::text FROM executions WHERE id = ?",
                            String.class,
                            id))
                    .isEqualTo("settled");
        }
        UUID accountId = jdbc.queryForObject("SELECT account_id FROM orders WHERE id = ?", UUID.class, orderId);
        String sym = jdbc.queryForObject("SELECT instrument_symbol FROM orders WHERE id = ?", String.class, orderId);
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_settled FROM positions WHERE account_id = ? AND instrument_symbol = ? AND custody_account_id = ?",
                        BigDecimal.class,
                        accountId,
                        sym,
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo(expectedSettledQty);
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_pending_buy_settle FROM positions WHERE account_id = ? AND instrument_symbol = ? AND custody_account_id = ?",
                        BigDecimal.class,
                        accountId,
                        sym,
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
    }

    /**
     * Registers broker confirms for every TRADE on the order, drains the queue, and asserts each
     * execution is {@code settled} with {@code quantity_pending_sell_settle} returned to zero and
     * one {@code SETTLEMENT_SELL_SETTLED} history row per execution.
     */
    public static void assertSellTradesSettledClearsPendingSell(
            JdbcTemplate jdbc,
            SettlementConfirmProcessor proc,
            UUID orderId,
            int expectedTradeExecs) {
        List<Long> ids = jdbc.query(
                """
                        SELECT id FROM executions
                        WHERE order_id = ? AND exec_type = CAST('TRADE' AS execution_exec_type)
                        ORDER BY id
                        """,
                (rs, rowNum) -> rs.getLong(1),
                orderId);
        assertThat(ids).hasSize(expectedTradeExecs);
        proc.registerAndDrain(ids, 100, 50);
        for (Long id : ids) {
            assertThat(jdbc.queryForObject(
                            "SELECT settlement_status::text FROM executions WHERE id = ?",
                            String.class,
                            id))
                    .isEqualTo("settled");
        }
        UUID accountId = jdbc.queryForObject("SELECT account_id FROM orders WHERE id = ?", UUID.class, orderId);
        String sym = jdbc.queryForObject("SELECT instrument_symbol FROM orders WHERE id = ?", String.class, orderId);
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_pending_sell_settle FROM positions WHERE account_id = ? AND instrument_symbol = ? AND custody_account_id = ?",
                        BigDecimal.class,
                        accountId,
                        sym,
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM position_history WHERE account_id = ? AND event_type = 'SETTLEMENT_SELL_SETTLED'",
                        Integer.class,
                        accountId))
                .isEqualTo(expectedTradeExecs);
    }
}
