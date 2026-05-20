package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Broker confirm queue + full §12.3 pipeline without FIX (seeded {@code orders} / {@code executions} / {@code positions}).
 */
class SettlementPipelineIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final UUID DEFAULT_CUSTODY =
            UUID.fromString("a0000001-0000-4000-8000-000000000001");

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    SettlementConfirmProcessor processor;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void markTradeFailed_removesPendingBrokerConfirm() {
        Seed seed = seedFilledBuyOrderWithExecution();
        processor.registerBrokerConfirms(List.of(seed.executionId()));
        assertThat(processor.markTradeFailed(seed.executionId())).isEqualTo(MarkTradeFailedResult.APPLIED);
        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        seed.executionId()))
                .isEqualTo("failed");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ? AND applied_at IS NULL",
                        Integer.class,
                        seed.executionId()))
                .isZero();
        assertThat(processor.markTradeFailed(seed.executionId())).isEqualTo(MarkTradeFailedResult.ALREADY_FAILED);
    }

    @Test
    void markTradeFailed_revertsBuyFillPosition() {
        Seed seed = seedFilledBuyOrderWithExecution();
        assertThat(processor.markTradeFailed(seed.executionId())).isEqualTo(MarkTradeFailedResult.APPLIED);
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_total FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_pending_buy_settle FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM position_history WHERE account_id = ? AND event_type = 'MARK_FAILED_UNWIND_BUY'",
                        Integer.class,
                        seed.accountId()))
                .isEqualTo(1);
    }

    @Test
    void markTradeFailed_revertsSellFillPositionWhenSplitStored() {
        SellSeed seed = seedFilledSellOrderWithExecution();
        assertThat(processor.markTradeFailed(seed.executionId())).isEqualTo(MarkTradeFailedResult.APPLIED);
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_total FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("10");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_settled FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("10");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_pending_sell_settle FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM position_history WHERE account_id = ? AND event_type = 'MARK_FAILED_UNWIND_SELL'",
                        Integer.class,
                        seed.accountId()))
                .isEqualTo(1);
    }

    @Test
    void brokerConfirmDrain_movesBuyPositionToSettled() {
        Seed seed = seedFilledBuyOrderWithExecution();
        processor.registerAndDrain(List.of(seed.executionId()), 20, 20);

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        seed.executionId()))
                .isEqualTo("settled");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_pending_buy_settle FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_settled FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("10");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM position_history WHERE account_id = ? AND event_type = 'SETTLEMENT_BUY_SETTLED'",
                        Integer.class,
                        seed.accountId()))
                .isEqualTo(1);
    }

    @Test
    void advanceOneStep_sellWithoutPositionRow_marksFailed() {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', 'SELL', 'NVDA', 1, 100, 'DAY',
                          NOW(), NOW(), 'h', NULL, 1
                        )
                        """,
                orderId,
                accountId,
                "settle-sell-nopos-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, settlement_status, raw_envelope_json
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          1, 100, 0, 1,
                          CAST('TRADE' AS execution_exec_type),
                          CAST('settling' AS execution_settlement_status),
                          CAST('{}' AS JSONB)
                        )
                        """,
                orderId,
                accountId,
                "vref-sell-nopos-" + orderId);
        long exId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);

        String next = processor.advanceOneSettlementStep(exId);
        assertThat(next).isEqualTo("failed");
        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        exId))
                .isEqualTo("failed");
    }

    @Test
    void brokerConfirmDrain_clearsSellPendingSettleAndAppendsHistory() {
        SellSeed seed = seedFilledSellOrderWithExecution();
        processor.registerAndDrain(List.of(seed.executionId()), 20, 20);

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        seed.executionId()))
                .isEqualTo("settled");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_pending_sell_settle FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT quantity_total FROM positions WHERE account_id = ? AND instrument_symbol = 'AAPL' AND custody_account_id = ?",
                        java.math.BigDecimal.class,
                        seed.accountId(),
                        DEFAULT_CUSTODY))
                .isEqualByComparingTo("0");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM position_history WHERE account_id = ? AND event_type = 'SETTLEMENT_SELL_SETTLED'",
                        Integer.class,
                        seed.accountId()))
                .isEqualTo(1);
    }

    @Test
    void classpathFixtureJsonShape_matchesControllerIngest() throws Exception {
        Seed seed = seedFilledBuyOrderWithExecution();
        String raw =
                new String(new ClassPathResource("settlement/broker-confirm-request.template.json")
                                .getInputStream()
                                .readAllBytes(),
                        StandardCharsets.UTF_8);
        raw = raw.replace("-1", String.valueOf(seed.executionId()));
        var tree = objectMapper.readTree(raw);
        assertThat(tree.get("executionIds").get(0).asLong()).isEqualTo(seed.executionId());
    }

    @Test
    void enqueueBrokerSettlementConfirm_nonTrade_throws() {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'REJECTED', 'BUY', 'AAPL', 10, 5, 'DAY',
                          NOW(), NOW(), 'h', NULL, 0
                        )
                        """,
                orderId,
                accountId,
                "settle-rej-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          0, 0, 10, 0,
                          CAST('REJECT' AS execution_exec_type), CAST('{}' AS JSONB)
                        )
                        """,
                orderId,
                accountId,
                "vref-rej-" + orderId);
        long exId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);

        assertThatThrownBy(() -> processor.enqueueBrokerSettlementConfirmForTradeOrThrow(exId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TRADE");
    }

    private Seed seedFilledBuyOrderWithExecution() {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', 'BUY', 'AAPL', 10, 5, 'DAY',
                          NOW(), NOW(), 'h', NULL, 10
                        )
                        """,
                orderId,
                accountId,
                "settle-it-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          10, 5, 0, 10,
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB)
                        )
                        """,
                orderId,
                accountId,
                "vref-" + orderId);
        long exId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', ?, 10, 0, 10, 0)
                        """,
                accountId,
                DEFAULT_CUSTODY);
        return new Seed(accountId, exId);
    }

    /**
     * Post-SELL-fill position state: customer had 10 settled AAPL, sold 10 → total/settled 0, pending_sell 10
     * (matches {@link com.balh.oms.persistence.PositionsRepository#recordTradeFill} SELL path).
     */
    private SellSeed seedFilledSellOrderWithExecution() {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', 'SELL', 'AAPL', 10, 5, 'DAY',
                          NOW(), NOW(), 'h', NULL, 10
                        )
                        """,
                orderId,
                accountId,
                "settle-sell-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          10, 5, 0, 10,
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB)
                        )
                        """,
                orderId,
                accountId,
                "vref-sell-" + orderId);
        long exId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', ?, 0, 0, 0, 10)
                        """,
                accountId,
                DEFAULT_CUSTODY);
        jdbc.update(
                "UPDATE executions SET sell_position_from_pending_buy = 0, sell_position_from_settled = 10 WHERE id = ?",
                exId);
        return new SellSeed(accountId, exId);
    }

    private record Seed(UUID accountId, long executionId) {}

    private record SellSeed(UUID accountId, long executionId) {}
}
