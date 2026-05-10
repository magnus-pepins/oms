package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource(properties = "oms.ledger.settlement-outbox-enabled=true")
class LedgerSettlementOutboxIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final UUID DEFAULT_CUSTODY =
            UUID.fromString("a0000001-0000-4000-8000-000000000001");

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    SettlementConfirmProcessor processor;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void brokerConfirmDrain_insertsLedgerOutboxWhenSettled() {
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
                "ledger-outbox-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, settlement_status
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          10, 5, 0, 10,
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          CAST('executed' AS execution_settlement_status)
                        )
                        """,
                orderId,
                accountId,
                "vref-outbox-" + orderId);
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

        processor.registerAndDrain(List.of(exId), 20, 20);

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM ledger_settlement_outbox WHERE execution_id = ? AND to_settlement_status = 'settled'",
                        Integer.class,
                        exId))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT payload_json->>'event' FROM ledger_settlement_outbox WHERE execution_id = ?",
                        String.class,
                        exId))
                .isEqualTo("SETTLEMENT_SETTLED");
        assertThat(jdbc.queryForObject(
                        "SELECT payload_json->>'schemaVersion' FROM ledger_settlement_outbox WHERE execution_id = ?",
                        String.class,
                        exId))
                .isEqualTo("1");
    }
}
