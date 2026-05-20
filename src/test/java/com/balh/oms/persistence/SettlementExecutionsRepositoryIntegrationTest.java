package com.balh.oms.persistence;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Postgres-backed checks for {@link SettlementExecutionsRepository#findNonTerminalTradeIds}.
 */
class SettlementExecutionsRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final long MAX_AGE_SECONDS = 3600L;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    SettlementExecutionsRepository repository;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void findNonTerminalTradeIds_excludesExecutionsOlderThanMaxAge() {
        long recentId = insertTradeExecution("recent", null);
        long staleId = insertTradeExecution("stale", "NOW() - interval '2 hours'");
        long terminalId = insertTradeExecution("settled", null);
        jdbc.update(
                "UPDATE executions SET settlement_status = CAST('settled' AS execution_settlement_status) WHERE id = ?",
                terminalId);

        List<Long> ids = repository.findNonTerminalTradeIds(MAX_AGE_SECONDS, 10, 100);

        assertThat(ids).contains(recentId).doesNotContain(staleId, terminalId);
    }

    /** @param createdAtSql {@code null} for default {@code NOW()}, else a SQL expression (no bind). */
    private long insertTradeExecution(String key, String createdAtSql) {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', 'BUY', 'AAPL', 1, 5, 'DAY',
                          NOW(), NOW(), 'h', NULL, 1
                        )
                        """,
                orderId,
                accountId,
                "repo-age-" + key + "-" + orderId);
        String createdClause = createdAtSql == null ? "DEFAULT" : createdAtSql;
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, settlement_status, raw_envelope_json, created_at
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          1, 5, 0, 1,
                          CAST('TRADE' AS execution_exec_type),
                          CAST('executed' AS execution_settlement_status),
                          CAST('{}' AS JSONB),
                          %s
                        )
                        """
                        .formatted(createdClause),
                orderId,
                accountId,
                "vref-" + key + "-" + orderId);
        return jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1",
                Long.class,
                orderId);
    }
}
