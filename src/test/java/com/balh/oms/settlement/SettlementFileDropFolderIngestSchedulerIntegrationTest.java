package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end drop-folder ingest: {@link SettlementFileDropFolderIngestScheduler} reads {@code *.json} from a host
 * directory and delegates to {@link SettlementFileImportService} (same path as HTTP multipart ingest).
 */
class SettlementFileDropFolderIngestSchedulerIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final Path DROP_ROOT;

    static {
        try {
            DROP_ROOT = Files.createTempDirectory("oms-drop-folder-it-");
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void dropFolderProps(DynamicPropertyRegistry registry) {
        registry.add("oms.settlement.file-import-drop-folder-enabled", () -> "true");
        registry.add("oms.settlement.file-import-drop-folder-path", () -> DROP_ROOT.toAbsolutePath().toString());
        registry.add("oms.settlement.file-import-drop-folder-poll-interval-ms", () -> "600000");
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    SettlementFileDropFolderIngestScheduler scheduler;

    @BeforeEach
    void cleanDbAndDropDir() throws IOException {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
        if (Files.isDirectory(DROP_ROOT)) {
            try (var walk = Files.walk(DROP_ROOT)) {
                walk.sorted((a, b) -> -a.compareTo(b))
                        .filter(p -> !p.equals(DROP_ROOT))
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException ignored) {
                            }
                        });
            }
        }
        Files.createDirectories(DROP_ROOT.resolve(".oms-done"));
        Files.createDirectories(DROP_ROOT.resolve(".oms-failed"));
    }

    @Test
    void pollDropFolder_ingestsJsonAndArchivesToDone() throws Exception {
        long exId = seedTradeExecution();
        String json =
                """
                        {"rows":[{"executionId":%d,"accountId":null,"venueExecRef":null}]}
                        """
                        .formatted(exId);
        Path incoming = DROP_ROOT.resolve("eod-drop.json");
        Files.writeString(incoming, json, StandardCharsets.UTF_8);

        scheduler.pollDropFolder();

        assertThat(Files.exists(incoming)).isFalse();
        assertThat(Files.list(DROP_ROOT.resolve(".oms-done")).anyMatch(p -> p.getFileName().toString().startsWith("eod-drop")))
                .isTrue();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ?",
                        Integer.class,
                        exId))
                .isEqualTo(1);
    }

    private long seedTradeExecution() {
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
                "drop-folder-it-" + orderId);
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
                "vref-df-" + orderId);
        long exId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', CAST(? AS UUID), 10, 0, 10, 0)
                        """,
                accountId,
                "a0000001-0000-4000-8000-000000000001");
        return exId;
    }
}
