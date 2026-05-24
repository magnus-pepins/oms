package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/** Phase E Slice 13a (gap plan §5.18): settlement ops Micrometer gauges. */
class SettlementOpsMetricsPublisherIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired ReconciliationBreakRepository reconciliationBreaks;
    @Autowired SettlementOpsMetricsPublisher publisher;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void pollMetrics_reflectsOpenBreaksStuckOutboxFailsAndPendingCorporateActions() {
        reconciliationBreaks.insert(
                new ReconciliationBreakRepository.InsertCommand(
                        ReconciliationBreakRepository.BREAK_POSITION_MISMATCH,
                        ReconciliationBreakRepository.SEVERITY_HIGH,
                        ReconciliationBreakRepository.SOURCE_BROKER,
                        null,
                        null,
                        UUID.randomUUID(),
                        LocalDate.of(2026, 5, 20),
                        "{}",
                        "test"));
        reconciliationBreaks.insert(
                new ReconciliationBreakRepository.InsertCommand(
                        ReconciliationBreakRepository.BREAK_CASH_MISMATCH,
                        ReconciliationBreakRepository.SEVERITY_HIGH,
                        ReconciliationBreakRepository.SOURCE_BROKER,
                        null,
                        null,
                        UUID.randomUUID(),
                        LocalDate.of(2026, 5, 20),
                        "{}",
                        "test"));

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
                "ops-metrics-" + orderId);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, settlement_status
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          1, 5, 0, 1,
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          CAST('settled' AS execution_settlement_status)
                        )
                        """,
                orderId,
                accountId,
                "vref-metrics-" + orderId);
        long exId = jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
        jdbc.update(
                """
                        INSERT INTO ledger_settlement_outbox (
                          execution_id, to_settlement_status, leg_kind, payload_json, attempts
                        ) VALUES (?, 'settled', 'cash', CAST('{}' AS JSONB), 3)
                        """,
                exId);

        jdbc.update(
                """
                        INSERT INTO broker_settlement_fail_batch (
                          broker_id, broker_file_id, business_date, schema_version,
                          file_sha256_hex, source, file_name, status
                        ) VALUES (
                          'DEFAULT', 'fail-metrics-1', '2026-05-20', 1,
                          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                          'test', 'fail.json', 'parsed'
                        )
                        """);
        long failBatchId =
                jdbc.queryForObject("SELECT id FROM broker_settlement_fail_batch ORDER BY id DESC LIMIT 1", Long.class);
        jdbc.update(
                """
                        INSERT INTO broker_settlement_fail_row (
                          batch_id, broker_id, broker_fail_id, execution_ref,
                          failed_quantity, intended_settlement_date, raw_row_json
                        ) VALUES (
                          ?, 'DEFAULT', 'BF-1', 'EX-1', 1, '2026-05-20', CAST('{}' AS JSONB)
                        )
                        """,
                failBatchId);

        jdbc.update(
                """
                        INSERT INTO corporate_action_event (
                          instrument_symbol, action_type, effective_date, payload_json
                        ) VALUES ('AAPL', 'CASH_DIVIDEND', '2026-06-01', CAST('{}' AS JSONB))
                        """);

        publisher.pollMetrics();

        assertThat(publisher.openBreakCount(ReconciliationBreakRepository.BREAK_POSITION_MISMATCH))
                .isEqualTo(1);
        assertThat(publisher.openBreakCount(ReconciliationBreakRepository.BREAK_CASH_MISMATCH))
                .isEqualTo(1);
        assertThat(publisher.breakAgeSeconds()).isGreaterThanOrEqualTo(0);
        assertThat(publisher.stuckOutboxTotal()).isEqualTo(1);
        assertThat(publisher.openFailsTotal()).isEqualTo(1);
        assertThat(publisher.pendingCorporateActions()).isEqualTo(1);
    }
}
