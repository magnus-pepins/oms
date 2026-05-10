package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.reconciler.LedgerSettlementOutboxReconciler;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LedgerSettlementOutboxReconciler} POSTs locked rows to Ledger and sets {@code posted_at} on success.
 */
class LedgerSettlementOutboxReconcilerIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final UUID DEFAULT_CUSTODY =
            UUID.fromString("a0000001-0000-4000-8000-000000000001");

    private static volatile WireMockServer ledgerWireMock;

    @DynamicPropertySource
    static void registerLedgerSettlementReconciler(DynamicPropertyRegistry registry) {
        synchronized (LedgerSettlementOutboxReconcilerIntegrationTest.class) {
            if (ledgerWireMock == null) {
                ledgerWireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
                ledgerWireMock.start();
            }
        }
        registry.add("oms.ledger.enabled", () -> "true");
        registry.add("oms.ledger.base-url", () -> "http://127.0.0.1:" + ledgerWireMock.port());
        registry.add("oms.ledger.api-key", () -> "it-key");
        registry.add("oms.ledger.settlement-outbox-reconciler-enabled", () -> "true");
        registry.add("oms.ledger.settlement-outbox-reconciler-age-ms", () -> "0");
        registry.add("oms.ledger.settlement-posting-http-path", () -> "/internal/v0/settlement-outbox");
    }

    @AfterAll
    static void stopLedgerWireMock() {
        if (ledgerWireMock != null) {
            ledgerWireMock.stop();
            ledgerWireMock = null;
        }
    }

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    LedgerSettlementOutboxRepository ledgerSettlementOutboxRepository;

    @Autowired
    LedgerSettlementOutboxReconciler ledgerSettlementOutboxReconciler;

    @BeforeEach
    void reset() {
        ledgerWireMock.resetAll();
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void reconcilerPostsToLedgerAndSetsPostedAt() {
        ledgerWireMock.stubFor(post(urlPathEqualTo("/internal/v0/settlement-outbox"))
                .withHeader("X-Ledger-Key", equalTo("it-key"))
                .willReturn(aResponse().withStatus(204)));

        long exId = insertFilledBuyExecution();
        ledgerSettlementOutboxRepository.insertIgnore(exId, "settled", "{\"event\":\"SETTLEMENT_SETTLED\"}");
        jdbc.update(
                "UPDATE ledger_settlement_outbox SET created_at = NOW() - interval '1 second' WHERE execution_id = ?",
                exId);

        assertThat(jdbc.queryForObject(
                        "SELECT posted_at IS NULL FROM ledger_settlement_outbox WHERE execution_id = ?",
                        Boolean.class,
                        exId))
                .isTrue();

        ledgerSettlementOutboxReconciler.runOnce();

        ledgerWireMock.verify(1, postRequestedFor(urlPathEqualTo("/internal/v0/settlement-outbox")));
        assertThat(jdbc.queryForObject(
                        "SELECT posted_at IS NOT NULL FROM ledger_settlement_outbox WHERE execution_id = ?",
                        Boolean.class,
                        exId))
                .isTrue();
    }

    @Test
    void reconcilerLeavesPostedAtNullWhenLedgerReturnsError() {
        ledgerWireMock.stubFor(post(urlPathEqualTo("/internal/v0/settlement-outbox"))
                .withHeader("X-Ledger-Key", equalTo("it-key"))
                .willReturn(aResponse().withStatus(503).withBody("unavailable")));

        long exId = insertFilledBuyExecution();
        ledgerSettlementOutboxRepository.insertIgnore(exId, "settled", "{}");
        jdbc.update(
                "UPDATE ledger_settlement_outbox SET created_at = NOW() - interval '1 second' WHERE execution_id = ?",
                exId);

        ledgerSettlementOutboxReconciler.runOnce();

        assertThat(jdbc.queryForObject(
                        "SELECT posted_at IS NULL FROM ledger_settlement_outbox WHERE execution_id = ?",
                        Boolean.class,
                        exId))
                .isTrue();
    }

    private long insertFilledBuyExecution() {
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
                "ledger-settle-recon-" + orderId);
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
                "vref-recon-" + orderId);
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', ?, 10, 0, 10, 0)
                        """,
                accountId,
                DEFAULT_CUSTODY);
        return jdbc.queryForObject(
                "SELECT id FROM executions WHERE order_id = ? ORDER BY id DESC LIMIT 1", Long.class, orderId);
    }
}
