package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.risk.ControlRiskEvaluator;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase E Slice 12a: ISK account mapping, instrument eligibility gate, settlement outbox metadata.
 */
@TestPropertySource(
        properties = {
            "oms.risk.isk-instrument-eligibility-check-enabled=true",
            "oms.risk.sell-position-check-enabled=false",
            "oms.ledger.settlement-outbox-enabled=true"
        })
class IskSettlementHardeningIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final UUID DEFAULT_CUSTODY =
            UUID.fromString("a0000001-0000-4000-8000-000000000001");

    @Autowired JdbcTemplate jdbc;
    @Autowired ControlRiskEvaluator controlRiskEvaluator;
    @Autowired InstrumentSettlementProfileRepository settlementProfiles;
    @Autowired OmsAccountTaxWrapperRepository accountTaxWrapper;
    @Autowired SettlementConfirmProcessor settlementProcessor;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
        jdbc.update("TRUNCATE TABLE instrument_settlement_profile RESTART IDENTITY CASCADE");
    }

    @Test
    void controlRisk_rejectsIskAccountOnNonEligibleInstrument() {
        UUID accountId = UUID.randomUUID();
        accountTaxWrapper.upsert(accountId, OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK, UUID.randomUUID(), "bal-isk");
        settlementProfiles.insert(
                new InstrumentSettlementProfileRepository.InsertCommand(
                        "GME-INST", "GME", null, "XNAS", "XNAS-CAL", "T+2", "USD", false, LocalDate.of(2020, 1, 1), null));

        Optional<RejectCode> code = controlRiskEvaluator.evaluate(sampleOrder(accountId, Side.BUY, "GME"));
        assertThat(code).contains(RejectCode.RISK_ISK_INSTRUMENT_NOT_ELIGIBLE);
    }

    @Test
    void controlRisk_acceptsIskAccountOnEligibleInstrument() {
        UUID accountId = UUID.randomUUID();
        accountTaxWrapper.upsert(accountId, OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK, UUID.randomUUID(), "bal-isk");
        settlementProfiles.insert(
                new InstrumentSettlementProfileRepository.InsertCommand(
                        "AAPL-INST", "AAPL", null, "XNAS", "XNAS-CAL", "T+2", "USD", true, LocalDate.of(2020, 1, 1), null));

        assertThat(controlRiskEvaluator.evaluate(sampleOrder(accountId, Side.BUY, "AAPL"))).isEmpty();
    }

    @Test
    void settlementOutbox_enrichesIskBuyWithTradeFundingClass() {
        UUID orderId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        UUID iskAccountId = UUID.randomUUID();
        accountTaxWrapper.upsert(accountId, OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK, iskAccountId, "bal-isk-buy");

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
                "isk-buy-" + orderId);
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
                "vref-isk-" + orderId);
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

        settlementProcessor.registerAndDrain(List.of(exId), 20, 20);

        assertThat(jdbc.queryForObject(
                        "SELECT payload_json->>'taxWrapper' FROM ledger_settlement_outbox "
                                + "WHERE execution_id = ? AND leg_kind = 'cash'",
                        String.class,
                        exId))
                .isEqualTo("isk");
        assertThat(jdbc.queryForObject(
                        "SELECT payload_json->>'iskAccountId' FROM ledger_settlement_outbox "
                                + "WHERE execution_id = ? AND leg_kind = 'cash'",
                        String.class,
                        exId))
                .isEqualTo(iskAccountId.toString());
        assertThat(jdbc.queryForObject(
                        "SELECT payload_json->>'iskDepositClass' FROM ledger_settlement_outbox "
                                + "WHERE execution_id = ? AND leg_kind = 'cash'",
                        String.class,
                        exId))
                .isEqualTo(IskDepositClass.TRADE_FUNDING);
        assertThat(jdbc.queryForObject(
                        "SELECT payload_json->>'iskDepositClass' FROM ledger_settlement_outbox "
                                + "WHERE execution_id = ? AND leg_kind = 'fee'",
                        String.class,
                        exId))
                .isEqualTo(IskDepositClass.COMMISSION);
    }

    private static Order sampleOrder(UUID accountId, Side side, String symbol) {
        Instant now = Instant.parse("2026-05-20T12:00:00Z");
        return new Order(
                UUID.randomUUID(),
                accountId,
                "idem-" + symbol,
                0,
                0,
                OrderStatus.NEW,
                null,
                side,
                symbol,
                BigDecimal.ONE,
                new BigDecimal("10.00"),
                "DAY",
                now,
                now,
                null,
                "h",
                null,
                BigDecimal.ZERO);
    }
}
