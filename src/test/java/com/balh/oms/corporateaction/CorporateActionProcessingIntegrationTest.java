package com.balh.oms.corporateaction;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource(properties = "oms.corporate-action.processor-enabled=true")
class CorporateActionProcessingIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final UUID DEFAULT_CUSTODY =
            UUID.fromString("a0000001-0000-4000-8000-000000000001");

    @Autowired JdbcTemplate jdbc;
    @Autowired CorporateActionProcessorJob processorJob;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void processor_cashDividend_writesEntitlementAndCashImpact() {
        UUID accountId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', ?, 10, 10, 0, 0)
                        """,
                accountId,
                DEFAULT_CUSTODY);
        long eventId =
                jdbc.queryForObject(
                        """
                                INSERT INTO corporate_action_event (
                                  instrument_symbol, action_type, effective_date, payload_json, payable_date
                                ) VALUES (
                                  'AAPL', 'CASH_DIVIDEND', '2026-06-01',
                                  CAST('{"dividendPerShare":"0.25","currency":"USD"}' AS JSONB),
                                  '2026-06-15'
                                ) RETURNING id
                                """,
                        Long.class);

        processorJob.processBatch();

        assertThat(jdbc.queryForObject(
                        "SELECT processed_at IS NOT NULL FROM corporate_action_event WHERE id = ?",
                        Boolean.class,
                        eventId))
                .isTrue();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM corporate_action_entitlement WHERE corporate_action_event_id = ?",
                        Integer.class,
                        eventId))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT entitlement_amount FROM corporate_action_entitlement WHERE corporate_action_event_id = ?",
                        BigDecimal.class,
                        eventId))
                .isEqualByComparingTo("2.50");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM corporate_action_cash_impact WHERE corporate_action_event_id = ?",
                        Integer.class,
                        eventId))
                .isEqualTo(1);
    }
}
