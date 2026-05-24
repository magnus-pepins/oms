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
    @Autowired CorporateActionRecordDateSnapshotService snapshotService;
    @Autowired CorporateActionElectionService electionService;

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

    @Test
    void processor_tenderOffer_participate_writesCashImpact() {
        UUID accountId = UUID.randomUUID();
        insertPosition(accountId, "AAPL", 100);
        long eventId = insertVoluntaryEvent("TENDER_OFFER", "AAPL", "{\"tenderPricePerShare\":\"10.00\",\"currency\":\"USD\"}");
        snapshotService.captureForEvent(eventId, "AAPL", java.time.LocalDate.of(2026, 5, 1));
        approveElection(eventId, accountId, "PARTICIPATE");

        processorJob.processBatch();

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM corporate_action_cash_impact WHERE corporate_action_event_id = ?",
                        Integer.class,
                        eventId))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT net_amount FROM corporate_action_cash_impact WHERE corporate_action_event_id = ?",
                        BigDecimal.class,
                        eventId))
                .isEqualByComparingTo("1000.00");
    }

    @Test
    void processor_tenderOffer_decline_skipsEntitlement() {
        UUID accountId = UUID.randomUUID();
        insertPosition(accountId, "AAPL", 100);
        long eventId = insertVoluntaryEvent("TENDER_OFFER", "AAPL", "{\"tenderPricePerShare\":\"10.00\",\"currency\":\"USD\"}");
        snapshotService.captureForEvent(eventId, "AAPL", java.time.LocalDate.of(2026, 5, 1));
        approveElection(eventId, accountId, "DECLINE");

        processorJob.processBatch();

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM corporate_action_entitlement WHERE corporate_action_event_id = ?",
                        Integer.class,
                        eventId))
                .isZero();
    }

    @Test
    void processor_rightsIssue_participate_writesRightsPositionImpact() {
        UUID accountId = UUID.randomUUID();
        insertPosition(accountId, "ERIC", 50);
        long eventId =
                insertVoluntaryEvent(
                        "RIGHTS_ISSUE",
                        "ERIC",
                        "{\"rightsPerShare\":\"0.5\",\"rightsSymbol\":\"ERIC.RT\"}");
        snapshotService.captureForEvent(eventId, "ERIC", java.time.LocalDate.of(2026, 5, 1));
        approveElection(eventId, accountId, "SUBSCRIBE");

        processorJob.processBatch();

        assertThat(jdbc.queryForObject(
                        "SELECT quantity_after FROM corporate_action_position_impact WHERE corporate_action_event_id = ?",
                        BigDecimal.class,
                        eventId))
                .isEqualByComparingTo("25");
        assertThat(jdbc.queryForObject(
                        "SELECT instrument_symbol FROM corporate_action_position_impact WHERE corporate_action_event_id = ?",
                        String.class,
                        eventId))
                .isEqualTo("ERIC.RT");
    }

    private void insertPosition(UUID accountId, String symbol, int qty) {
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, ?, ?, ?, ?, 0, 0)
                        """,
                accountId,
                symbol,
                DEFAULT_CUSTODY,
                qty,
                qty);
    }

    private long insertVoluntaryEvent(String actionType, String symbol, String payloadJson) {
        return jdbc.queryForObject(
                """
                        INSERT INTO corporate_action_event (
                          instrument_symbol, action_type, effective_date, payload_json,
                          record_date, payable_date
                        ) VALUES (
                          ?, ?, '2026-06-01', CAST(? AS JSONB), '2026-05-01', '2026-06-15'
                        ) RETURNING id
                        """,
                Long.class,
                symbol,
                actionType,
                payloadJson);
    }

    private void approveElection(long eventId, UUID accountId, String choice) {
        long electionId = electionService.submit(eventId, accountId, choice, "customer@test.local");
        assertThat(electionService.approve(electionId, "ops@test.local"))
                .isEqualTo(CorporateActionElectionService.ApproveResult.APPROVED);
    }
}
