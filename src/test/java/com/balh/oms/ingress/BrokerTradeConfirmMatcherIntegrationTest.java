package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.settlement.ReconciliationBreakRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Slice 3 of gap plan §5.2: economic broker confirm matcher.
 *
 * <p>Three match outcomes covered:
 * <ul>
 *   <li>matched — economic fields agree; matcher enqueues a
 *       {@code broker_settlement_confirm} row so the existing v1 pipeline advances state.</li>
 *   <li>mismatch — at least one field disagrees; matcher opens a
 *       {@code reconciliation_breaks} row of type {@code trade_mismatch} and does
 *       <strong>not</strong> enqueue the v1 confirm.</li>
 *   <li>unresolved — no execution found for {@code (accountId, venueExecRef)}; matcher opens
 *       a {@code reconciliation_breaks} row of type {@code unresolved_confirm}.</li>
 * </ul>
 *
 * <p>Slice 2a of gap plan §5.3 layers in the settlement-date axis: the matcher now also
 * compares broker {@code settlementDate} against the OMS-stored
 * {@code executions.expected_settlement_date} (V58 + {@code SettlementDateCalculator}) and
 * opens a side break of type {@code settlement_date_mismatch} (severity {@code medium})
 * when an otherwise-matched confirm disagrees on date — without blocking settlement.
 *
 * <p>Also asserts the {@code GET /reconciliation-breaks} endpoint returns the rows the
 * matcher wrote (gap plan §5.13).
 */
class BrokerTradeConfirmMatcherIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void processPendingMatches_match_enqueuesBrokerSettlementConfirm() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-MATCH-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");

        long confirmId = ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-MATCH-1");

        var matchRes = postProcessPending();
        assertThat(matchRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(matchRes.getBody()).isNotNull();
        assertThat(matchRes.getBody().items()).hasSize(1);
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("matched");
        assertThat(matchRes.getBody().items().getFirst().confirmId()).isEqualTo(confirmId);

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE id = ?",
                        String.class,
                        confirmId))
                .isEqualTo("matched");
        Long resolvedExecutionId = jdbc.queryForObject(
                "SELECT resolved_execution_id FROM broker_trade_confirm WHERE id = ?",
                Long.class,
                confirmId);
        assertThat(resolvedExecutionId).isNotNull();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ?",
                        Integer.class,
                        resolvedExecutionId))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks", Integer.class))
                .isZero();
    }

    @Test
    void processPendingMatches_quantityMismatch_opensTradeMismatchBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-MM-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");

        long confirmId = ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "9", "5.00", "BT-MM-1");
        var matchRes = postProcessPending();
        assertThat(matchRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(matchRes.getBody()).isNotNull();
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("mismatch");

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE id = ?",
                        String.class,
                        confirmId))
                .isEqualTo("mismatch");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'trade_mismatch'",
                        Integer.class))
                .isEqualTo(1);
        String diffJson = jdbc.queryForObject(
                "SELECT diff_json::text FROM reconciliation_breaks WHERE confirm_id = ?",
                String.class,
                confirmId);
        assertThat(diffJson)
                .contains("\"quantity\"")
                .contains("\"broker\": \"9.")
                .contains("\"oms\": \"10.");
    }

    @Test
    void processPendingMatches_noExecution_opensUnresolvedBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-NX-" + UUID.randomUUID();

        long confirmId =
                ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-NX-1");
        var matchRes = postProcessPending();
        assertThat(matchRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(matchRes.getBody()).isNotNull();
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("unresolved");

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE id = ?",
                        String.class,
                        confirmId))
                .isEqualTo("unresolved");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'unresolved_confirm'",
                        Integer.class))
                .isEqualTo(1);
    }

    @Test
    void matchById_appliesOnceThenReturns404ForAlreadyDecided() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-ONCE-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");
        long confirmId = ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-ONCE-1");

        ResponseEntity<SettlementController.BrokerTradeMatchResultResponse> first = postMatchOne(confirmId);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).isNotNull();
        assertThat(first.getBody().outcome()).isEqualTo("matched");

        ResponseEntity<SettlementController.BrokerTradeMatchResultResponse> second = postMatchOne(confirmId);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    /**
     * Gap plan §5.3 Slice 2a: when broker {@code settlementDate} agrees with the OMS-stored
     * {@code expected_settlement_date}, the matched path must NOT open a side break. This
     * case asserts the {@code settlementDate} axis serialises as
     * {@code "match": true} in the confirm's stored {@code match_diff_json} so future
     * regressions in the axis serialiser are caught here.
     */
    @Test
    void processPendingMatches_settlementDateMatches_diffCarriesMatchTrueAxisAndNoBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-SDM-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");

        long confirmId = ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-SDM-OK-1");
        var matchRes = postProcessPending();
        assertThat(matchRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("matched");

        String diffJson = jdbc.queryForObject(
                "SELECT match_diff_json::text FROM broker_trade_confirm WHERE id = ?", String.class, confirmId);
        assertThat(diffJson)
                .contains("\"settlementDate\"")
                .contains("\"broker\": \"2026-05-27\"")
                .contains("\"oms\": \"2026-05-27\"")
                .contains("\"match\": true");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks", Integer.class))
                .isZero();
    }

    /**
     * Gap plan §5.3 Slice 2a: when broker {@code settlementDate} disagrees with OMS
     * {@code expected_settlement_date} on an otherwise-matched confirm, the matcher opens
     * a {@code settlement_date_mismatch} break (severity {@code medium}) AND still marks
     * the confirm {@code matched} AND still enqueues the v1 {@code broker_settlement_confirm}
     * so settlement is not blocked — the broker is authoritative on actual settlement
     * date, and the break exists only for calendar / config drift visibility.
     */
    @Test
    void processPendingMatches_settlementDateMismatchOnMatched_opensSideBreakSeverityMedium() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-SDM-DRIFT-" + UUID.randomUUID();
        seedTradeExecution(
                accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", LocalDate.of(2026, 5, 28));

        long confirmId =
                ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-SDM-DRIFT-1");
        var matchRes = postProcessPending();
        assertThat(matchRes.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("matched");

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE id = ?", String.class, confirmId))
                .isEqualTo("matched");
        Long resolvedExecutionId = jdbc.queryForObject(
                "SELECT resolved_execution_id FROM broker_trade_confirm WHERE id = ?", Long.class, confirmId);
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ?",
                        Integer.class,
                        resolvedExecutionId))
                .isEqualTo(1);

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks", Integer.class))
                .isEqualTo(1);
        var brk = jdbc.queryForMap(
                "SELECT break_type, severity, source_system, status, confirm_id, execution_id, account_id, diff_json::text AS diff_json"
                        + " FROM reconciliation_breaks WHERE confirm_id = ?",
                confirmId);
        assertThat(brk.get("break_type")).isEqualTo("settlement_date_mismatch");
        assertThat(brk.get("severity")).isEqualTo("medium");
        assertThat(brk.get("source_system")).isEqualTo("broker");
        assertThat(brk.get("status")).isEqualTo("open");
        assertThat(brk.get("confirm_id")).isEqualTo(confirmId);
        assertThat(brk.get("execution_id")).isEqualTo(resolvedExecutionId);
        assertThat(((String) brk.get("diff_json")))
                .contains("\"settlementDate\"")
                .contains("\"broker\": \"2026-05-27\"")
                .contains("\"oms\": \"2026-05-28\"")
                .contains("\"match\": false");
    }

    /**
     * Gap plan §5.3 Slice 2a: when the OMS execution has NULL
     * {@code expected_settlement_date} (legacy row pre-V58 or non-TRADE), the matcher must
     * NOT open a settlement-date break — the OMS expectation is simply unknown. The diff
     * JSON records the reason for ops visibility.
     */
    @Test
    void processPendingMatches_omsExpectedSettlementDateNull_skipsAxisAndNoBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-SDM-NULL-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", null);

        long confirmId =
                ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-SDM-NULL-1");
        var matchRes = postProcessPending();
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("matched");

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks", Integer.class))
                .isZero();
        String diffJson = jdbc.queryForObject(
                "SELECT match_diff_json::text FROM broker_trade_confirm WHERE id = ?", String.class, confirmId);
        assertThat(diffJson)
                .contains("\"settlementDate\"")
                .contains("\"oms\": null")
                .contains("\"reason\": \"oms_expected_unknown_pre_v58_or_non_trade\"");
    }

    @Test
    void processBatchMatches_advancesBatchToAppliedWithCounts() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-BATCH-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");

        long batchId = ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-BATCH-1");

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<SettlementController.BrokerConfirmBatchMatchResponse> res = http.exchange(
                base() + "/broker-trade-confirms/batches/" + batchId + "/process-matches",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().batchId()).isEqualTo(batchId);
        assertThat(res.getBody().status()).isEqualTo("applied");
        assertThat(res.getBody().matchedRows()).isEqualTo(1);
        assertThat(res.getBody().breakRows()).isZero();

        assertThat(jdbc.queryForObject(
                        "SELECT status FROM broker_confirm_batch WHERE id = ?", String.class, batchId))
                .isEqualTo("applied");
        assertThat(jdbc.queryForObject(
                        "SELECT matched_row_count FROM broker_confirm_batch WHERE id = ?", Integer.class, batchId))
                .isEqualTo(1);
    }

    @Test
    void processPendingMatches_grossAmountMismatch_opensTradeMismatchBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-GA-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");

        String json = buildConfirmJson(
                accountId,
                venueExecRef,
                "AAPL",
                "BUY",
                "10",
                "5.00",
                "BT-GA-1",
                "2026-05-23",
                "2026-05-27",
                "60.00",
                null);
        postIngest(json);
        postProcessPending();

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'trade_mismatch'",
                        Integer.class))
                .isEqualTo(1);
    }

    @Test
    void processPendingMatches_tradeDateMismatch_opensTradeMismatchBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-TD-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");

        String json = buildConfirmJson(
                accountId,
                venueExecRef,
                "AAPL",
                "BUY",
                "10",
                "5.00",
                "BT-TD-1",
                "2026-05-24",
                "2026-05-27",
                "50.00",
                null);
        postIngest(json);
        postProcessPending();

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'trade_mismatch'",
                        Integer.class))
                .isEqualTo(1);
    }

    @Test
    void listBrokerConfirmBatches_returnsRecentIngest() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-LB-" + UUID.randomUUID();
        ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-LB-1");

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<SettlementController.BrokerConfirmBatchListResponse> list = http.exchange(
                base() + "/broker-trade-confirms/batches?limit=10&offset=0",
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(list.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(list.getBody()).isNotNull();
        assertThat(list.getBody().items()).isNotEmpty();
        assertThat(list.getBody().items().getFirst().status()).isEqualTo("parsed");
    }

    @Test
    void processPendingMatches_cancelAfterOriginalMatch_marksExecutionFailed() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-CANCEL-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");
        seedBuyPosition(accountId, "AAPL", "10");
        String originalTradeId = "BT-ORIG-CANCEL-1";
        ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", originalTradeId);
        postProcessPending();

        Long executionId = jdbc.queryForObject(
                "SELECT resolved_execution_id FROM broker_trade_confirm WHERE broker_trade_id = ?",
                Long.class,
                originalTradeId);
        assertThat(executionId).isNotNull();

        ingestCorrectionConfirm(
                accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-CANCEL-1", originalTradeId, "cancel");
        var matchRes = postProcessPending();
        assertThat(matchRes.getBody()).isNotNull();
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("matched");

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        executionId))
                .isEqualTo("failed");
        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        String.class,
                        "BT-CANCEL-1"))
                .isEqualTo("matched");
    }

    @Test
    void processPendingMatches_amendAfterOriginalMatch_rematchesExecution() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-AMEND-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");
        String originalTradeId = "BT-ORIG-AMEND-1";
        ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", originalTradeId);
        postProcessPending();

        ingestCorrectionConfirm(
                accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-AMEND-1", originalTradeId, "amend");
        var matchRes = postProcessPending();
        assertThat(matchRes.getBody()).isNotNull();
        assertThat(matchRes.getBody().items().getFirst().outcome()).isEqualTo("matched");

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        String.class,
                        "BT-AMEND-1"))
                .isEqualTo("matched");
        Long executionId = jdbc.queryForObject(
                "SELECT resolved_execution_id FROM broker_trade_confirm WHERE broker_trade_id = ?",
                Long.class,
                "BT-AMEND-1");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ?",
                        Integer.class,
                        executionId))
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    void processPendingMatches_cancelWithoutPriorMatch_opensUnresolvedBreak() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-CANCEL-NX-" + UUID.randomUUID();
        ingestCorrectionConfirm(
                accountId,
                venueExecRef,
                "AAPL",
                "BUY",
                "10",
                "5.00",
                "BT-CANCEL-NX-1",
                "BT-MISSING-ORIG",
                "cancel");
        postProcessPending();

        assertThat(jdbc.queryForObject(
                        "SELECT match_status FROM broker_trade_confirm WHERE broker_trade_id = ?",
                        String.class,
                        "BT-CANCEL-NX-1"))
                .isEqualTo("unresolved");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM reconciliation_breaks WHERE break_type = 'unresolved_confirm'",
                        Integer.class))
                .isEqualTo(1);
    }

    private void ingestCorrectionConfirm(
            UUID accountId,
            String venueExecRef,
            String symbol,
            String side,
            String quantity,
            String price,
            String brokerTradeId,
            String originalBrokerTradeId,
            String correctionType) {
        String json = buildConfirmJson(
                accountId,
                venueExecRef,
                symbol,
                side,
                quantity,
                price,
                brokerTradeId,
                "2026-05-23",
                "2026-05-27",
                new BigDecimal(quantity).multiply(new BigDecimal(price)).toPlainString(),
                null,
                correctionType,
                originalBrokerTradeId);
        postIngest(json);
    }

    private void postIngest(String json) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<SettlementController.BrokerTradeConfirmImportResponse> ingestRes = http.exchange(
                base() + "/broker-trade-confirms/import-json?source=it-matcher",
                HttpMethod.POST,
                new HttpEntity<>(json.getBytes(StandardCharsets.UTF_8), h),
                new ParameterizedTypeReference<>() {});
        assertThat(ingestRes.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void matchByBrokerIdAndVenueRef_whenAccountIdOmitted() {
        UUID accountId = UUID.randomUUID();
        UUID custodyId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO custody_accounts (id, broker_id, account_type, csd_or_book_ref, currency_class)
                        VALUES (?, 'broker_x', 'omnibus', '', 'MULTI')
                        """,
                custodyId);
        String venueExecRef = "EX-BROKER-ONLY-" + UUID.randomUUID();
        seedTradeExecution(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00");
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id, currency,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, 'AAPL', ?, 'USD', 10, 10, 0, 0)
                        """,
                accountId,
                custodyId);

        long confirmId = ingestSingleConfirmWithoutAccount(
                venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-BROKER-ONLY-1");
        postProcessPending();

        String status = jdbc.queryForObject(
                "SELECT match_status FROM broker_trade_confirm WHERE id = ?",
                String.class,
                confirmId);
        assertThat(status).isEqualTo("matched");
    }

    private long ingestSingleConfirmWithoutAccount(
            String venueExecRef, String symbol, String side, String quantity, String price, String brokerTradeId) {
        String json = buildConfirmJsonWithoutAccount(
                venueExecRef, symbol, side, quantity, price, brokerTradeId, "2026-05-23", "2026-05-27", "50.00", null);
        postIngest(json);
        return jdbc.queryForObject(
                "SELECT id FROM broker_trade_confirm WHERE broker_trade_id = ?",
                Long.class,
                brokerTradeId);
    }

    private String buildConfirmJsonWithoutAccount(
            String venueExecRef,
            String symbol,
            String side,
            String quantity,
            String price,
            String brokerTradeId,
            String tradeDate,
            String settlementDate,
            String grossAmount,
            String feesJson) {
        String feesBlock = feesJson == null ? "" : ", \"fees\": " + feesJson;
        return ("""
                {
                  "schemaVersion": 1,
                  "brokerId": "broker_x",
                  "fileId": "%FILE%",
                  "businessDate": "2026-05-23",
                  "rows": [
                    {
                      "brokerTradeId": "%TRADE%",
                      "venueExecRef": "%VREF%",
                      "instrument": {
                        "symbol": "%SYMBOL%",
                        "currency": "USD"
                      },
                      "side": "%SIDE%",
                      "quantity": "%QTY%",
                      "price": "%PRICE%",
                      "grossAmount": "%GROSS%",
                      "tradeDate": "%TDATE%",
                      "settlementDate": "%SDATE%",
                      "settlementCurrency": "USD",
                      "correctionType": "new"%FEES%
                    }
                  ]
                }
                """)
                .replace("%FILE%", "F-" + UUID.randomUUID())
                .replace("%TRADE%", brokerTradeId)
                .replace("%VREF%", venueExecRef)
                .replace("%SYMBOL%", symbol)
                .replace("%SIDE%", side)
                .replace("%QTY%", quantity)
                .replace("%PRICE%", price)
                .replace("%GROSS%", grossAmount)
                .replace("%TDATE%", tradeDate)
                .replace("%SDATE%", settlementDate)
                .replace("%FEES%", feesBlock);
    }

    private String buildConfirmJson(
            UUID accountId,
            String venueExecRef,
            String symbol,
            String side,
            String quantity,
            String price,
            String brokerTradeId,
            String tradeDate,
            String settlementDate,
            String grossAmount,
            String feesJson) {
        return buildConfirmJson(
                accountId,
                venueExecRef,
                symbol,
                side,
                quantity,
                price,
                brokerTradeId,
                tradeDate,
                settlementDate,
                grossAmount,
                feesJson,
                "new",
                null);
    }

    private String buildConfirmJson(
            UUID accountId,
            String venueExecRef,
            String symbol,
            String side,
            String quantity,
            String price,
            String brokerTradeId,
            String tradeDate,
            String settlementDate,
            String grossAmount,
            String feesJson,
            String correctionType,
            String originalBrokerTradeId) {
        String feesBlock = feesJson == null ? "" : ", \"fees\": " + feesJson;
        String originalBlock =
                originalBrokerTradeId == null
                        ? ""
                        : ", \"originalBrokerTradeId\": \"" + originalBrokerTradeId + "\"";
        return ("""
                {
                  "schemaVersion": 1,
                  "brokerId": "broker_x",
                  "fileId": "%FILE%",
                  "businessDate": "2026-05-23",
                  "rows": [
                    {
                      "brokerTradeId": "%TRADE%",
                      "venueExecRef": "%VREF%",
                      "accountId": "%ACCT%",
                      "instrument": {
                        "symbol": "%SYMBOL%",
                        "currency": "USD"
                      },
                      "side": "%SIDE%",
                      "quantity": "%QTY%",
                      "price": "%PRICE%",
                      "grossAmount": "%GROSS%",
                      "tradeDate": "%TDATE%",
                      "settlementDate": "%SDATE%",
                      "settlementCurrency": "USD",
                      "correctionType": "%CORR%"%ORIG%%FEES%
                    }
                  ]
                }
                """)
                .replace("%FILE%", "F-" + UUID.randomUUID())
                .replace("%TRADE%", brokerTradeId)
                .replace("%VREF%", venueExecRef)
                .replace("%ACCT%", accountId.toString())
                .replace("%SYMBOL%", symbol)
                .replace("%SIDE%", side)
                .replace("%QTY%", quantity)
                .replace("%PRICE%", price)
                .replace("%GROSS%", grossAmount)
                .replace("%TDATE%", tradeDate)
                .replace("%SDATE%", settlementDate)
                .replace("%CORR%", correctionType)
                .replace("%ORIG%", originalBlock)
                .replace("%FEES%", feesBlock);
    }

    @Test
    void listReconciliationBreaks_returnsOpenBreaksByDefault() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-LIST-" + UUID.randomUUID();
        ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-LIST-1");
        postProcessPending();

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<SettlementController.ReconciliationBreaksListResponse> list = http.exchange(
                base() + "/reconciliation-breaks?limit=10&offset=0",
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(list.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(list.getBody()).isNotNull();
        assertThat(list.getBody().status()).isEqualTo("open");
        assertThat(list.getBody().items()).hasSize(1);
        assertThat(list.getBody().items().getFirst().breakType()).isEqualTo("unresolved_confirm");
    }

    @Test
    void getReconciliationBreakById_returnsRowWithDiffJson() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-DETAIL-" + UUID.randomUUID();
        long confirmId =
                ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-DETAIL-1");
        postProcessPending();

        Long breakId = jdbc.queryForObject(
                "SELECT id FROM reconciliation_breaks WHERE confirm_id = ?",
                Long.class,
                confirmId);
        assertThat(breakId).isNotNull();

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<ReconciliationBreakRepository.BreakRow> res = http.exchange(
                base() + "/reconciliation-breaks/" + breakId,
                HttpMethod.GET,
                new HttpEntity<>(h),
                ReconciliationBreakRepository.BreakRow.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().breakType()).isEqualTo("unresolved_confirm");
        assertThat(res.getBody().confirmId()).isEqualTo(confirmId);
    }

    @Test
    void exportReconciliationBreaks_returnsCsvAttachment() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-CSV-" + UUID.randomUUID();
        ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-CSV-1");
        postProcessPending();

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<String> res = http.exchange(
                base() + "/reconciliation-breaks/export?status=open",
                HttpMethod.GET,
                new HttpEntity<>(h),
                String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION)).contains("attachment");
        assertThat(res.getBody()).contains("break_type");
        assertThat(res.getBody()).contains("unresolved_confirm");
    }

    @Test
    void summarizeReconciliationBreaks_countsOpenBreaks() {
        UUID accountId = UUID.randomUUID();
        String venueExecRef = "EX-SUM-" + UUID.randomUUID();
        ingestSingleConfirm(accountId, venueExecRef, "AAPL", "BUY", "10", "5.00", "BT-SUM-1");
        postProcessPending();

        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        ResponseEntity<SettlementController.ReconciliationBreakSummaryResponse> res = http.exchange(
                base() + "/reconciliation-breaks/summary?status=open",
                HttpMethod.GET,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().total()).isGreaterThanOrEqualTo(1);
        assertThat(res.getBody().byBreakType()).containsKey("unresolved_confirm");
    }

    private ResponseEntity<SettlementController.BrokerTradeMatchBatchResponse> postProcessPending() {
        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        return http.exchange(
                base() + "/broker-trade-confirms/process-pending-matches",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
    }

    private ResponseEntity<SettlementController.BrokerTradeMatchResultResponse> postMatchOne(long confirmId) {
        HttpHeaders h = new HttpHeaders();
        h.set(ApiKeyFilter.HEADER, "test-key");
        return http.exchange(
                base() + "/broker-trade-confirms/" + confirmId + "/match",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
    }

    private long ingestSingleConfirm(
            UUID accountId,
            String venueExecRef,
            String symbol,
            String side,
            String quantity,
            String price,
            String brokerTradeId) {
        String json = buildConfirmJson(
                accountId,
                venueExecRef,
                symbol,
                side,
                quantity,
                price,
                brokerTradeId,
                "2026-05-23",
                "2026-05-27",
                new BigDecimal(quantity).multiply(new BigDecimal(price)).toPlainString(),
                null);
        postIngest(json);
        return jdbc.queryForObject(
                "SELECT id FROM broker_trade_confirm WHERE broker_trade_id = ?",
                Long.class,
                brokerTradeId);
    }

    /**
     * Default expected settlement date matches the broker confirm template's hardcoded
     * {@code settlementDate: 2026-05-27} (see {@link #ingestSingleConfirm}), so the
     * existing match-path cases assert "no break" cleanly. Tests that want to drive the
     * settlement-date axis pass an explicit value (or {@code null}) via the overload.
     */
    private static final LocalDate DEFAULT_EXPECTED_SETTLEMENT_DATE = LocalDate.of(2026, 5, 27);

    private void seedBuyPosition(UUID accountId, String symbol, String quantity) {
        jdbc.update(
                """
                        INSERT INTO positions (
                          account_id, instrument_symbol, custody_account_id, currency,
                          quantity_total, quantity_settled, quantity_pending_buy_settle, quantity_pending_sell_settle
                        ) VALUES (?, ?, CAST(? AS UUID), 'USD', CAST(? AS NUMERIC), 0, CAST(? AS NUMERIC), 0)
                        """,
                accountId,
                symbol,
                "a0000001-0000-4000-8000-000000000001",
                quantity,
                quantity);
    }

    private void seedTradeExecution(
            UUID accountId, String venueExecRef, String symbol, String side, String quantity, String price) {
        seedTradeExecution(accountId, venueExecRef, symbol, side, quantity, price, DEFAULT_EXPECTED_SETTLEMENT_DATE);
    }

    private void seedTradeExecution(
            UUID accountId,
            String venueExecRef,
            String symbol,
            String side,
            String quantity,
            String price,
            LocalDate expectedSettlementDate) {
        UUID orderId = UUID.randomUUID();
        jdbc.update(
                """
                        INSERT INTO orders (
                          id, account_id, client_idempotency_key, shard_id, version,
                          status, side, instrument_symbol, quantity, limit_price, time_in_force,
                          received_at, accepted_at, account_id_hash, ledger_balance_id, cum_filled_quantity
                        ) VALUES (
                          ?, ?, ?, 0, 2, 'FILLED', CAST(? AS order_side), ?, CAST(? AS NUMERIC), CAST(? AS NUMERIC), 'DAY',
                          NOW(), NOW(), 'h', NULL, CAST(? AS NUMERIC)
                        )
                        """,
                orderId,
                accountId,
                "matcher-" + orderId,
                side,
                symbol,
                quantity,
                price,
                quantity);
        jdbc.update(
                """
                        INSERT INTO executions (
                          order_id, account_id, venue_id, venue_ts, venue_exec_ref,
                          last_quantity, last_price, leaves_quantity, cum_quantity_after,
                          exec_type, raw_envelope_json, trade_date, expected_settlement_date
                        ) VALUES (
                          ?, ?, 'SIM', NOW(), ?,
                          CAST(? AS NUMERIC), CAST(? AS NUMERIC), 0, CAST(? AS NUMERIC),
                          CAST('TRADE' AS execution_exec_type), CAST('{}' AS JSONB),
                          ?, ?
                        )
                        """,
                orderId,
                accountId,
                venueExecRef,
                quantity,
                price,
                quantity,
                java.sql.Date.valueOf(LocalDate.of(2026, 5, 23)),
                expectedSettlementDate == null ? null : java.sql.Date.valueOf(expectedSettlementDate));
    }

    private String base() {
        return "http://localhost:" + port + "/internal/v1/settlement";
    }
}
