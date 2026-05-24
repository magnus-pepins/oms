package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.settlement.BrokerFixtureRow;
import com.balh.oms.settlement.MarkTradeFailedResult;
import com.balh.oms.settlement.SettlementConfirmProcessor;
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

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementControllerIntegrationTest extends AbstractPostgresIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    SettlementConfirmProcessor processor;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void rejectsWithoutInternalApiKey() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> res = http.postForEntity(
                base() + "/broker-confirms",
                new HttpEntity<>(new SettlementController.BrokerConfirmIngestRequest(List.of(1L)), h),
                String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void listExecutions_rejectsWithoutInternalApiKey() {
        ResponseEntity<String> res = http.getForEntity(base() + "/executions?orderId=" + UUID.randomUUID(), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void listExecutions_badRequestWhenNoOrderIdAndNoTimeWindow() {
        ResponseEntity<String> res =
                http.exchange(base() + "/executions", HttpMethod.GET, new HttpEntity<>(headers()), String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void listExecutions_badRequestWhenInvalidSettlementStatus() {
        UUID orderId = UUID.randomUUID();
        ResponseEntity<String> res =
                http.exchange(
                        base() + "/executions?orderId=" + orderId + "&settlementStatus=not_an_enum",
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void listExecutions_byOrderId_returnsJoinedOrderFieldsAndSettlementDates() {
        long exId = seedTradeExecutionWithDates(
                LocalDate.parse("2026-05-21"), LocalDate.parse("2026-05-25"));
        UUID orderId =
                jdbc.queryForObject("SELECT order_id FROM executions WHERE id = ?", UUID.class, exId);
        ResponseEntity<SettlementExecutionsPageResponse> res =
                http.exchange(
                        base() + "/executions?orderId=" + orderId + "&limit=20",
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).hasSize(1);
        SettlementExecutionResponse row = res.getBody().items().getFirst();
        assertThat(row.id()).isEqualTo(exId);
        assertThat(row.orderId()).isEqualTo(orderId);
        assertThat(row.orderStatus()).isEqualTo("FILLED");
        assertThat(row.side()).isEqualTo("BUY");
        assertThat(row.instrumentSymbol()).isEqualTo("AAPL");
        assertThat(row.settlementStatus()).isEqualTo("executed");
        assertThat(row.execType()).isEqualTo("TRADE");
        assertThat(row.tradeDate()).isEqualTo(LocalDate.parse("2026-05-21"));
        assertThat(row.expectedSettlementDate()).isEqualTo(LocalDate.parse("2026-05-25"));
    }

    @Test
    void listExecutions_byOrderId_returnsJoinedOrderFields() {
        long exId = seedTradeExecution();
        UUID orderId =
                jdbc.queryForObject("SELECT order_id FROM executions WHERE id = ?", UUID.class, exId);
        ResponseEntity<SettlementExecutionsPageResponse> res =
                http.exchange(
                        base() + "/executions?orderId=" + orderId + "&limit=20",
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).hasSize(1);
        SettlementExecutionResponse row = res.getBody().items().getFirst();
        assertThat(row.id()).isEqualTo(exId);
        assertThat(row.orderId()).isEqualTo(orderId);
        assertThat(row.orderStatus()).isEqualTo("FILLED");
        assertThat(row.side()).isEqualTo("BUY");
        assertThat(row.instrumentSymbol()).isEqualTo("AAPL");
        assertThat(row.settlementStatus()).isEqualTo("executed");
        assertThat(row.execType()).isEqualTo("TRADE");
    }

    @Test
    void listExecutions_byTimeWindow_withoutOrderId() {
        long exId = seedTradeExecution();
        Timestamp createdTs =
                jdbc.queryForObject("SELECT created_at FROM executions WHERE id = ?", Timestamp.class, exId);
        Instant created = createdTs.toInstant();
        Instant from = created.minusSeconds(3600);
        Instant to = created.plusSeconds(3600);
        ResponseEntity<SettlementExecutionsPageResponse> res =
                http.exchange(
                        base() + "/executions?from=" + from + "&to=" + to,
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().items()).isNotEmpty();
        assertThat(res.getBody().items().stream().map(SettlementExecutionResponse::id).anyMatch(id -> id == exId))
                .isTrue();
    }

    @Test
    void getExecution_rejectsWithoutInternalApiKey() {
        long exId = seedTradeExecution();
        ResponseEntity<String> res = http.getForEntity(base() + "/executions/" + exId, String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void getExecution_byId_returnsDetailIncludingRawEnvelopeAndSettlementDates() {
        long exId = seedTradeExecutionWithDates(
                LocalDate.parse("2026-05-22"), LocalDate.parse("2026-05-26"));
        jdbc.update("UPDATE executions SET raw_envelope_json = CAST(? AS JSONB) WHERE id = ?", "{\"fill\":1}", exId);
        ResponseEntity<SettlementExecutionDetailResponse> res =
                http.exchange(
                        base() + "/executions/" + exId,
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().id()).isEqualTo(exId);
        assertThat(res.getBody().rawEnvelopeJson()).contains("fill");
        assertThat(res.getBody().instrumentSymbol()).isEqualTo("AAPL");
        assertThat(res.getBody().tradeDate()).isEqualTo(LocalDate.parse("2026-05-22"));
        assertThat(res.getBody().expectedSettlementDate()).isEqualTo(LocalDate.parse("2026-05-26"));
    }

    @Test
    void getExecution_byId_returnsDetailIncludingRawEnvelope() {
        long exId = seedTradeExecution();
        jdbc.update("UPDATE executions SET raw_envelope_json = CAST(? AS JSONB) WHERE id = ?", "{\"fill\":1}", exId);
        ResponseEntity<SettlementExecutionDetailResponse> res =
                http.exchange(
                        base() + "/executions/" + exId,
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().id()).isEqualTo(exId);
        assertThat(res.getBody().rawEnvelopeJson()).contains("fill");
        assertThat(res.getBody().instrumentSymbol()).isEqualTo("AAPL");
    }

    @Test
    void getTimeline_returnsTradeAndExpectedSettlementDates() {
        // Slice 3 pull-through: trade_date + expected_settlement_date are written by
        // SettlementDateCalculator on the projector trade path. The IT seeds them
        // directly via JDBC (cheaper than going through the projector) and asserts
        // the GET /timeline endpoint surfaces them as ISO-8601 date strings —
        // beard-admin Detail panel + customer-frontend BFF both consume this shape.
        LocalDate tradeDate = LocalDate.parse("2026-05-21");
        LocalDate expectedSettlementDate = LocalDate.parse("2026-05-25");
        long exId = seedTradeExecutionWithDates(tradeDate, expectedSettlementDate);

        ResponseEntity<Map> res = http.exchange(
                base() + "/executions/" + exId + "/timeline",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().get("tradeDate")).isEqualTo("2026-05-21");
        assertThat(res.getBody().get("expectedSettlementDate")).isEqualTo("2026-05-25");
        // Currency-blind sanity: timeline still works for an executed-only row.
        assertThat(res.getBody().get("currentSettlementStatus")).isEqualTo("executed");
    }

    @Test
    void getTimeline_legacyExecutionWithoutDates_returnsNullDateFields() {
        // Pre-V58 execution rows have NULL trade_date + expected_settlement_date. The
        // matcher already treats NULL expectedSettlementDate as 'OMS expectation
        // unknown' (no break opened); the timeline must therefore *also* expose
        // them as JSON nulls — not crash, not fall back to a guess.
        long exId = seedTradeExecution();
        jdbc.update("UPDATE executions SET trade_date = NULL, expected_settlement_date = NULL WHERE id = ?", exId);

        ResponseEntity<Map> res = http.exchange(
                base() + "/executions/" + exId + "/timeline",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().get("tradeDate")).isNull();
        assertThat(res.getBody().get("expectedSettlementDate")).isNull();
    }

    @Test
    void getTimeline_ledgerLegExposesAttemptsAndLastError() {
        // Slice 3 outbox enrichment: the reconciler writes attempts + last_error_text
        // on every failed delivery. The timeline must surface them so beard-admin can
        // show "retried 3x — last error: HTTP 503" without an extra round-trip to the
        // stuck-outbox panel.
        long exId = seedTradeExecution();
        long outboxId = jdbc.queryForObject(
                """
                        INSERT INTO ledger_settlement_outbox (
                          execution_id, to_settlement_status, leg_kind, payload_json,
                          attempts, last_error_text, last_attempt_at
                        ) VALUES (
                          ?, 'settled', 'cash', CAST('{}' AS JSONB),
                          3, 'ledger /transactions HTTP 503: unavailable',
                          NOW() - interval '2 seconds'
                        ) RETURNING id
                        """,
                Long.class,
                exId);

        ResponseEntity<Map> res = http.exchange(
                base() + "/executions/" + exId + "/timeline",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        List<Map<String, Object>> legs = (List<Map<String, Object>>) res.getBody().get("ledgerLegs");
        assertThat(legs).hasSize(1);
        Map<String, Object> leg = legs.getFirst();
        assertThat(((Number) leg.get("outboxId")).longValue()).isEqualTo(outboxId);
        assertThat(((Number) leg.get("attempts")).intValue()).isEqualTo(3);
        assertThat((String) leg.get("lastErrorText")).contains("HTTP 503");
        assertThat(leg.get("lastAttemptAt")).isNotNull();
        assertThat(leg.get("postedAt")).isNull();
        assertThat(leg.get("skippedAt")).isNull();
        assertThat(leg.get("skipReason")).isNull();
    }

    @Test
    void getTimeline_ledgerLegExposesSkipReason() {
        // V43 tombstone path: when the reconciler decides a row is unrecoverable
        // (customer balance never funded, indicator never resolved), it sets
        // skipped_at + skip_reason and stops retrying. The timeline distinguishes
        // this from "still failing" so operators can tell which queue to consult.
        long exId = seedTradeExecution();
        jdbc.update(
                """
                        INSERT INTO ledger_settlement_outbox (
                          execution_id, to_settlement_status, leg_kind, payload_json,
                          attempts, last_error_text, last_attempt_at,
                          skipped_at, skip_reason
                        ) VALUES (
                          ?, 'settled', 'cash', CAST('{}' AS JSONB),
                          5, 'customer balance not found in Ledger: indicator=inv-…',
                          NOW() - interval '10 seconds',
                          NOW() - interval '5 seconds', 'unfunded_balance'
                        )
                        """,
                exId);

        ResponseEntity<Map> res = http.exchange(
                base() + "/executions/" + exId + "/timeline",
                HttpMethod.GET,
                new HttpEntity<>(headers()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        List<Map<String, Object>> legs = (List<Map<String, Object>>) res.getBody().get("ledgerLegs");
        assertThat(legs).hasSize(1);
        Map<String, Object> leg = legs.getFirst();
        assertThat((String) leg.get("skipReason")).isEqualTo("unfunded_balance");
        assertThat(leg.get("skippedAt")).isNotNull();
        assertThat(((Number) leg.get("attempts")).intValue()).isEqualTo(5);
    }

    @Test
    void getTimeline_notFound_returns404() {
        ResponseEntity<String> res =
                http.exchange(
                        base() + "/executions/999999999/timeline",
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void getExecution_notFound_returns404() {
        ResponseEntity<String> res =
                http.exchange(
                        base() + "/executions/999999999",
                        HttpMethod.GET,
                        new HttpEntity<>(headers()),
                        String.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void advanceOneStep_notFound_returns404() {
        HttpHeaders h = headers();
        ResponseEntity<SettlementController.SettlementStepResponse> res = http.exchange(
                base() + "/executions/999999999/advance-one-step",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void brokerConfirmsThenProcessPending_settlesSeededExecution() {
        long exId = seedTradeExecution();
        HttpHeaders h = headers();
        ResponseEntity<SettlementController.BrokerConfirmIngestResponse> ingest = http.exchange(
                base() + "/broker-confirms",
                HttpMethod.POST,
                new HttpEntity<>(new SettlementController.BrokerConfirmIngestRequest(List.of(exId)), h),
                new ParameterizedTypeReference<>() {});
        assertThat(ingest.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ingest.getBody()).isNotNull();
        assertThat(ingest.getBody().insertedRows()).isEqualTo(1);

        ResponseEntity<SettlementController.BrokerConfirmProcessResponse> proc = http.exchange(
                base() + "/process-pending",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(proc.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(proc.getBody()).isNotNull();
        assertThat(proc.getBody().processedRows()).isEqualTo(1);

        assertThat(jdbc.queryForObject(
                        "SELECT settlement_status::text FROM executions WHERE id = ?",
                        String.class,
                        exId))
                .isEqualTo("settled");
    }

    @Test
    void advanceOneStepRepeatedly_reachesSettled() {
        long exId = seedTradeExecution();
        HttpHeaders h = headers();
        String[] chain = {"matched", "confirmed", "settling", "settled"};
        for (String expected : chain) {
            ResponseEntity<SettlementController.SettlementStepResponse> res = http.exchange(
                    base() + "/executions/" + exId + "/advance-one-step",
                    HttpMethod.POST,
                    new HttpEntity<>(h),
                    new ParameterizedTypeReference<>() {});
            assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(res.getBody()).isNotNull();
            assertThat(res.getBody().settlementStatus()).isEqualTo(expected);
        }
    }

    @Test
    void importJson_resolvesByExecutionIdAndByVenueRef() {
        long exId = seedTradeExecution();
        UUID accountId = jdbc.queryForObject("SELECT account_id FROM executions WHERE id = ?", UUID.class, exId);
        String vref = jdbc.queryForObject("SELECT venue_exec_ref FROM executions WHERE id = ?", String.class, exId);
        HttpHeaders h = headers();
        var body = new SettlementController.BrokerFixtureImportRequest(
                List.of(new BrokerFixtureRow(exId, null, null), new BrokerFixtureRow(null, accountId, vref)));
        ResponseEntity<SettlementController.BrokerFixtureImportResponse> res = http.exchange(
                base() + "/broker-confirms/import-json",
                HttpMethod.POST,
                new HttpEntity<>(body, h),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().insertedRows()).isEqualTo(1);
        assertThat(res.getBody().skippedUnresolvedRows()).isZero();
    }

    @Test
    void markTradeFailed_whenAlreadySettled_returns409() {
        long exId = seedTradeExecution();
        HttpHeaders h = headers();
        for (int i = 0; i < 4; i++) {
            ResponseEntity<SettlementController.SettlementStepResponse> step = http.exchange(
                    base() + "/executions/" + exId + "/advance-one-step",
                    HttpMethod.POST,
                    new HttpEntity<>(h),
                    new ParameterizedTypeReference<>() {});
            assertThat(step.getStatusCode()).isEqualTo(HttpStatus.OK);
        }
        ResponseEntity<?> fail = http.exchange(
                base() + "/executions/" + exId + "/mark-failed",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(fail.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(processor.markTradeFailed(exId)).isEqualTo(MarkTradeFailedResult.ALREADY_SETTLED);
    }

    @Test
    void markTradeFailed_whenPendingConfirm_clearsQueue() {
        long exId = seedTradeExecution();
        processor.registerBrokerConfirms(List.of(exId));
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ? AND applied_at IS NULL",
                        Integer.class,
                        exId))
                .isEqualTo(1);
        HttpHeaders h = headers();
        ResponseEntity<?> res = http.exchange(
                base() + "/executions/" + exId + "/mark-failed",
                HttpMethod.POST,
                new HttpEntity<>(h),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(jdbc.queryForObject("SELECT settlement_status::text FROM executions WHERE id = ?", String.class, exId))
                .isEqualTo("failed");
        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM broker_settlement_confirm WHERE execution_id = ? AND applied_at IS NULL",
                        Integer.class,
                        exId))
                .isZero();
    }

    private long seedTradeExecutionWithDates(LocalDate tradeDate, LocalDate expectedSettlementDate) {
        long exId = seedTradeExecution();
        jdbc.update(
                "UPDATE executions SET trade_date = ?, expected_settlement_date = ? WHERE id = ?",
                Date.valueOf(tradeDate),
                Date.valueOf(expectedSettlementDate),
                exId);
        return exId;
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
                "ctl-it-" + orderId);
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
                        ) VALUES (?, 'AAPL', CAST(? AS UUID), 10, 0, 10, 0)
                        """,
                accountId,
                "a0000001-0000-4000-8000-000000000001");
        return exId;
    }

    private String base() {
        return "http://localhost:" + port + "/internal/v1/settlement";
    }

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(ApiKeyFilter.HEADER, "test-key");
        return h;
    }
}
