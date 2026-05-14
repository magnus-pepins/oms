package com.balh.oms.ledger;

import com.balh.oms.ledger.LedgerInflightBulkDispatcher.HoldItem;
import com.balh.oms.ledger.LedgerInflightBulkDispatcher.LedgerInflightBulkException;
import com.balh.oms.ledger.LedgerInflightBulkDispatcher.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Targeted coverage for the response-parsing branch of {@link RestLedgerInflightBulkDispatcher}.
 * The HTTP path itself is exercised end-to-end in
 * {@code LedgerInflightCoalescerIntegrationTest} via WireMock; here we just verify that the
 * parser correctly attributes per-item failures (the contract that lets the coalescer route only
 * the failed orderIds to the outbox).
 */
class RestLedgerInflightBulkDispatcherTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final RestLedgerInflightBulkDispatcher dispatcher =
            new RestLedgerInflightBulkDispatcher(
                    /* http = */ null, "test-key", objectMapper, "hold-dest", "EUR", 100);

    @Test
    void appliedResponse_allItemsSucceed() throws Exception {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        List<HoldItem> items = List.of(item(a), item(b));
        String body = """
                {"batch_id":"b1","status":"applied","transaction_count":2}""";
        Result r = dispatcher.parseResponse(items, body, 201);
        assertThat(r.requested()).isEqualTo(2);
        assertThat(r.succeeded()).isEqualTo(2);
        assertThat(r.failedOrderIds()).isEmpty();
    }

    @Test
    void partialResponse_failedOrderIdsExtractedFromErrors() throws Exception {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        List<HoldItem> items = List.of(item(a), item(b));
        String body = """
                {"batch_id":"b2","status":"partial","transaction_count":1,"errors":[
                  "Transaction oms:order:%s: Balance version conflict: source balance was modified by another transaction"
                ]}""".formatted(b);
        Result r = dispatcher.parseResponse(items, body, 201);
        assertThat(r.requested()).isEqualTo(2);
        assertThat(r.succeeded()).isEqualTo(1);
        assertThat(r.failedOrderIds()).containsExactly(b);
    }

    @Test
    void all400Response_withParseableErrors_returnsFailedOrderIds() throws Exception {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        List<HoldItem> items = List.of(item(a), item(b));
        String body = """
                {"error":"All transactions failed","batch_id":"b3","errors":[
                  "Transaction oms:order:%s: insufficient_balance",
                  "Transaction oms:order:%s: insufficient_balance"
                ]}""".formatted(a, b);
        Result r = dispatcher.parseResponse(items, body, 400);
        assertThat(r.requested()).isEqualTo(2);
        assertThat(r.succeeded()).isEqualTo(0);
        assertThat(r.failedOrderIds()).containsExactlyInAnyOrder(a, b);
    }

    @Test
    void all400Response_withoutParseableErrors_throwsBulkException() {
        UUID a = UUID.randomUUID();
        List<HoldItem> items = List.of(item(a));
        String body = """
                {"error":"All transactions failed","batch_id":"b4","errors":[]}""";
        assertThatThrownBy(() -> dispatcher.parseResponse(items, body, 400))
                .isInstanceOf(LedgerInflightBulkException.class)
                .hasMessageContaining("400 without parsable errors");
    }

    @Test
    void unknownReferenceInErrors_isIgnoredNotMisattributed() throws Exception {
        // Defence against a stale-batch / proxy-mux response: a reference we did not submit must
        // not knock one of our items into the failed set.
        UUID a = UUID.randomUUID();
        UUID stranger = UUID.randomUUID();
        List<HoldItem> items = List.of(item(a));
        String body = """
                {"batch_id":"b5","status":"partial","transaction_count":1,"errors":[
                  "Transaction oms:order:%s: someone-else's-failure"
                ]}""".formatted(stranger);
        Result r = dispatcher.parseResponse(items, body, 201);
        assertThat(r.failedOrderIds()).isEmpty();
        assertThat(r.succeeded()).isEqualTo(1);
    }

    @Test
    void emptyBodyOn2xx_isTreatedAsAllApplied() throws Exception {
        UUID a = UUID.randomUUID();
        Result r = dispatcher.parseResponse(List.of(item(a)), "", 201);
        assertThat(r.failedOrderIds()).isEmpty();
        assertThat(r.succeeded()).isEqualTo(1);
    }

    private static HoldItem item(UUID orderId) {
        return new HoldItem(orderId, "src", new BigDecimal("1"), new BigDecimal("1"));
    }
}
