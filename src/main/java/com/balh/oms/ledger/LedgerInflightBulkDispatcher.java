package com.balh.oms.ledger;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Best-effort bulk inflight-hold dispatcher used by {@link LedgerInflightCoalescer} (Phase 4
 * slice 4q of the Aeron Cluster substrate plan).
 *
 * <p>One call posts up to {@code maxBatchSize} BUY notional holds to Ledger
 * {@code POST /transactions/bulk?inflight=true&atomic=false}. The bulk endpoint iterates the
 * batch sequentially server-side within a single HTTP, which is what eliminates the per-order
 * {@code balances.version} OCC race surface that the slice 4p sync path hits at burst load
 * (see runbook "Slice 4p evidence" / "Slice 4q evidence"). {@code atomic=false} is required
 * for partial failure handling: any single hold that hits an OCC race or an insufficient-funds
 * rejection appears in the response {@link Result#failedOrderIds()} set without poisoning the
 * other items in the batch.
 *
 * <p>The interface is deliberately functional / non-Spring so unit tests can stub the dispatcher
 * directly without WireMock; the production {@link RestLedgerInflightBulkDispatcher} owns the
 * actual {@code RestClient} call.
 */
public interface LedgerInflightBulkDispatcher {

    /**
     * Posts {@code items} to Ledger as one bulk inflight transaction request.
     *
     * @return per-item success / failure mapping. {@link Result#failedOrderIds()} is the subset
     *     of {@code items} that Ledger reported as individually failed (e.g. balance OCC race);
     *     items not present in {@code failedOrderIds} succeeded.
     * @throws LedgerInflightBulkException on whole-batch HTTP failure (network, non-2xx with no
     *     parsable per-item failure breakdown, malformed response). The coalescer treats this
     *     as "every item in the batch needs the outbox fallback path" — different from a
     *     {@link Result#failedOrderIds()} response, where the outbox fallback is per-item.
     */
    Result dispatch(List<HoldItem> items) throws LedgerInflightBulkException;

    record HoldItem(UUID orderId, String sourceBalanceId, BigDecimal holdAmount) {}

    record Result(int requested, int succeeded, Set<UUID> failedOrderIds) {
        public Result {
            failedOrderIds = failedOrderIds == null ? Set.of() : Set.copyOf(failedOrderIds);
        }
    }

    final class LedgerInflightBulkException extends Exception {
        public LedgerInflightBulkException(String message) {
            super(message);
        }

        public LedgerInflightBulkException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
