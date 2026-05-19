package com.balh.oms.ledger;

/**
 * Delivers {@code ledger_settlement_outbox} rows to Ledger HTTP (contract is product-specific; v1 posts a JSON
 * envelope for Ledger to interpret).
 */
public interface LedgerSettlementPostingClient {

    /**
     * POST a single settlement outbox leg to Ledger. {@code legKind} selects which Ledger leg
     * to produce ({@code cash}, {@code cash-base}, {@code cash-quote}, {@code fee}). Idempotency
     * is best-effort on a deterministic transaction {@code reference}; the same {@code outboxId}
     * may be retried.
     *
     * @throws LedgerSettlementPostingException on non-2xx, transport errors, or unknown leg
     */
    void postSettlementOutbox(
            long outboxId, long executionId, String toSettlementStatus, String legKind, String payloadJson)
            throws LedgerSettlementPostingException;

    final class LedgerSettlementPostingException extends Exception {
        public LedgerSettlementPostingException(String message) {
            super(message);
        }

        public LedgerSettlementPostingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
