package com.balh.oms.ledger;

/**
 * Delivers {@code ledger_settlement_outbox} rows to Ledger HTTP (contract is product-specific; v1 posts a JSON
 * envelope for Ledger to interpret).
 */
public interface LedgerSettlementPostingClient {

    /**
     * POST settlement outbox payload to Ledger. Idempotency is Ledger-side (same {@code outboxId} may be retried).
     *
     * @throws LedgerSettlementPostingException on non-2xx or transport errors
     */
    void postSettlementOutbox(long outboxId, long executionId, String toSettlementStatus, String payloadJson)
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
