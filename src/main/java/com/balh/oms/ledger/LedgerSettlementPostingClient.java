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

        /**
         * Classifies why posting did not succeed so the reconciler can publish skips and failures
         * as distinct counters (otherwise the demo case where customer balances aren't funded
         * yet inflates the "failed" rate and drowns the real-failure signal). Default is
         * {@link Reason#FAILED}; explicit {@link Reason#SKIPPED_UNFUNDED_BALANCE} is reserved
         * for the well-known "customer balance not found in Ledger" path in
         * {@link LedgerSettlementLegPoster}.
         */
        public enum Reason {
            FAILED,
            SKIPPED_UNFUNDED_BALANCE
        }

        private final Reason reason;

        public LedgerSettlementPostingException(String message) {
            this(Reason.FAILED, message, null);
        }

        public LedgerSettlementPostingException(String message, Throwable cause) {
            this(Reason.FAILED, message, cause);
        }

        public LedgerSettlementPostingException(Reason reason, String message) {
            this(reason, message, null);
        }

        public LedgerSettlementPostingException(Reason reason, String message, Throwable cause) {
            super(message, cause);
            this.reason = reason == null ? Reason.FAILED : reason;
        }

        public Reason reason() {
            return reason;
        }
    }
}
