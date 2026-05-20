package com.balh.oms.ledger;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Places a synchronous Ledger inflight debit (buying-power hold) at order accept time.
 *
 * <p>Uses {@code POST /transactions} with {@code sync: true} and {@code inflight: true}.
 * Call happens inside the same OMS DB transaction as the order insert; keep Ledger latency low.
 *
 * <p>Wed-demo: returns the Ledger {@code transactionId} from the response so the caller can
 * persist it for the lifecycle reconciler ({@code PUT /transactions/inflight/{txID}} addressing).
 * Implementations that cannot extract the id (e.g. test stubs) return {@code null} — the caller
 * stores {@code null} in the outbox row and the lifecycle reconciler skips it (cannot settle a
 * hold without the txn id; same shape as a pre-V32 outbox row).
 */
public interface LedgerInflightReservationClient {

    /**
     * Places an inflight debit for {@code holdAmount} (notional + fee) from {@code sourceBalanceId}
     * to the configured hold destination balance.
     *
     * @param orderId        used for idempotent {@code reference} ({@code oms:order:{uuid}})
     * @param sourceBalanceId customer cash balance (Ledger {@code balance_id})
     * @param holdAmount     positive total cash to reserve (notional + estimated commission)
     * @return Ledger {@code transactionId} (e.g. {@code "txn_<uuid>"}) parsed from the
     *     response, or {@code null} when the implementation does not surface it (test stubs;
     *     legacy callers should treat null as "lifecycle commit/void not available for this hold").
     */
    String placeBuyFundsHold(UUID orderId, String sourceBalanceId, BigDecimal holdAmount)
            throws LedgerReservationException;

    /**
     * @deprecated use {@link #placeBuyFundsHold}; retained for tests that only size notional.
     */
    @Deprecated
    default String placeBuyNotionalHold(
            UUID orderId, String sourceBalanceId, BigDecimal quantity, BigDecimal limitPrice)
            throws LedgerReservationException {
        if (quantity == null || limitPrice == null) {
            throw new LedgerReservationException("quantity and limitPrice required for notional hold");
        }
        return placeBuyFundsHold(orderId, sourceBalanceId, quantity.multiply(limitPrice));
    }

    final class LedgerReservationException extends Exception {
        public LedgerReservationException(String message) {
            super(message);
        }

        public LedgerReservationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
