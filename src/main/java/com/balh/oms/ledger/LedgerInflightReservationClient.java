package com.balh.oms.ledger;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Places a synchronous Ledger inflight debit (buying-power hold) at order accept time.
 *
 * <p>Uses {@code POST /transactions} with {@code sync: true} and {@code inflight: true}.
 * Call happens inside the same OMS DB transaction as the order insert; keep Ledger latency low.
 */
public interface LedgerInflightReservationClient {

    /**
     * Moves notional {@code quantity * limitPrice} from {@code sourceBalanceId} to the configured
     * hold destination balance as an inflight transaction.
     *
     * @param orderId        used for idempotent {@code reference} ({@code oms:order:{uuid}})
     * @param sourceBalanceId customer cash balance (Ledger {@code balance_id})
     */
    void placeBuyNotionalHold(UUID orderId, String sourceBalanceId, BigDecimal quantity, BigDecimal limitPrice)
            throws LedgerReservationException;

    final class LedgerReservationException extends Exception {
        public LedgerReservationException(String message) {
            super(message);
        }

        public LedgerReservationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
