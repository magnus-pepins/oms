package com.balh.oms.settlement;

/** Outcome of {@link SettlementConfirmProcessor#clearPendingBrokerConfirmsForTradeOrThrow(long)}. */
public enum ClearPendingBrokerConfirmResult {
    /** Execution id not found. */
    NOT_FOUND,
    /** Row exists but is not {@code TRADE}. */
    NOT_TRADE,
    /** Pending queue rows removed (or none were present — idempotent). */
    APPLIED
}
