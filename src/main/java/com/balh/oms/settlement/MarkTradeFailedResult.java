package com.balh.oms.settlement;

/** Outcome of {@link SettlementConfirmProcessor#markTradeFailed(long)}. */
public enum MarkTradeFailedResult {
    /** Execution id not found. */
    NOT_FOUND,
    /** Row exists but is not {@code TRADE}. */
    NOT_TRADE,
    /** Already {@code settled}; cannot fail. */
    ALREADY_SETTLED,
    /** Idempotent no-op. */
    ALREADY_FAILED,
    /** Updated to {@code failed} and pending broker confirms removed. */
    APPLIED
}
