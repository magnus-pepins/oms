package com.balh.oms.ledger;

/**
 * Classifies Ledger inflight hold failures from HTTP error bodies / exception messages.
 */
public final class LedgerInflightReservationFailures {

    private LedgerInflightReservationFailures() {}

    /**
     * {@code true} when the Ledger shim returned an explicit insufficient-funds outcome
     * (e.g. {@code "code":"INSUFFICIENT_FUNDS"} or {@code INSUFFICIENT_FUNDS:} in the body).
     */
    public static boolean isInsufficientFunds(Throwable t) {
        if (t == null) {
            return false;
        }
        return isInsufficientFundsMessage(t.getMessage())
                || (t.getCause() != null && isInsufficientFundsMessage(t.getCause().getMessage()));
    }

    public static boolean isInsufficientFundsMessage(String message) {
        if (message == null || message.isBlank()) {
            return false;
        }
        return message.contains("INSUFFICIENT_FUNDS")
                || message.contains("\"code\":\"INSUFFICIENT_FUNDS\"");
    }
}
