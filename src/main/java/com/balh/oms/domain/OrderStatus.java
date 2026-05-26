package com.balh.oms.domain;

public enum OrderStatus {
    PENDING_NEW,
    NEW,
    WORKING,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED,
    REJECTED,
    EXPIRED,
    /** Phase B: binary contract resolved; no further trading on this symbol. */
    RESOLVED;

    public boolean isTerminal() {
        return this == FILLED
                || this == CANCELLED
                || this == REJECTED
                || this == EXPIRED
                || this == RESOLVED;
    }
}
