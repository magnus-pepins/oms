package com.balh.oms.domain;

public enum OrderStatus {
    PENDING_NEW,
    NEW,
    WORKING,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED,
    REJECTED,
    EXPIRED;

    public boolean isTerminal() {
        return this == FILLED || this == CANCELLED || this == REJECTED || this == EXPIRED;
    }
}
