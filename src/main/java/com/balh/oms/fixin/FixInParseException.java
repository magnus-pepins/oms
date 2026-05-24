package com.balh.oms.fixin;

/** FIX-in edge validation failure mapped to client-facing rejects. */
public final class FixInParseException extends RuntimeException {

    private final String rejectReason;

    public FixInParseException(String rejectReason) {
        super(rejectReason);
        this.rejectReason = rejectReason;
    }

    public String rejectReason() {
        return rejectReason;
    }
}
