package com.balh.oms.ingress;

import com.balh.oms.domain.RejectCode;
import org.springframework.http.HttpStatus;

/**
 * Rejects an order when a Ledger buying-power inflight hold cannot be placed at accept time.
 * Mapped to HTTP 422 in {@link OmsExceptionHandler} with {@link RejectCode#RISK_BUYING_POWER}
 * so the BFF can surface an insufficient-funds outcome without admitting the order to the cluster
 * or forwarding it to the venue.
 */
public final class LedgerInflightHoldException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final RejectCode rejectCode;
    private final String errorCode;

    public LedgerInflightHoldException(
            HttpStatus httpStatus, RejectCode rejectCode, String errorCode, String detail) {
        super(detail);
        this.httpStatus = httpStatus;
        this.rejectCode = rejectCode;
        this.errorCode = errorCode;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public RejectCode getRejectCode() {
        return rejectCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
