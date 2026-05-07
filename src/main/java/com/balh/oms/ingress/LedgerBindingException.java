package com.balh.oms.ingress;

import org.springframework.http.HttpStatus;

/**
 * Rejects an order when {@code ledgerBalanceId} / {@code ledgerIdentityId} cannot be
 * verified against Ledger (defense in depth on the OMS ingress).
 */
public final class LedgerBindingException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final String errorCode;

    public LedgerBindingException(HttpStatus httpStatus, String errorCode, String detail) {
        super(detail);
        this.httpStatus = httpStatus;
        this.errorCode = errorCode;
    }

    public LedgerBindingException(HttpStatus httpStatus, String errorCode, String detail, Throwable cause) {
        super(detail, cause);
        this.httpStatus = httpStatus;
        this.errorCode = errorCode;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
