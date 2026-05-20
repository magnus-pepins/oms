package com.balh.oms.ingress;

import com.balh.oms.domain.RejectCode;
import org.springframework.http.HttpStatus;

/**
 * Rejects an order when the BFF-provided {@code fxQuoteId} cannot be recalled
 * from {@link com.balh.oms.fx.FxQuoteService} — typically because the quote
 * expired between the customer-frontend tap and the OMS accept. Maps to HTTP
 * 422 in {@link OmsExceptionHandler} so the BFF can distinguish a stale-rate
 * reject from a validation 400 and surface a UI "Rate expired, refresh and
 * try again" affordance.
 *
 * <p>§8.4 quote-lock flow. The OMS recall guard only runs when
 * {@code oms.fx.accept-use-quoter.enabled=true} and the request carries a
 * non-null {@code fxQuoteId}.
 */
public final class FxQuoteLockException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final RejectCode rejectCode;
    private final String errorCode;

    public FxQuoteLockException(HttpStatus httpStatus, RejectCode rejectCode, String errorCode, String detail) {
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
