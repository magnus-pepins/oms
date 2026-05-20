package com.balh.oms.fx;

import com.balh.oms.domain.RejectCode;
import org.springframework.http.HttpStatus;

/**
 * Thrown when {@link FxQuoteService} cannot price a pair because the live
 * vendor mid is absent or stale and {@code oms.fx.stub-mids-allowed=false}.
 * Maps to HTTP 422 {@code RISK_FX_STALE_QUOTE} via {@link com.balh.oms.ingress.OmsExceptionHandler}.
 */
public final class FxQuoteStaleException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final RejectCode rejectCode;
    private final String errorCode;

    public FxQuoteStaleException(HttpStatus httpStatus, RejectCode rejectCode, String errorCode, String detail) {
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
