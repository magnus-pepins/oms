package com.balh.oms.ingress;

import com.balh.oms.domain.RejectCode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps validation and framework errors to the canonical reject-code envelope.
 */
@RestControllerAdvice
public class OmsExceptionHandler {

    @ExceptionHandler(LedgerBindingException.class)
    public ResponseEntity<ApiErrorResponse> handleLedgerBinding(LedgerBindingException ex) {
        var body = new ApiErrorResponse(
                RejectCode.INTERNAL_ERROR.name(),
                ex.getErrorCode(),
                List.of(new ApiErrorResponse.FieldViolation("ledgerBinding", ex.getMessage()))
        );
        return ResponseEntity.status(ex.getHttpStatus()).body(body);
    }

    /**
     * §8.4 quote-lock reject path. The {@code rejectCode} is the canonical
     * {@link RejectCode} value (e.g. {@code RISK_FX_QUOTE_EXPIRED}) so the
     * BFF + Ops Console can branch on it directly; {@code errorCode} stays
     * a short slug ("fx_quote_expired") for logging / metrics tagging.
     */
    @ExceptionHandler(FxQuoteLockException.class)
    public ResponseEntity<ApiErrorResponse> handleFxQuoteLock(FxQuoteLockException ex) {
        var body = new ApiErrorResponse(
                ex.getRejectCode().name(),
                ex.getErrorCode(),
                List.of(new ApiErrorResponse.FieldViolation("fxQuoteId", ex.getMessage()))
        );
        return ResponseEntity.status(ex.getHttpStatus()).body(body);
    }

    /** §8.6 — vendor mid stale/absent when stub mids are disabled in production. */
    @ExceptionHandler(com.balh.oms.fx.FxQuoteStaleException.class)
    public ResponseEntity<ApiErrorResponse> handleFxQuoteStale(com.balh.oms.fx.FxQuoteStaleException ex) {
        var body = new ApiErrorResponse(
                ex.getRejectCode().name(),
                ex.getErrorCode(),
                List.of(new ApiErrorResponse.FieldViolation("pair", ex.getMessage()))
        );
        return ResponseEntity.status(ex.getHttpStatus()).body(body);
    }

    @ExceptionHandler(ClusterAdmissionException.class)
    public ResponseEntity<ApiErrorResponse> handleClusterAdmission(ClusterAdmissionException ex) {
        var body = new ApiErrorResponse(
                RejectCode.INTERNAL_ERROR.name(),
                ex.getErrorCode(),
                List.of(new ApiErrorResponse.FieldViolation("clusterAdmission", ex.getMessage()))
        );
        return ResponseEntity.status(ex.getHttpStatus()).body(body);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        var violations = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> new ApiErrorResponse.FieldViolation(fe.getField(), fe.getDefaultMessage()))
                .collect(Collectors.toList());
        var body = new ApiErrorResponse(
                RejectCode.INTERNAL_ERROR.name(),
                "validation_failed",
                violations
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }
}
