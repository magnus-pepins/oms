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
