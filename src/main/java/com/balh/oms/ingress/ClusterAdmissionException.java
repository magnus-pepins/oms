package com.balh.oms.ingress;

import org.springframework.http.HttpStatus;

/**
 * Thrown by {@link OrderIngressService} when the OMS Aeron Cluster either
 * rejects an admission command or fails to deliver a deterministic decision in
 * time (timeout, disconnect).
 *
 * <p>Phase 1b artifact of the Aeron Cluster substrate plan
 * ({@code system-documentation/plans/oms-aeron-cluster-substrate.md}). The
 * {@code OmsClusterIngressClient} round-trip is the gate for HTTP / gRPC
 * order admission when {@code oms.cluster.client.enabled=true}; this exception
 * is the single way that gate refuses or fails. Mapped to HTTP responses by
 * {@link OmsExceptionHandler}.
 */
public final class ClusterAdmissionException extends RuntimeException {

    private final HttpStatus httpStatus;
    private final String errorCode;

    public ClusterAdmissionException(HttpStatus httpStatus, String errorCode, String detail) {
        super(detail);
        this.httpStatus = httpStatus;
        this.errorCode = errorCode;
    }

    public ClusterAdmissionException(HttpStatus httpStatus, String errorCode, String detail, Throwable cause) {
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
