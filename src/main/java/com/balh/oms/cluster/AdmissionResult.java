package com.balh.oms.cluster;

/**
 * Outcome of submitting an {@link AcceptOrderCommand} to the OMS Aeron Cluster
 * via {@link OmsClusterIngressClient}.
 *
 * <p>Sealed so callers can {@code switch} exhaustively. The cluster's two
 * documented egress events for admission ({@link OrderAcceptedEvent} and
 * {@link OrderRejectedEvent}) map one-to-one to the two implementations; new
 * outcomes (e.g. timeout) are not modeled here — those propagate as exceptions
 * from {@code submitAcceptOrder}, not as result variants.
 */
public sealed interface AdmissionResult
        permits AdmissionResult.Accepted, AdmissionResult.Rejected {

    /** Correlation id echoed back from the cluster (matches the command). */
    long correlationId();

    /** Idempotent admission (fresh accept or duplicate re-hit). */
    record Accepted(OrderAcceptedEvent event) implements AdmissionResult {
        @Override
        public long correlationId() {
            return event.correlationId();
        }
    }

    /** Cluster rejected the command (e.g. malformed input). */
    record Rejected(OrderRejectedEvent event) implements AdmissionResult {
        @Override
        public long correlationId() {
            return event.correlationId();
        }
    }
}
