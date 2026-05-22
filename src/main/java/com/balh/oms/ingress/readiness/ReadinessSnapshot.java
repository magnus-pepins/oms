package com.balh.oms.ingress.readiness;

import com.balh.oms.cluster.OmsAdmissionClusteredService;

import java.util.Objects;

/** Ingress view of the cluster readiness Aeron counter (Phase 2.3). */
public record ReadinessSnapshot(
        Status status,
        long counterValue,
        int counterId,
        long observedAtMillis,
        String description) {

    public enum Status {
        UNKNOWN,
        READY,
        NOT_READY,
        COUNTER_MISSING
    }

    public ReadinessSnapshot {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(description, "description");
    }

    public static ReadinessSnapshot unknown(long observedAtMillis) {
        return new ReadinessSnapshot(
                Status.UNKNOWN, -1L, -1, observedAtMillis, "reader has not polled yet");
    }

    public static ReadinessSnapshot counterMissing(long observedAtMillis) {
        return new ReadinessSnapshot(
                Status.COUNTER_MISSING,
                -1L,
                -1,
                observedAtMillis,
                "no counter with matching typeId+label found on the media driver");
    }

    public static ReadinessSnapshot observed(
            long counterValue, int counterId, long observedAtMillis, long readyValue) {
        Status s = counterValue == readyValue ? Status.READY : Status.NOT_READY;
        return new ReadinessSnapshot(
                s,
                counterValue,
                counterId,
                observedAtMillis,
                s == Status.READY ? "cluster reports ready" : "cluster reports not-ready");
    }

    public boolean isReady() {
        return status == Status.READY;
    }
}
