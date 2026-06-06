package com.balh.oms.ingress.readiness;

import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.cluster.disk.AeronDiskPressureLevel;

import java.util.Objects;

/** Ingress view of the cluster readiness Aeron counter (Phase 2.3). */
public record ReadinessSnapshot(
        Status status,
        long counterValue,
        int counterId,
        long observedAtMillis,
        String description,
        AeronDiskPressureLevel diskPressureLevel,
        long diskPressureCounterValue,
        int diskPressureCounterId) {

    public enum Status {
        UNKNOWN,
        READY,
        NOT_READY,
        COUNTER_MISSING,
        DISK_PRESSURE
    }

    public ReadinessSnapshot {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(diskPressureLevel, "diskPressureLevel");
    }

    public static ReadinessSnapshot unknown(long observedAtMillis) {
        return new ReadinessSnapshot(
                Status.UNKNOWN,
                -1L,
                -1,
                observedAtMillis,
                "reader has not polled yet",
                AeronDiskPressureLevel.OK,
                -1L,
                -1);
    }

    public static ReadinessSnapshot counterMissing(long observedAtMillis) {
        return new ReadinessSnapshot(
                Status.COUNTER_MISSING,
                -1L,
                -1,
                observedAtMillis,
                "no counter with matching typeId+label found on the media driver",
                AeronDiskPressureLevel.OK,
                -1L,
                -1);
    }

    public static ReadinessSnapshot observed(
            long counterValue, int counterId, long observedAtMillis, long readyValue) {
        return observedWithDiskPressure(
                counterValue, counterId, observedAtMillis, readyValue, AeronDiskPressureLevel.OK, 0L, -1);
    }

    public static ReadinessSnapshot observedWithDiskPressure(
            long counterValue,
            int counterId,
            long observedAtMillis,
            long readyValue,
            AeronDiskPressureLevel diskPressureLevel,
            long diskPressureCounterValue,
            int diskPressureCounterId) {
        if (diskPressureLevel.blocksWrites()) {
            return new ReadinessSnapshot(
                    Status.DISK_PRESSURE,
                    counterValue,
                    counterId,
                    observedAtMillis,
                    "cluster data directory disk pressure "
                            + diskPressureLevel.name()
                            + " — admission writes refused",
                    diskPressureLevel,
                    diskPressureCounterValue,
                    diskPressureCounterId);
        }
        Status s = counterValue == readyValue ? Status.READY : Status.NOT_READY;
        return new ReadinessSnapshot(
                s,
                counterValue,
                counterId,
                observedAtMillis,
                s == Status.READY ? "cluster reports ready" : "cluster reports not-ready",
                diskPressureLevel,
                diskPressureCounterValue,
                diskPressureCounterId);
    }

    public boolean isReady() {
        return status == Status.READY;
    }
}
