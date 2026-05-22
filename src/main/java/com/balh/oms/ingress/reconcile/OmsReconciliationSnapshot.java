package com.balh.oms.ingress.reconcile;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** Combined cluster + projector reconcile view (Phase 3). */
public record OmsReconciliationSnapshot(
        Map<ReconcileEntityKind, EntityDrift> drift,
        long clusterObservedAtMillis,
        long projectorObservedAtMillis,
        ProjectorStatus projectorStatus,
        String projectorErrorOrNull) {

    public OmsReconciliationSnapshot {
        Objects.requireNonNull(drift, "drift");
        Objects.requireNonNull(projectorStatus, "projectorStatus");
        if (drift.size() != ReconcileEntityKind.values().length) {
            throw new IllegalArgumentException("drift must cover every ReconcileEntityKind");
        }
        drift = Map.copyOf(drift);
    }

    public static OmsReconciliationSnapshot compose(
            ClusterCountsSnapshot clusterSnapshot,
            Map<ReconcileEntityKind, Long> projectorCountsOrNull,
            long projectorObservedAtMillis,
            ProjectorStatus projectorStatus,
            String projectorErrorOrNull) {
        EnumMap<ReconcileEntityKind, EntityDrift> driftMap = new EnumMap<>(ReconcileEntityKind.class);
        for (ReconcileEntityKind kind : ReconcileEntityKind.values()) {
            ClusterCountsSnapshot.EntityCount cluster = clusterSnapshot.countFor(kind);
            Long projector = projectorCountsOrNull == null ? null : projectorCountsOrNull.get(kind);
            driftMap.put(kind, computeDrift(cluster, projector));
        }
        return new OmsReconciliationSnapshot(
                driftMap,
                clusterSnapshot.observedAtMillis(),
                projectorObservedAtMillis,
                projectorStatus,
                projectorErrorOrNull);
    }

    private static EntityDrift computeDrift(ClusterCountsSnapshot.EntityCount cluster, Long projectorOrNull) {
        if (!cluster.present() || projectorOrNull == null) {
            return new EntityDrift(
                    DriftStatus.UNKNOWN,
                    cluster.present() ? cluster.value() : -1L,
                    projectorOrNull == null ? -1L : projectorOrNull,
                    0L);
        }
        long delta = cluster.value() - projectorOrNull;
        DriftStatus status = delta == 0L ? DriftStatus.IN_SYNC : DriftStatus.DRIFT;
        return new EntityDrift(status, cluster.value(), projectorOrNull, delta);
    }

    public boolean isAllInSync() {
        for (EntityDrift d : drift.values()) {
            if (d.status() != DriftStatus.IN_SYNC) {
                return false;
            }
        }
        return true;
    }

    public EntityDrift driftFor(ReconcileEntityKind kind) {
        return drift.get(kind);
    }

    public record EntityDrift(DriftStatus status, long clusterCount, long projectorCount, long delta) {
        public EntityDrift {
            Objects.requireNonNull(status, "status");
        }
    }

    public enum DriftStatus {
        IN_SYNC,
        DRIFT,
        UNKNOWN
    }

    public enum ProjectorStatus {
        OK,
        ERROR,
        NOT_CONFIGURED
    }
}
