package com.balh.oms.ingress.reconcile;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** Cluster-side entity count observation for reconcile (Phase 3). */
public record ClusterCountsSnapshot(Map<ReconcileEntityKind, EntityCount> counts, long observedAtMillis) {

    public ClusterCountsSnapshot {
        Objects.requireNonNull(counts, "counts");
        if (counts.size() != ReconcileEntityKind.values().length) {
            throw new IllegalArgumentException("counts must cover every ReconcileEntityKind");
        }
        counts = Map.copyOf(counts);
    }

    public static ClusterCountsSnapshot allMissing(long observedAtMillis) {
        EnumMap<ReconcileEntityKind, EntityCount> m = new EnumMap<>(ReconcileEntityKind.class);
        for (ReconcileEntityKind k : ReconcileEntityKind.values()) {
            m.put(k, EntityCount.counterMissing());
        }
        return new ClusterCountsSnapshot(m, observedAtMillis);
    }

    public static ClusterCountsSnapshot openOrdersObserved(long openOrders, long observedAtMillis) {
        return new ClusterCountsSnapshot(
                Map.of(ReconcileEntityKind.OPEN_ORDERS, EntityCount.observed(openOrders)), observedAtMillis);
    }

    public EntityCount countFor(ReconcileEntityKind kind) {
        return counts.get(kind);
    }

    public record EntityCount(boolean present, long value) {
        public static EntityCount observed(long value) {
            return new EntityCount(true, value);
        }

        public static EntityCount counterMissing() {
            return new EntityCount(false, -1L);
        }
    }
}
