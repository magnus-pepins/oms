package com.balh.oms.ingress.reconcile;

import com.balh.oms.cluster.OmsAdmissionClusteredService;

/** Phase 3 — reconciled entity kinds (orders-only for now). */
public enum ReconcileEntityKind {
    OPEN_ORDERS(
            OmsAdmissionClusteredService.OPEN_ORDERS_COUNT_COUNTER_TYPE_ID,
            OmsAdmissionClusteredService.OPEN_ORDERS_COUNT_COUNTER_LABEL);

    private final int counterTypeId;
    private final String counterLabel;

    ReconcileEntityKind(int counterTypeId, String counterLabel) {
        this.counterTypeId = counterTypeId;
        this.counterLabel = counterLabel;
    }

    public int counterTypeId() {
        return counterTypeId;
    }

    public String counterLabel() {
        return counterLabel;
    }

    public String tag() {
        return name().toLowerCase(java.util.Locale.ROOT);
    }
}
