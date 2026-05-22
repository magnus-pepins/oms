package com.balh.oms.ingress.readiness;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OmsClusterReadinessFilterDecisionTest {

    @Test
    void getOrderList_notGated() {
        assertThat(OmsClusterReadinessFilter.isGatedFor("GET", "/internal/v1/orders")).isFalse();
    }

    @Test
    void postOrderCreate_gated() {
        assertThat(OmsClusterReadinessFilter.isGatedFor("POST", "/internal/v1/orders")).isTrue();
    }

    @Test
    void postCancel_gated() {
        assertThat(OmsClusterReadinessFilter.isGatedFor(
                        "POST", "/internal/v1/orders/00000000-0000-4000-8000-000000000001/cancel"))
                .isTrue();
    }

    @Test
    void postForceMarkPostgresOnly_notGated() {
        assertThat(OmsClusterReadinessFilter.isGatedFor(
                        "POST",
                        "/internal/v1/admin/orders/00000000-0000-4000-8000-000000000001/force-mark-cancelled-postgres-only"))
                .isFalse();
    }

    @Test
    void actuatorReadiness_notGated() {
        assertThat(OmsClusterReadinessFilter.isGatedFor("GET", "/actuator/oms-cluster-readiness"))
                .isFalse();
    }

    @Test
    void actuatorReconcile_notGated() {
        assertThat(OmsClusterReadinessFilter.isGatedFor("GET", "/actuator/oms-cluster-reconcile"))
                .isFalse();
    }
}
