package com.balh.oms.ingress.reconcile;

import com.balh.oms.ingress.reconcile.OmsReconciliationSnapshot.ProjectorStatus;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Phase 3 — locks {@code GET /actuator/oms-cluster-reconcile} JSON shape and HTTP status. */
class OmsClusterReconcileControllerTest {

    private static final long T_CLUSTER = 1_700_000_000_000L;
    private static final long T_PROJECTOR = T_CLUSTER + 5_000L;

    @Test
    void inSync_returns200_withOpenOrdersBody() {
        OmsReconciliationService svc = mock(OmsReconciliationService.class);
        ClusterCountsSnapshot cluster = ClusterCountsSnapshot.openOrdersObserved(3, T_CLUSTER);
        OmsReconciliationSnapshot snap = OmsReconciliationSnapshot.compose(
                cluster,
                Map.of(ReconcileEntityKind.OPEN_ORDERS, 3L),
                T_PROJECTOR,
                ProjectorStatus.OK,
                null);
        when(svc.snapshot()).thenReturn(snap);
        when(svc.ageSeconds()).thenReturn(12L);

        ResponseEntity<Map<String, Object>> response = new OmsClusterReconcileController(svc).reconcile();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        Map<String, Object> body = response.getBody();
        assertThat(body)
                .containsEntry("inSync", true)
                .containsEntry("clusterObservedAtMillis", T_CLUSTER)
                .containsEntry("projectorObservedAtMillis", T_PROJECTOR)
                .containsEntry("projectorStatus", "OK")
                .containsEntry("ageSeconds", 12L)
                .doesNotContainKey("projectorError");

        @SuppressWarnings("unchecked")
        Map<String, Object> entities = (Map<String, Object>) body.get("entities");
        assertThat(entities).containsOnlyKeys("open_orders");
        @SuppressWarnings("unchecked")
        Map<String, Object> openOrders = (Map<String, Object>) entities.get("open_orders");
        assertThat(openOrders)
                .containsEntry("status", "IN_SYNC")
                .containsEntry("cluster", 3L)
                .containsEntry("projector", 3L)
                .containsEntry("delta", 0L);
    }

    @Test
    void drift_returns503_andDeltaCarriesSign() {
        OmsReconciliationService svc = mock(OmsReconciliationService.class);
        ClusterCountsSnapshot cluster = ClusterCountsSnapshot.openOrdersObserved(5, T_CLUSTER);
        OmsReconciliationSnapshot snap = OmsReconciliationSnapshot.compose(
                cluster,
                Map.of(ReconcileEntityKind.OPEN_ORDERS, 3L),
                T_PROJECTOR,
                ProjectorStatus.OK,
                null);
        when(svc.snapshot()).thenReturn(snap);
        when(svc.ageSeconds()).thenReturn(8L);

        ResponseEntity<Map<String, Object>> response = new OmsClusterReconcileController(svc).reconcile();

        assertThat(response.getStatusCode().value()).isEqualTo(503);
        assertThat(response.getBody()).containsEntry("inSync", false);

        @SuppressWarnings("unchecked")
        Map<String, Object> entities = (Map<String, Object>) response.getBody().get("entities");
        @SuppressWarnings("unchecked")
        Map<String, Object> openOrders = (Map<String, Object>) entities.get("open_orders");
        assertThat(openOrders)
                .containsEntry("status", "DRIFT")
                .containsEntry("delta", 2L);
    }

    @Test
    void unknownClusterSide_emitsNullClusterAndOmitsDelta() {
        OmsReconciliationService svc = mock(OmsReconciliationService.class);
        ClusterCountsSnapshot cluster = ClusterCountsSnapshot.allMissing(T_CLUSTER);
        OmsReconciliationSnapshot snap = OmsReconciliationSnapshot.compose(
                cluster,
                Map.of(ReconcileEntityKind.OPEN_ORDERS, 10L),
                T_PROJECTOR,
                ProjectorStatus.OK,
                null);
        when(svc.snapshot()).thenReturn(snap);
        when(svc.ageSeconds()).thenReturn(3L);

        ResponseEntity<Map<String, Object>> response = new OmsClusterReconcileController(svc).reconcile();

        assertThat(response.getStatusCode().value()).isEqualTo(503);
        @SuppressWarnings("unchecked")
        Map<String, Object> openOrders =
                (Map<String, Object>) ((Map<?, ?>) response.getBody().get("entities")).get("open_orders");
        assertThat(openOrders)
                .containsEntry("status", "UNKNOWN")
                .containsEntry("cluster", null)
                .containsEntry("projector", 10L)
                .containsEntry("delta", null);
    }

    @Test
    void projectorError_surfacesErrorMessage() {
        OmsReconciliationService svc = mock(OmsReconciliationService.class);
        ClusterCountsSnapshot cluster = ClusterCountsSnapshot.openOrdersObserved(2, T_CLUSTER);
        OmsReconciliationSnapshot snap = OmsReconciliationSnapshot.compose(
                cluster,
                null,
                T_PROJECTOR,
                ProjectorStatus.ERROR,
                "DataAccessResourceFailureException: connection refused");
        when(svc.snapshot()).thenReturn(snap);
        when(svc.ageSeconds()).thenReturn(1L);

        ResponseEntity<Map<String, Object>> response = new OmsClusterReconcileController(svc).reconcile();

        assertThat(response.getStatusCode().value()).isEqualTo(503);
        assertThat(response.getBody())
                .containsEntry("projectorStatus", "ERROR")
                .containsEntry("projectorError", "DataAccessResourceFailureException: connection refused");
    }
}
