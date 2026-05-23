package com.balh.oms.ingress.reconcile;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Gating note (2026-05-23): this controller previously used
 * {@code @ConditionalOnBean(OmsReconciliationService.class)} to slave its registration to
 * {@link OmsReconciliationConfiguration}. Per Spring Boot docs, {@code @ConditionalOnBean} on a
 * component-scanned class races with {@code @Configuration} processing — the bean isn't yet in
 * the registry when the scanner evaluates the condition, so the controller is silently excluded
 * even though the service is later created. On pop this manifested as a permanent 404 on
 * {@code /actuator/oms-cluster-reconcile} despite logs showing "OMS reconciliation service started".
 * The sibling {@link com.balh.oms.ingress.readiness.OmsClusterReadinessController} happened to win
 * the race only on certain scan orderings, hiding the same latent bug. We now mirror the exact
 * gating from {@link OmsReconciliationConfiguration} ({@code @Profile} + {@code @ConditionalOnProperty}),
 * both of which are evaluated against the {@code Environment} at the same lifecycle phase — no race.
 * If the controller is loaded while the service bean is absent (e.g. a misconfigured profile)
 * Spring will fail-fast at startup on the constructor injection, which is louder than the
 * previous silent 404.
 */
@RestController
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public class OmsClusterReconcileController {

    private final OmsReconciliationService service;

    public OmsClusterReconcileController(OmsReconciliationService service) {
        this.service = service;
    }

    @GetMapping("/actuator/oms-cluster-reconcile")
    public ResponseEntity<Map<String, Object>> reconcile() {
        OmsReconciliationSnapshot s = service.snapshot();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("inSync", s.isAllInSync());
        body.put("clusterObservedAtMillis", s.clusterObservedAtMillis());
        body.put("projectorObservedAtMillis", s.projectorObservedAtMillis());
        body.put("projectorStatus", s.projectorStatus().name());
        if (s.projectorErrorOrNull() != null) {
            body.put("projectorError", s.projectorErrorOrNull());
        }
        body.put("ageSeconds", service.ageSeconds());

        Map<String, Object> entities = new LinkedHashMap<>();
        for (ReconcileEntityKind kind : ReconcileEntityKind.values()) {
            OmsReconciliationSnapshot.EntityDrift d = s.driftFor(kind);
            Map<String, Object> entityBody = new LinkedHashMap<>();
            entityBody.put("status", d.status().name());
            entityBody.put("cluster", d.clusterCount() < 0 ? null : d.clusterCount());
            entityBody.put("projector", d.projectorCount() < 0 ? null : d.projectorCount());
            entityBody.put(
                    "delta",
                    d.status() == OmsReconciliationSnapshot.DriftStatus.UNKNOWN ? null : d.delta());
            entities.put(kind.tag(), entityBody);
        }
        body.put("entities", entities);

        return s.isAllInSync() ? ResponseEntity.ok(body) : ResponseEntity.status(503).body(body);
    }
}
