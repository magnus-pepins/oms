package com.balh.oms.ingress.reconcile;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
@ConditionalOnBean(OmsReconciliationService.class)
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
