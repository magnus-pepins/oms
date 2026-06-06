package com.balh.oms.ingress.readiness;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Gating note (2026-05-23): see the sibling
 * {@link com.balh.oms.ingress.reconcile.OmsClusterReconcileController} javadoc for why
 * {@code @ConditionalOnBean} on a component-scanned controller races with {@code @Configuration}
 * processing. This controller happened to win the race on pop's deploy, but the sibling did not —
 * same code, different scan order, undefined behaviour. Pre-emptively mirror the gating of
 * {@link OmsClusterReadinessConfiguration} so this can't bite us later when class scan order
 * shifts (new dependency, classpath reorder, Spring upgrade, etc.). Loud constructor-injection
 * failure if the reader bean is absent is preferred over a silent 404.
 */
@RestController
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public class OmsClusterReadinessController {

    private final OmsClusterReadinessReader reader;

    public OmsClusterReadinessController(OmsClusterReadinessReader reader) {
        this.reader = reader;
    }

    @GetMapping("/actuator/oms-cluster-readiness")
    public ResponseEntity<Map<String, Object>> readiness() {
        ReadinessSnapshot s = reader.snapshot();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ready", s.isReady());
        body.put("status", s.status().name());
        body.put("counterValue", s.counterValue());
        body.put("counterId", s.counterId());
        body.put("observedAtMillis", s.observedAtMillis());
        body.put("description", s.description());
        body.put("diskPressureLevel", s.diskPressureLevel().name());
        body.put("diskPressureCounterValue", s.diskPressureCounterValue());
        body.put("diskPressureCounterId", s.diskPressureCounterId());
        return s.isReady() ? ResponseEntity.ok(body) : ResponseEntity.status(503).body(body);
    }
}
