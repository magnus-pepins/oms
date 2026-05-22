package com.balh.oms.ingress.readiness;

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
@ConditionalOnBean(OmsClusterReadinessReader.class)
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
        return s.isReady() ? ResponseEntity.ok(body) : ResponseEntity.status(503).body(body);
    }
}
