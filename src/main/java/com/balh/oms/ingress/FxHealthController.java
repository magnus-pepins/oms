package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * FX module readiness (M3 slice): returns module flag until §11.5 backend ships.
 */
@RestController
@RequestMapping("/internal/v1/fx")
public class FxHealthController {

    private final OmsConfig omsConfig;

    public FxHealthController(OmsConfig omsConfig) {
        this.omsConfig = omsConfig;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        boolean on = omsConfig.getFx().isModuleEnabled();
        return ResponseEntity.ok(
                Map.of(
                        "status",
                        on ? "module_enabled_pending_impl" : "not_enabled",
                        "moduleEnabled",
                        on,
                        "doc",
                        "oms/docs/fx-architecture-slice8.md"));
    }
}
