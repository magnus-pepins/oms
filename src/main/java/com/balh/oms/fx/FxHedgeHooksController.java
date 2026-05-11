package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * M3 Track 4: read-only hedge hook / kill-switch surface (stub values until LP routing exists).
 */
@RestController
@RequestMapping("/internal/v1/fx")
public class FxHedgeHooksController {

    private final OmsConfig omsConfig;
    private final Counter hedgeProbe;

    public FxHedgeHooksController(OmsConfig omsConfig, MeterRegistry registry) {
        this.omsConfig = omsConfig;
        this.hedgeProbe = Counter.builder("oms.fx.hedge_hook.probe")
                .description("FX hedge hook status probe invocations")
                .register(registry);
    }

    @GetMapping("/hedge/hooks-status")
    public ResponseEntity<Map<String, Object>> hooksStatus() {
        var fx = omsConfig.getFx();
        if (!fx.isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_module_disabled"));
        }
        if (!fx.isHedgeHooksEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "fx_hedge_hooks_disabled"));
        }
        hedgeProbe.increment();
        return ResponseEntity.ok(
                Map.of("paused", false, "perPairKillSwitch", Map.of(), "schemaVersion", 1, "financeGated", true));
    }
}
