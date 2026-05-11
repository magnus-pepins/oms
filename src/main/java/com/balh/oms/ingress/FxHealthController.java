package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FX module readiness (M3 slice): returns module flag and per-track rollout hints until §11.5 backend ships.
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
        var fx = omsConfig.getFx();
        boolean on = fx.isModuleEnabled();
        String status = on ? "module_enabled_pending_impl" : "not_enabled";
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status);
        body.put("moduleEnabled", on);
        body.put("tracks", buildTracks(fx, on));
        body.put("doc", "oms/docs/fx-architecture-slice8.md");
        return ResponseEntity.ok(body);
    }

    private static Map<String, String> buildTracks(OmsConfig.Fx fx, boolean module) {
        Map<String, String> t = new LinkedHashMap<>();
        t.put("quoteIngress", !module ? "off" : (fx.isQuoteStubEnabled() ? "stub" : "pending"));
        t.put("nostroRead", !module ? "off" : (fx.isNostroReadEnabled() ? "live" : "pending"));
        t.put("multiLegAtomicity", !module ? "off" : (fx.isMultiLegAtomicityStubEnabled() ? "stub" : "pending"));
        t.put("hedgeHooks", !module ? "off" : (fx.isHedgeHooksEnabled() ? "stub" : "pending"));
        t.put("eodFlatten", !module ? "off" : (fx.isEodFlattenEnabled() ? "stub" : "pending"));
        return t;
    }
}
