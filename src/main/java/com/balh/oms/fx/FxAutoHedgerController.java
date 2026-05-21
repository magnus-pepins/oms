package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * REST surface for the auto-hedger (plan B1 + B2).
 *
 * <ul>
 *   <li>{@code GET    /internal/v1/fx/auto-hedger/policy} — full policy table snapshot + engine flags.</li>
 *   <li>{@code POST   /internal/v1/fx/auto-hedger/policy} — upsert one row (four-eyes on advisory→auto).</li>
 *   <li>{@code GET    /internal/v1/fx/auto-hedger/recommendations?active=true|false&limit=N} — live or recent.</li>
 *   <li>{@code POST   /internal/v1/fx/auto-hedger/recommendations/{id}/dismiss} — mark a recommendation as dismissed.</li>
 *   <li>{@code GET    /internal/v1/fx/auto-hedger/status} — last tick diagnostics for ops-console / Grafana.</li>
 *   <li>{@code POST   /internal/v1/fx/auto-hedger/refresh} — force a synchronous policy cache refresh.</li>
 * </ul>
 *
 * <p>The trading-desk server proxies these endpoints under
 * {@code /api/desk/fx/auto-hedger/*} so the Treasury page never talks
 * to OMS directly.
 */
@RestController
@RequestMapping("/internal/v1/fx/auto-hedger")
public class FxAutoHedgerController {

    private final OmsConfig omsConfig;
    private final FxAutoHedgerPolicyService policyService;
    private final FxAutoHedger engine;

    public FxAutoHedgerController(
            OmsConfig omsConfig,
            FxAutoHedgerPolicyService policyService,
            FxAutoHedger engine) {
        this.omsConfig = omsConfig;
        this.policyService = policyService;
        this.engine = engine;
    }

    @GetMapping("/policy")
    public ResponseEntity<Map<String, Object>> policy() {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        FxAutoHedgerPolicyService.PolicyView v = policyService.view();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", v.asOf().toString());
        body.put("cacheLoadedAtMs", v.cacheLoadedAtMs());
        body.put("pricingTier", v.pricingTier());
        body.put("engineEnabled", v.engineEnabled());
        body.put("autoFireEnabled", v.autoFireEnabled());
        List<Map<String, Object>> rows = v.rows().stream().map(this::policyRowToMap).toList();
        body.put("policies", rows);
        return ResponseEntity.ok(body);
    }

    @PostMapping("/policy")
    public ResponseEntity<Map<String, Object>> upsertPolicy(@RequestBody UpsertBody body) {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        if (body == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "body_required"));
        }
        try {
            FxAutoHedgerPolicyService.UpsertResult r = policyService.upsert(
                    new FxAutoHedgerPolicyService.UpsertRequest(
                            body.currency(),
                            body.targetBalance(),
                            body.thresholdAbs(),
                            body.thresholdPct(),
                            body.pairRoute(),
                            body.maxPerAction(),
                            body.cooldownS() == null ? 300 : body.cooldownS(),
                            body.mode(),
                            body.baseNostroId(),
                            body.quoteNostroId(),
                            body.updatedBy(),
                            body.autoApprovedBy()));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("currency", r.currency());
            out.put("mode", r.mode());
            out.put("promotedToAuto", r.promotedToAuto());
            out.put("demotedFromAuto", r.demotedFromAuto());
            return ResponseEntity.ok(out);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "invalid_policy", "message", e.getMessage()));
        }
    }

    @GetMapping("/recommendations")
    public ResponseEntity<Map<String, Object>> recommendations(
            @RequestParam(value = "active", defaultValue = "true") boolean active,
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        List<Map<String, Object>> rows = active
                ? engine.activeRecommendations(limit)
                : engine.recentRecommendations(limit);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("active", active);
        body.put("limit", limit);
        body.put("count", rows.size());
        body.put("rows", rows);
        return ResponseEntity.ok(body);
    }

    @PostMapping("/recommendations/{id}/dismiss")
    public ResponseEntity<Map<String, Object>> dismiss(
            @PathVariable("id") long id,
            @RequestBody DismissBody body) {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        if (body == null || body.dismissedBy() == null || body.dismissedBy().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "dismissedBy_required"));
        }
        try {
            Map<String, Object> row = engine.dismiss(id, body.dismissedBy());
            if (row == null) {
                return ResponseEntity.status(HttpStatus.CONFLICT)
                        .body(Map.of("error", "dismiss_conflict",
                                "message", "recommendation already terminal or not found"));
            }
            return ResponseEntity.ok(row);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "invalid_dismiss", "message", e.getMessage()));
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        FxAutoHedger.TickStatus t = engine.lastTickStatus();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", t.asOf().toString());
        body.put("engineRan", t.engineRan());
        body.put("reason", t.reason());
        List<Map<String, Object>> rs = t.results().stream().map(r -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("currency", r.currency());
            m.put("outcome", r.outcome());
            m.put("drift", r.drift() == null ? null : r.drift().toPlainString());
            m.put("actionKey", r.actionKey());
            m.put("detail", r.detail());
            return m;
        }).toList();
        body.put("results", rs);
        FxAutoHedgerPolicyService.PolicyView v = policyService.view();
        body.put("pricingTier", v.pricingTier());
        body.put("engineEnabled", v.engineEnabled());
        body.put("autoFireEnabled", v.autoFireEnabled());
        return ResponseEntity.ok(body);
    }

    @PostMapping("/refresh")
    public ResponseEntity<Map<String, Object>> refresh() {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        policyService.refreshNow();
        return ResponseEntity.ok(Map.of("refreshed", true));
    }

    private Map<String, Object> policyRowToMap(FxAutoHedgerPolicyService.PolicyRow r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("currency", r.currency());
        m.put("targetBalance", r.targetBalance().toPlainString());
        m.put("thresholdAbs", r.thresholdAbs() == null ? null : r.thresholdAbs().toPlainString());
        m.put("thresholdPct", r.thresholdPct() == null ? null : r.thresholdPct().toPlainString());
        m.put("pairRoute", r.pairRoute());
        m.put("maxPerAction", r.maxPerAction().toPlainString());
        m.put("cooldownS", r.cooldownS());
        m.put("mode", r.mode());
        m.put("baseNostroId", r.baseNostroId());
        m.put("quoteNostroId", r.quoteNostroId());
        m.put("createdBy", r.createdBy());
        m.put("createdAt", r.createdAt().toString());
        m.put("updatedBy", r.updatedBy());
        m.put("updatedAt", r.updatedAt() == null ? null : r.updatedAt().toString());
        m.put("autoApprovedBy", r.autoApprovedBy());
        m.put("autoApprovedAt", r.autoApprovedAt() == null ? null : r.autoApprovedAt().toString());
        return m;
    }

    public record UpsertBody(
            String currency,
            BigDecimal targetBalance,
            BigDecimal thresholdAbs,
            BigDecimal thresholdPct,
            String pairRoute,
            BigDecimal maxPerAction,
            Integer cooldownS,
            String mode,
            String baseNostroId,
            String quoteNostroId,
            String updatedBy,
            String autoApprovedBy
    ) {}

    public record DismissBody(String dismissedBy) {}
}
