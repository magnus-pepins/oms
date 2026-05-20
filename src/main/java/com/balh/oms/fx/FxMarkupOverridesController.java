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
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal write surface for the tactical FX markup override flow (P3.7).
 *
 * <p>Gated by {@link com.balh.oms.ingress.ApiKeyFilter} along with the
 * rest of {@code /internal/v1/**}. The trading-desk server proxies these
 * calls with an audit line; the {@code createdBy}/{@code approvedBy}/
 * {@code revokedBy} fields in the payload come from the operator's
 * Supabase identity on the trading-desk side (see
 * {@code trading-desk/server/routes/fxDesk.ts}).
 *
 * <p>Four-eyes (P3.9) is enforced in
 * {@link FxMarkupOverridesService#create} / {@link FxMarkupOverridesService#approve};
 * this controller is intentionally a thin shell so the business rules
 * live in one place.
 */
@RestController
@RequestMapping("/internal/v1/fx/markup-overrides")
public class FxMarkupOverridesController {

    private final OmsConfig omsConfig;
    private final FxMarkupOverridesService overrides;
    private final Clock clock;

    public FxMarkupOverridesController(
            OmsConfig omsConfig, FxMarkupOverridesService overrides, Clock clock) {
        this.omsConfig = omsConfig;
        this.overrides = overrides;
        this.clock = clock;
    }

    /**
     * Lists every override the trading-desk Treasury panel should display
     * (active, pending approval, scheduled, and recently revoked). Includes
     * a derived {@code status} string per row.
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> list() {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        List<Map<String, Object>> rows = overrides.listAll();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", clock.instant().toString());
        body.put("count", rows.size());
        body.put("rows", rows);
        // Echo the operator-facing thresholds so the UI can render the
        // "this needs four-eyes" hint without a separate config fetch.
        OmsConfig.Fx.MarkupOverrides cfg = omsConfig.getFx().getMarkupOverrides();
        Map<String, Object> limits = new LinkedHashMap<>();
        limits.put("maxAbsAdditiveBps", cfg.getMaxAbsAdditiveBps());
        limits.put("maxDurationMs", cfg.getMaxDurationMs());
        limits.put("autoApproveAbsBps", cfg.getAutoApproveAbsBps());
        limits.put("autoApproveMaxDurationMs", cfg.getAutoApproveMaxDurationMs());
        limits.put("autoApproveEnabled", cfg.isAutoApproveEnabled());
        body.put("limits", limits);
        return ResponseEntity.ok(body);
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody CreateBody body) {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        if (body == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing_body"));
        }
        FxMarkupOverridesService.CreateRequest req = new FxMarkupOverridesService.CreateRequest(
                body.pair(),
                body.side(),
                body.tier(),
                body.additiveBps(),
                body.validFrom(),
                body.durationMs() == null ? 0L : body.durationMs(),
                body.reason(),
                body.createdBy());
        try {
            FxMarkupOverridesService.CreateResult r = overrides.create(req);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", r.id());
            out.put("autoApproved", r.autoApproved());
            out.put("requiresApproval", !r.autoApproved());
            return ResponseEntity.status(HttpStatus.CREATED).body(out);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "invalid_override", "message", e.getMessage()));
        }
    }

    @PostMapping("/{id}/approve")
    public ResponseEntity<Map<String, Object>> approve(
            @PathVariable long id, @RequestBody ApprovalBody body) {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        if (body == null || body.approvedBy() == null || body.approvedBy().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing_approvedBy"));
        }
        try {
            overrides.approve(id, body.approvedBy());
            return ResponseEntity.ok(Map.of("id", id, "approved", true));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "invalid_approval", "message", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "approve_conflict", "message", e.getMessage()));
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> revoke(
            @PathVariable long id, @RequestParam("revokedBy") String revokedBy) {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        if (revokedBy == null || revokedBy.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing_revokedBy"));
        }
        try {
            overrides.revoke(id, revokedBy);
            return ResponseEntity.ok(Map.of("id", id, "revoked", true));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "revoke_conflict", "message", e.getMessage()));
        }
    }

    /**
     * Request payload for {@code POST /markup-overrides}. {@code pair} /
     * {@code side} / {@code tier} are optional (null = wildcard).
     * {@code validFrom} defaults to "now" when null.
     */
    public record CreateBody(
            String pair,
            String side,
            String tier,
            BigDecimal additiveBps,
            Instant validFrom,
            Long durationMs,
            String reason,
            String createdBy
    ) {}

    public record ApprovalBody(String approvedBy) {}
}
