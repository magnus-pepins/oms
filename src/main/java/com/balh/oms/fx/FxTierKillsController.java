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

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal write surface for the per-tier kill-switches (plan A2 in
 * {@code plans/fx-treasury-auto-hedger-and-publisher-controls.md}).
 *
 * <p>Mirrors {@link FxMarkupOverridesController} — same Supabase-identity
 * injection from the trading-desk proxy, same four-eyes flow.
 * Wildcard-pair kills (pair=NULL) always need a second approver; scoped
 * pair kills under the auto-approve duration auto-approve. See
 * {@link FxTierKillsService} for the rules.
 */
@RestController
@RequestMapping("/internal/v1/fx/tier-kills")
public class FxTierKillsController {

    private final OmsConfig omsConfig;
    private final FxTierKillsService kills;
    private final Clock clock;

    public FxTierKillsController(
            OmsConfig omsConfig, FxTierKillsService kills, Clock clock) {
        this.omsConfig = omsConfig;
        this.kills = kills;
        this.clock = clock;
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> list() {
        if (!omsConfig.getFx().isModuleEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "fx_module_disabled"));
        }
        List<Map<String, Object>> rows = kills.listAll();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("asOf", clock.instant().toString());
        body.put("count", rows.size());
        body.put("rows", rows);
        OmsConfig.Fx.TierKills cfg = omsConfig.getFx().getTierKills();
        Map<String, Object> limits = new LinkedHashMap<>();
        limits.put("maxDurationMs", cfg.getMaxDurationMs());
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
        FxTierKillsService.CreateRequest req = new FxTierKillsService.CreateRequest(
                body.pair(),
                body.tier(),
                body.validFrom(),
                body.durationMs() == null ? 0L : body.durationMs(),
                body.reason(),
                body.createdBy());
        try {
            FxTierKillsService.CreateResult r = kills.create(req);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", r.id());
            out.put("autoApproved", r.autoApproved());
            out.put("requiresApproval", !r.autoApproved());
            return ResponseEntity.status(HttpStatus.CREATED).body(out);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "invalid_kill", "message", e.getMessage()));
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
            kills.approve(id, body.approvedBy());
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
            kills.revoke(id, revokedBy);
            return ResponseEntity.ok(Map.of("id", id, "revoked", true));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "revoke_conflict", "message", e.getMessage()));
        }
    }

    /**
     * Request payload for {@code POST /tier-kills}. {@code pair} is
     * optional (null = wildcard, "kill this tier across every pair");
     * {@code tier} is required. {@code validFrom} defaults to "now"
     * when null.
     */
    public record CreateBody(
            String pair,
            String tier,
            Instant validFrom,
            Long durationMs,
            String reason,
            String createdBy
    ) {}

    public record ApprovalBody(String approvedBy) {}
}
