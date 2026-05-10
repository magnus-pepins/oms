package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.fix.FixManualMassCancelService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Internal ops: manual FIX mass cancel (Slice U7+). Default policy is signal-only; wire is opt-in per
 * {@code oms.fix.manual-mass-cancel-wire-enabled} and requires logged-on session.
 */
@RestController
@RequestMapping("/internal/v1/fix/mass-cancel-request")
public class FixMassCancelRequestController {

    private final OmsConfig omsConfig;
    private final ObjectProvider<FixManualMassCancelService> manualMassCancelService;

    public FixMassCancelRequestController(
            OmsConfig omsConfig, ObjectProvider<FixManualMassCancelService> manualMassCancelService) {
        this.omsConfig = omsConfig;
        this.manualMassCancelService = manualMassCancelService;
    }

    public record MassCancelRequestBody(String requestedBy, String reason, Boolean wire) {}

    public record MassCancelResponseBody(String mode, String message, String massCancelClOrdId) {}

    @PostMapping
    public ResponseEntity<?> post(@RequestBody MassCancelRequestBody body) {
        if (!"fix".equalsIgnoreCase(omsConfig.getRouting().getBackend())) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "fix_routing_not_enabled", "message", "oms.routing.backend must be fix"));
        }
        FixManualMassCancelService svc = manualMassCancelService.getIfAvailable();
        if (svc == null) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "fix_manual_mass_cancel_unavailable"));
        }
        if (!omsConfig.getFix().isManualMassCancelEnabled()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "manual_mass_cancel_disabled", "message", "Set OMS_FIX_MANUAL_MASS_CANCEL_ENABLED=true after policy sign-off"));
        }
        if (body.requestedBy() == null || body.requestedBy().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "requested_by_required"));
        }
        boolean wire = Boolean.TRUE.equals(body.wire());
        try {
            FixManualMassCancelService.Outcome out = svc.execute(body.requestedBy(), body.reason(), wire);
            MassCancelResponseBody res = new MassCancelResponseBody(
                    out.mode(),
                    out.message(),
                    out.massCancelClOrdId().orElse(""));
            return ResponseEntity.accepted().body(res);
        } catch (IllegalArgumentException e) {
            if ("reason_too_long".equals(e.getMessage())) {
                return ResponseEntity.badRequest().body(Map.of("error", "reason_too_long"));
            }
            throw e;
        } catch (IllegalStateException e) {
            if ("manual_mass_cancel_disabled".equals(e.getMessage())) {
                return ResponseEntity.status(HttpStatus.FORBIDDEN)
                        .body(Map.of("error", "manual_mass_cancel_disabled"));
            }
            throw e;
        }
    }
}
