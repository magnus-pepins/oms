package com.balh.oms.ingress;

import com.balh.oms.persistence.ControlRuntimeFlagsRepository;
import com.balh.oms.persistence.OmsRuntimeFlagKeys;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal ops: read/update Postgres-backed {@code oms_runtime_flags.global_halt} (slice U2+).
 */
@RestController
@RequestMapping("/internal/v1/runtime-flags")
public class RuntimeFlagsController {

    private static final Logger log = LoggerFactory.getLogger(RuntimeFlagsController.class);

    private final ControlRuntimeFlagsRepository runtimeFlags;

    public RuntimeFlagsController(ControlRuntimeFlagsRepository runtimeFlags) {
        this.runtimeFlags = runtimeFlags;
    }

    @GetMapping("/global_halt")
    public ResponseEntity<GlobalHaltResponse> getGlobalHalt() {
        return ResponseEntity.ok(
                runtimeFlags
                        .findGlobalHaltRow()
                        .map(r -> new GlobalHaltResponse(r.value(), r.updatedAt()))
                        .orElseGet(GlobalHaltResponse::absent));
    }

    @PatchMapping("/global_halt")
    public ResponseEntity<GlobalHaltResponse> patchGlobalHalt(@Valid @RequestBody GlobalHaltPatchRequest body) {
        String actor = body.updatedBy() == null || body.updatedBy().isBlank() ? "internal-api" : body.updatedBy();
        runtimeFlags.setGlobalHalt(Boolean.TRUE.equals(body.globalHalt()));
        log.warn("global_halt set to {} by {}", body.globalHalt(), actor);
        return ResponseEntity.ok(
                runtimeFlags
                        .findGlobalHaltRow()
                        .map(r -> new GlobalHaltResponse(r.value(), r.updatedAt()))
                        .orElseGet(GlobalHaltResponse::absent));
    }

    @GetMapping("/canary_pause_simulated_fills")
    public ResponseEntity<BooleanRuntimeFlagResponse> getCanaryPauseSimulatedFills() {
        return ResponseEntity.ok(
                runtimeFlags
                        .findBooleanFlagRow(OmsRuntimeFlagKeys.CANARY_PAUSE_SIMULATED_FILLS)
                        .map(r -> new BooleanRuntimeFlagResponse(r.value(), r.updatedAt()))
                        .orElseGet(BooleanRuntimeFlagResponse::absent));
    }

    @PatchMapping("/canary_pause_simulated_fills")
    public ResponseEntity<BooleanRuntimeFlagResponse> patchCanaryPauseSimulatedFills(
            @Valid @RequestBody BooleanRuntimeFlagPatchRequest body) {
        String actor = body.updatedBy() == null || body.updatedBy().isBlank() ? "internal-api" : body.updatedBy();
        runtimeFlags.upsertBooleanFlag(
                OmsRuntimeFlagKeys.CANARY_PAUSE_SIMULATED_FILLS, Boolean.TRUE.equals(body.value()));
        log.warn("canary_pause_simulated_fills set to {} by {}", body.value(), actor);
        return ResponseEntity.ok(
                runtimeFlags
                        .findBooleanFlagRow(OmsRuntimeFlagKeys.CANARY_PAUSE_SIMULATED_FILLS)
                        .map(r -> new BooleanRuntimeFlagResponse(r.value(), r.updatedAt()))
                        .orElseGet(BooleanRuntimeFlagResponse::absent));
    }
}
