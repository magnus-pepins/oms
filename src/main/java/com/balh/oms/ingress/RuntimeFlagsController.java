package com.balh.oms.ingress;

import com.balh.oms.persistence.ControlRuntimeFlagsRepository;
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
}
