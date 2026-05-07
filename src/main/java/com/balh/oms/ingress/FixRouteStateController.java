package com.balh.oms.ingress;

import com.balh.oms.persistence.FixRouteStateRepository;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal ops: read/update {@code fix_route_state.send_enabled} (slice 4+).
 */
@RestController
@RequestMapping("/internal/v1/fix/route-state")
public class FixRouteStateController {

    private final FixRouteStateRepository fixRouteStateRepository;

    public FixRouteStateController(FixRouteStateRepository fixRouteStateRepository) {
        this.fixRouteStateRepository = fixRouteStateRepository;
    }

    @GetMapping("/{routeKey}")
    public ResponseEntity<FixRouteStateResponse> get(@PathVariable String routeKey) {
        return fixRouteStateRepository
                .findByRouteKey(routeKey)
                .map(row -> ResponseEntity.ok(FixRouteStateResponse.from(row)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PatchMapping("/{routeKey}")
    public ResponseEntity<FixRouteStateResponse> patch(
            @PathVariable String routeKey, @Valid @RequestBody FixRouteStateUpdateRequest body) {
        String actor = body.updatedBy() == null || body.updatedBy().isBlank() ? "internal-api" : body.updatedBy();
        boolean updated =
                fixRouteStateRepository.updateSendEnabled(routeKey, body.sendEnabled(), actor, body.note());
        if (!updated) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        return fixRouteStateRepository
                .findByRouteKey(routeKey)
                .map(row -> ResponseEntity.ok(FixRouteStateResponse.from(row)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
    }
}
