package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ManualSettlementActionsRepository;
import com.balh.oms.persistence.ManualSettlementActionRow;
import java.time.Duration;
import java.time.Instant;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Four-eyes manual settlement instructions (§12.8). Secured by {@link ApiKeyFilter} like other
 * {@code /internal/v1/**} routes; Beard Admin enforces human RBAC before proxying.
 */
@RestController
@RequestMapping("/internal/v1/settlement/manual-actions")
public class ManualSettlementActionsController {

    private static final int MAX_LIST_OFFSET = 10_000;
    private static final long MAX_LIST_RANGE_WITHOUT_EXECUTION_DAYS = 31;

    public record CreateManualSettlementActionRequest(
            long executionId, String actionType, String requestedBy, String payloadJson) {}

    public record ApproveManualSettlementActionRequest(String approvedBy) {}

    private final ManualSettlementActionsRepository repo;
    private final OmsConfig config;

    public ManualSettlementActionsController(ManualSettlementActionsRepository repo, OmsConfig config) {
        this.repo = repo;
        this.config = config;
    }

    @PostMapping
    public ResponseEntity<ManualSettlementActionResponse> create(@RequestBody CreateManualSettlementActionRequest body) {
        if (body == null) {
            return ResponseEntity.badRequest().build();
        }
        String requestedBy = body.requestedBy() == null ? "" : body.requestedBy().trim();
        if (requestedBy.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        String actionType = body.actionType() == null ? "" : body.actionType().trim();
        int typeMax = config.getSettlement().getManualActionTypeMaxLength();
        if (actionType.isEmpty() || actionType.length() > typeMax) {
            return ResponseEntity.badRequest().build();
        }
        String payload = body.payloadJson() == null ? "{}" : body.payloadJson().trim();
        if (payload.isEmpty()) {
            payload = "{}";
        }
        int payloadMax = config.getSettlement().getManualActionPayloadJsonMaxChars();
        if (payload.length() > payloadMax) {
            return ResponseEntity.badRequest().build();
        }
        if (!repo.executionExists(body.executionId())) {
            return ResponseEntity.notFound().build();
        }
        long id = repo.insert(body.executionId(), actionType, requestedBy, payload);
        return repo.findById(id)
                .map(ManualSettlementActionsController::toResponse)
                .map(ResponseEntity.status(HttpStatus.CREATED)::body)
                .orElse(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
    }

    @GetMapping
    public ResponseEntity<ManualSettlementActionsPageResponse> list(
            @RequestParam(required = false) Long executionId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        if (executionId == null && (from == null || to == null)) {
            return ResponseEntity.badRequest().build();
        }
        if (from != null ^ to != null) {
            return ResponseEntity.badRequest().build();
        }
        if (from != null && to != null) {
            if (!to.isAfter(from)) {
                return ResponseEntity.badRequest().build();
            }
            if (executionId == null) {
                long days = Duration.between(from, to).toDays();
                if (days > MAX_LIST_RANGE_WITHOUT_EXECUTION_DAYS) {
                    return ResponseEntity.badRequest().build();
                }
            }
        }
        int def = config.getSettlement().getManualActionListDefaultLimit();
        int max = config.getSettlement().getManualActionListMaxLimit();
        int lim = limit == null ? def : limit;
        if (lim < 1) {
            lim = def;
        }
        if (lim > max) {
            lim = max;
        }
        int off = offset == null ? 0 : offset;
        if (off < 0 || off > MAX_LIST_OFFSET) {
            return ResponseEntity.badRequest().build();
        }
        var items = repo.findByFilters(executionId, from, to, lim, off).stream()
                .map(ManualSettlementActionsController::toResponse)
                .toList();
        return ResponseEntity.ok(new ManualSettlementActionsPageResponse(items, lim, off));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ManualSettlementActionResponse> get(@PathVariable long id) {
        return repo.findById(id)
                .map(ManualSettlementActionsController::toResponse)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/approve")
    public ResponseEntity<ManualSettlementActionResponse> approve(
            @PathVariable long id, @RequestBody(required = false) ApproveManualSettlementActionRequest body) {
        String approvedBy = body == null || body.approvedBy() == null ? "" : body.approvedBy().trim();
        if (approvedBy.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        return switch (repo.approve(id, approvedBy)) {
            case OK -> repo.findById(id)
                    .map(ManualSettlementActionsController::toResponse)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
            case NOT_FOUND -> ResponseEntity.notFound().build();
            case ALREADY_APPROVED -> ResponseEntity.status(HttpStatus.CONFLICT).build();
            case SAME_ACTOR, INVALID_APPROVER -> ResponseEntity.badRequest().build();
            case CONFLICT -> ResponseEntity.status(HttpStatus.CONFLICT).build();
        };
    }

    private static ManualSettlementActionResponse toResponse(ManualSettlementActionRow r) {
        return new ManualSettlementActionResponse(
                r.id(),
                r.executionId(),
                r.actionType(),
                r.requestedBy(),
                r.approvedBy(),
                r.payloadJson(),
                r.createdAt());
    }
}
