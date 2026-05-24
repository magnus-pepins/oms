package com.balh.oms.ingress;

import com.balh.oms.corporateaction.ManualCorporateActionEventRepository;
import com.balh.oms.corporateaction.ManualCorporateActionService;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Manual CA event templates (merger / spin-off / bankruptcy) with four-eyes approval. */
@RestController
@RequestMapping("/internal/v1/corporate-actions/manual-events")
public class ManualCorporateActionController {

    public record CreateRequest(String templateType, JsonNode payloadJson, String createdBy) {}

    public record ApproveRequest(String approver) {}

    private final ManualCorporateActionService service;
    private final ManualCorporateActionEventRepository repository;

    public ManualCorporateActionController(
            ManualCorporateActionService service, ManualCorporateActionEventRepository repository) {
        this.service = service;
        this.repository = repository;
    }

    @GetMapping
    public ResponseEntity<List<ManualCorporateActionEventRepository.Row>> list(
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(repository.listRecent(Math.min(200, Math.max(1, limit))));
    }

    @PostMapping
    public ResponseEntity<ManualCorporateActionService.CreateResult> create(@RequestBody CreateRequest body) {
        if (body.createdBy() == null || body.createdBy().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        try {
            return ResponseEntity.ok(service.create(body.templateType(), body.payloadJson(), body.createdBy()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping("/{id}/approve")
    public ResponseEntity<ManualCorporateActionService.ApproveResult> approve(
            @PathVariable long id, @RequestBody ApproveRequest body) {
        try {
            return repository
                    .findById(id)
                    .flatMap(row -> service.approve(id, body.approver(), row.createdBy()))
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }
}
