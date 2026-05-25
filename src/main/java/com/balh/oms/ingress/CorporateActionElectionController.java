package com.balh.oms.ingress;

import com.balh.oms.corporateaction.CorporateActionBrokerElectionExportService;
import com.balh.oms.corporateaction.CorporateActionElectionRepository;
import com.balh.oms.corporateaction.CorporateActionElectionService;
import com.balh.oms.corporateaction.CorporateActionEventRepository;
import com.balh.oms.corporateaction.CorporateActionRecordDateSnapshotService;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Ops workflow for voluntary corporate-action elections (gap plan §5.9). */
@RestController
@RequestMapping("/internal/v1/corporate-action-events")
public class CorporateActionElectionController {

    public record SubmitElectionRequest(UUID accountId, String electionChoice) {}

    public record SubmitElectionResponse(long electionId) {}

    public record ApproveElectionResponse(long electionId, String status) {}

    private final CorporateActionEventRepository events;
    private final CorporateActionElectionService electionService;
    private final CorporateActionElectionRepository elections;
    private final CorporateActionBrokerElectionExportService brokerExport;
    private final CorporateActionRecordDateSnapshotService recordDateSnapshots;

    public CorporateActionElectionController(
            CorporateActionEventRepository events,
            CorporateActionElectionService electionService,
            CorporateActionElectionRepository elections,
            CorporateActionBrokerElectionExportService brokerExport,
            CorporateActionRecordDateSnapshotService recordDateSnapshots) {
        this.events = events;
        this.electionService = electionService;
        this.elections = elections;
        this.brokerExport = brokerExport;
        this.recordDateSnapshots = recordDateSnapshots;
    }

    public record CaptureSnapshotResponse(long eventId, int holdersCaptured, String recordDate) {}

    @PostMapping("/{eventId}/record-date-snapshot/capture")
    public ResponseEntity<CaptureSnapshotResponse> captureRecordDateSnapshot(@PathVariable long eventId) {
        return events.findById(eventId)
                .map(
                        row -> {
                            LocalDate recordDate =
                                    row.recordDate() != null ? row.recordDate() : LocalDate.now();
                            int captured =
                                    recordDateSnapshots.captureForEvent(
                                            eventId, row.instrumentSymbol(), recordDate);
                            return ResponseEntity.ok(
                                    new CaptureSnapshotResponse(
                                            eventId, captured, recordDate.toString()));
                        })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{eventId}/elections/export")
    public ResponseEntity<?> exportBrokerElections(@PathVariable long eventId) {
        return brokerExport
                .exportForEvent(eventId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not_found")));
    }

    @GetMapping("/{eventId}/elections")
    public ResponseEntity<?> list(@PathVariable long eventId) {
        if (events.findById(eventId).isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not_found"));
        }
        List<CorporateActionElectionRepository.ElectionRow> rows = elections.listByEvent(eventId);
        return ResponseEntity.ok(Map.of("items", rows));
    }

    @PostMapping("/{eventId}/elections")
    public ResponseEntity<?> submit(
            @PathVariable long eventId,
            @RequestBody SubmitElectionRequest body,
            @RequestHeader(name = "X-OMS-Actor", required = false) String actorHeader) {
        if (events.findById(eventId).isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not_found"));
        }
        if (body.accountId() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "account_id_required"));
        }
        String actor = actorHeader == null || actorHeader.isBlank() ? "ops" : actorHeader.trim();
        try {
            long id = electionService.submit(eventId, body.accountId(), body.electionChoice(), actor);
            return ResponseEntity.status(HttpStatus.CREATED).body(new SubmitElectionResponse(id));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/elections/{electionId}/approve")
    public ResponseEntity<?> approve(
            @PathVariable long electionId,
            @RequestHeader(name = "X-OMS-Actor", required = false) String actorHeader) {
        String approver = actorHeader == null || actorHeader.isBlank() ? null : actorHeader.trim();
        if (approver == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "approver_required"));
        }
        return switch (electionService.approve(electionId, approver)) {
            case APPROVED -> ResponseEntity.ok(new ApproveElectionResponse(electionId, "approved"));
            case NOT_FOUND -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not_found"));
            case ALREADY_APPROVED -> ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", "already_approved"));
            case SAME_ACTOR -> ResponseEntity.badRequest().body(Map.of("error", "approver_must_differ_from_requester"));
        };
    }
}
