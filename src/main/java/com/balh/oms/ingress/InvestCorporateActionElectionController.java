package com.balh.oms.ingress;

import com.balh.oms.corporateaction.CorporateActionElectionService;
import com.balh.oms.persistence.CustomerInvestReadRepository;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Customer invest elections for voluntary corporate actions (gap plan §5.9 Phase 2). */
@RestController
@RequestMapping("/internal/v1/invest/corporate-action-elections")
public class InvestCorporateActionElectionController {

    public record SubmitRequest(String electionChoice) {}

    public record SubmitResponse(long electionId, String status) {}

    public record PendingElectionItem(
            long eventId,
            String instrumentSymbol,
            String actionType,
            String effectiveDate,
            String quantitySettled,
            String electionChoice,
            String electionStatus) {}

    public record PendingElectionListResponse(List<PendingElectionItem> items) {}

    private final CustomerInvestReadRepository investRead;
    private final CorporateActionElectionService electionService;

    public InvestCorporateActionElectionController(
            CustomerInvestReadRepository investRead, CorporateActionElectionService electionService) {
        this.investRead = investRead;
        this.electionService = electionService;
    }

    @GetMapping
    public ResponseEntity<PendingElectionListResponse> listPending(@RequestParam("accountId") UUID accountId) {
        var rows = investRead.listPendingVoluntaryCorporateActions(accountId);
        var items =
                rows.stream()
                        .map(
                                r ->
                                        new PendingElectionItem(
                                                r.eventId(),
                                                r.instrumentSymbol(),
                                                r.actionType(),
                                                r.effectiveDate() == null ? null : r.effectiveDate().toString(),
                                                r.quantitySettled() == null
                                                        ? null
                                                        : r.quantitySettled().toPlainString(),
                                                r.electionChoice(),
                                                r.electionStatus()))
                        .toList();
        return ResponseEntity.ok(new PendingElectionListResponse(items));
    }

    @PostMapping("/{eventId}")
    public ResponseEntity<?> submit(
            @PathVariable long eventId,
            @RequestParam("accountId") UUID accountId,
            @RequestBody SubmitRequest body,
            @RequestHeader(name = "X-OMS-Customer-Id", required = false) String customerIdHeader) {
        String requestedBy =
                customerIdHeader == null || customerIdHeader.isBlank()
                        ? "customer:" + accountId
                        : "customer:" + customerIdHeader.trim();
        if (body.electionChoice() == null || body.electionChoice().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "election_choice_required"));
        }
        if (!investRead.accountHasPendingVoluntaryCorporateAction(accountId, eventId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not_found"));
        }
        try {
            long id = electionService.submit(eventId, accountId, body.electionChoice(), requestedBy);
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(new SubmitResponse(id, "pending_approval"));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
