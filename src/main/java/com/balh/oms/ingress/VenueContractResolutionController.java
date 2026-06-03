package com.balh.oms.ingress;

import com.balh.oms.settlement.ManualVenueResolutionService;
import com.balh.oms.settlement.VenueContractResolutionRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Phase B operator surface for prediction-market resolution dispute handling. Secured by
 * {@link ApiKeyFilter}; Beard Admin proxies with human RBAC.
 */
@RestController
@RequestMapping("/internal/v1/venue/resolutions")
public class VenueContractResolutionController {

    private static final int DEFAULT_LIST_LIMIT = 50;
    private static final int MAX_LIST_LIMIT = 200;

    public record PostingPausedRequest(boolean postingPaused) {}

    public record PostingPausedResponse(long resolutionId, boolean postingPaused) {}

    public record ResolutionListItemResponse(
            long id,
            String contractSymbol,
            String outcome,
            String resolutionSource,
            Instant resolutionTimestamp,
            String evidenceHash,
            String venueId,
            Instant disputeUntil,
            boolean postingPaused,
            int ordersResolvedCount,
            Instant createdAt,
            int postedLegs,
            int pendingLegs,
            int skippedLegs) {}

    public record ResolutionListResponse(List<ResolutionListItemResponse> items) {}

    public record ManualResolutionBody(
            String yesSymbol, String outcome, String evidenceHash, String resolutionSource) {}

    public record ManualResolutionResponse(
            String yesSymbol, byte outcomeCode, String evidenceHash, String resolutionSource) {}

    private final VenueContractResolutionRepository resolutionRepository;
    private final Optional<ManualVenueResolutionService> manualResolutionService;

    public VenueContractResolutionController(
            VenueContractResolutionRepository resolutionRepository,
            Optional<ManualVenueResolutionService> manualResolutionService) {
        this.resolutionRepository = resolutionRepository;
        this.manualResolutionService = manualResolutionService;
    }

    @GetMapping
    public ResponseEntity<ResolutionListResponse> list(
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) String contractSymbol) {
        int lim = limit == null ? DEFAULT_LIST_LIMIT : Math.min(Math.max(limit, 1), MAX_LIST_LIMIT);
        List<ResolutionListItemResponse> items =
                resolutionRepository.listRecent(lim, contractSymbol).stream()
                        .map(VenueContractResolutionController::toListItem)
                        .toList();
        return ResponseEntity.ok(new ResolutionListResponse(items));
    }

    @PostMapping("/manual")
    public ResponseEntity<ManualResolutionResponse> applyManual(@RequestBody ManualResolutionBody body) {
        if (body == null || manualResolutionService.isEmpty()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
        try {
            var result =
                    manualResolutionService
                            .get()
                            .submit(
                                    new ManualVenueResolutionService.ManualResolutionRequest(
                                            body.yesSymbol(),
                                            body.outcome(),
                                            body.evidenceHash(),
                                            body.resolutionSource()));
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(
                            new ManualResolutionResponse(
                                    result.yesSymbol(),
                                    result.outcomeCode(),
                                    result.evidenceHash(),
                                    result.resolutionSource()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        } catch (java.util.concurrent.TimeoutException e) {
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).build();
        }
    }

    @PatchMapping("/{id}/posting-paused")
    public ResponseEntity<PostingPausedResponse> setPostingPaused(
            @PathVariable("id") long resolutionId, @RequestBody PostingPausedRequest body) {
        if (body == null) {
            return ResponseEntity.badRequest().build();
        }
        if (!resolutionRepository.existsById(resolutionId)) {
            return ResponseEntity.notFound().build();
        }
        if (!resolutionRepository.setPostingPaused(resolutionId, body.postingPaused())) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new PostingPausedResponse(resolutionId, body.postingPaused()));
    }

    private static ResolutionListItemResponse toListItem(
            VenueContractResolutionRepository.ResolutionListRow row) {
        return new ResolutionListItemResponse(
                row.id(),
                row.contractSymbol(),
                row.outcome(),
                row.resolutionSource(),
                row.resolutionTimestamp(),
                row.evidenceHash(),
                row.venueId(),
                row.disputeUntil(),
                row.postingPaused(),
                row.ordersResolvedCount(),
                row.createdAt(),
                row.postedLegs(),
                row.pendingLegs(),
                row.skippedLegs());
    }
}
