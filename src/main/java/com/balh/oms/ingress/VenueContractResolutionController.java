package com.balh.oms.ingress;

import com.balh.oms.settlement.VenueContractResolutionRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Phase B operator surface for prediction-market resolution dispute handling. Secured by
 * {@link ApiKeyFilter}; Beard Admin proxies with human RBAC.
 */
@RestController
@RequestMapping("/internal/v1/venue/resolutions")
public class VenueContractResolutionController {

    public record PostingPausedRequest(boolean postingPaused) {}

    public record PostingPausedResponse(long resolutionId, boolean postingPaused) {}

    private final VenueContractResolutionRepository resolutionRepository;

    public VenueContractResolutionController(VenueContractResolutionRepository resolutionRepository) {
        this.resolutionRepository = resolutionRepository;
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
}
