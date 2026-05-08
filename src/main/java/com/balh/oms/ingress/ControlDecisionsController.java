package com.balh.oms.ingress;

import com.balh.oms.persistence.ControlDecisionsRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal ops: read-only audit of {@code control_decisions} (slice U3+).
 */
@RestController
@RequestMapping("/internal/v1/control-decisions")
public class ControlDecisionsController {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 500;
    private static final int MAX_OFFSET = 10_000;
    private static final long MAX_RANGE_WITHOUT_ORDER_DAYS = 31;

    private final ControlDecisionsRepository decisions;

    public ControlDecisionsController(ControlDecisionsRepository decisions) {
        this.decisions = decisions;
    }

    /**
     * Query control-plane decisions.
     *
     * <p>Either {@code orderId} is present, or both {@code from} and {@code to} must be present
     * (half-open range {@code [from, to)} on {@code decided_at}). When {@code orderId} is absent,
     * the range must not exceed 31 days and must have {@code to} strictly after {@code from}.
     */
    @GetMapping
    public ResponseEntity<ControlDecisionsPageResponse> list(
            @RequestParam(required = false) UUID orderId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        if (orderId == null && (from == null || to == null)) {
            return ResponseEntity.badRequest().build();
        }
        if (from != null && to != null) {
            if (!to.isAfter(from)) {
                return ResponseEntity.badRequest().build();
            }
            if (orderId == null) {
                long days = Duration.between(from, to).toDays();
                if (days > MAX_RANGE_WITHOUT_ORDER_DAYS) {
                    return ResponseEntity.badRequest().build();
                }
            }
        }
        int lim = limit == null ? DEFAULT_LIMIT : limit;
        if (lim < 1) {
            lim = DEFAULT_LIMIT;
        }
        if (lim > MAX_LIMIT) {
            lim = MAX_LIMIT;
        }
        int off = offset == null ? 0 : offset;
        if (off < 0 || off > MAX_OFFSET) {
            return ResponseEntity.badRequest().build();
        }
        var items =
                decisions.findByFilters(orderId, from, to, lim, off).stream()
                        .map(
                                r ->
                                        new ControlDecisionResponse(
                                                r.id(),
                                                r.orderId(),
                                                r.orderVersionBefore(),
                                                r.outcome(),
                                                r.rejectCode(),
                                                r.stage(),
                                                r.detail(),
                                                r.decidedAt()))
                        .toList();
        return ResponseEntity.ok(new ControlDecisionsPageResponse(items, lim, off));
    }
}
