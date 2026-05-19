package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.OrdersRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Internal desk snapshot (bounded list). Disabled unless {@code oms.desk.snapshot-enabled=true}.
 *
 * <h2>Time windowing</h2>
 *
 * The endpoint takes an optional ISO-8601 instant pair {@code since} / {@code until}:
 * <ul>
 *   <li>{@code since} narrows the lower bound. The hard floor is still {@code now - snapshotMaxAgeHours}
 *       (cap 168h / 7d in {@link OmsConfig}), so an operator asking for "yesterday" on a 24h-windowed
 *       deploy will get nothing rather than an error — surfacing the wrong shape than what they expect.
 *       Increase {@code OMS_DESK_SNAPSHOT_MAX_AGE_HOURS} on the deploy that needs longer history.</li>
 *   <li>{@code until} is exclusive and only applied when present. Useful for "yesterday" =
 *       {@code since=midnightUtc-1d, until=midnightUtc}.</li>
 *   <li>If {@code since}/{@code until} are unparseable we 400 rather than silently returning the
 *       default window — the desk UI should always send well-formed instants.</li>
 * </ul>
 *
 * Status filtering is intentionally NOT pushed to SQL — the snapshot is already bounded by
 * {@code limit} (max 500) and the desk UI filters client-side over an O(N) array. Pushing
 * 8-way status-IN logic down would require either binding an array or generating SQL per call
 * for no measurable win at these row counts.
 */
@RestController
@RequestMapping("/internal/v1/desk/orders")
public class DeskSnapshotController {

    private final OmsConfig omsConfig;
    private final OrdersRepository ordersRepository;

    public DeskSnapshotController(OmsConfig omsConfig, OrdersRepository ordersRepository) {
        this.omsConfig = omsConfig;
        this.ordersRepository = ordersRepository;
    }

    public record DeskSnapshotResponse(
            List<OrdersRepository.DeskSnapshotRow> orders,
            Instant minReceived,
            Instant maxReceivedExclusive,
            int limit) {}

    @GetMapping("/snapshot")
    public ResponseEntity<?> snapshot(
            @RequestParam(name = "limit", required = false) Integer limitRaw,
            @RequestParam(name = "since", required = false) String sinceRaw,
            @RequestParam(name = "until", required = false) String untilRaw) {
        if (!omsConfig.getDesk().isSnapshotEnabled()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "desk_snapshot_disabled", "message", "Set OMS_DESK_SNAPSHOT_ENABLED=true for bounded desk reads"));
        }
        int max = omsConfig.getDesk().getSnapshotMaxLimit();
        int lim = limitRaw == null ? Math.min(50, max) : Math.min(Math.max(1, limitRaw), max);
        int hours = omsConfig.getDesk().getSnapshotMaxAgeHours();
        Instant floor = Instant.now().minus(hours, ChronoUnit.HOURS);

        Instant since;
        Instant until;
        try {
            since = parseInstantOrNull(sinceRaw);
            until = parseInstantOrNull(untilRaw);
        } catch (DateTimeException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_instant",
                    "message", "`since` and `until` must be ISO-8601 instants (e.g. 2026-05-19T00:00:00Z)"));
        }
        // Lower bound: never go further back than the deploy's hard floor.
        Instant effectiveSince = since == null || since.isBefore(floor) ? floor : since;
        // Upper bound: optional. If until is before effectiveSince we'd return nothing,
        // so reject as a 400 rather than silently emptying the list.
        if (until != null && !until.isAfter(effectiveSince)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_window",
                    "message", "`until` must be strictly after the effective `since`"));
        }
        var rows = ordersRepository.findDeskSnapshot(effectiveSince, until, lim);
        return ResponseEntity.ok(new DeskSnapshotResponse(rows, effectiveSince, until, lim));
    }

    private static Instant parseInstantOrNull(String raw) {
        if (raw == null) return null;
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) return null;
        return Instant.parse(trimmed);
    }
}
