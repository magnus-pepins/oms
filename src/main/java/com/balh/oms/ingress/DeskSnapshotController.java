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
 * <h2>GTC correctness</h2>
 *
 * <p>The snapshot is split into two SQL queries that the controller executes back-to-back:
 *
 * <ul>
 *   <li><strong>Active orders</strong> (PENDING_NEW / NEW / WORKING / PARTIALLY_FILLED) — returned
 *       <em>regardless of {@code received_at}</em>. A Good-Til-Cancel LIMIT placed weeks ago is
 *       still WORKING today; date-windowing it would render the live blotter wrong. Bounded only
 *       by {@code snapshotActiveLimit} (default 500).</li>
 *   <li><strong>Terminal orders</strong> (FILLED / CANCELLED / REJECTED / EXPIRED) — date-windowed
 *       using {@code since} / {@code until} (see below). Bounded by {@code limit} (default 50, cap
 *       {@code snapshotMaxLimit}).</li>
 * </ul>
 *
 * <p>The {@code orders} field in the response is the two lists concatenated (actives first), so
 * existing clients that just iterate {@code orders} keep working. {@code activeCount} +
 * {@code terminalCount} let the UI render an accurate breakdown ("237 active · 50 terminal in
 * window"). The previous shape — a single date-windowed list — silently dropped GTC actives older
 * than the floor, which was the bug this slice fixes.
 *
 * <h2>Time windowing (applies to terminals only)</h2>
 *
 * <ul>
 *   <li>{@code since} narrows the lower bound. Hard floor is {@code now - snapshotMaxAgeHours}
 *       (cap 168h / 7d in {@link OmsConfig}); "All time" history goes through the dedicated
 *       {@link DeskOrderSearchController}.</li>
 *   <li>{@code until} is exclusive; only applied when present. Useful for "yesterday" =
 *       {@code since=midnightUtc-1d, until=midnightUtc}.</li>
 *   <li>If {@code since} / {@code until} are unparseable we 400 rather than silently returning
 *       the default window.</li>
 * </ul>
 *
 * Status filtering across the active/terminal split is intentionally NOT pushed any further into
 * SQL — the desk UI's multi-select status filter still works client-side over the combined list.
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

    /**
     * Response shape. {@code orders} = actives ++ terminals (actives first) so legacy iterators
     * keep working; {@code activeCount} / {@code terminalCount} expose the split.
     * {@code activeLimit} / {@code terminalLimit} echo the per-bucket caps so the UI can render
     * truthful "showing N of cap" indicators when a bucket is at the ceiling.
     */
    public record DeskSnapshotResponse(
            List<OrdersRepository.DeskSnapshotRow> orders,
            int activeCount,
            int terminalCount,
            int activeLimit,
            int terminalLimit,
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
        int terminalCap = omsConfig.getDesk().getSnapshotMaxLimit();
        int activeCap = omsConfig.getDesk().getSnapshotActiveLimit();
        int terminalLim = limitRaw == null ? Math.min(50, terminalCap) : Math.min(Math.max(1, limitRaw), terminalCap);
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

        var actives = ordersRepository.findActiveDeskSnapshot(activeCap);
        var terminals = ordersRepository.findTerminalDeskSnapshot(effectiveSince, until, terminalLim);
        // Concatenate actives + terminals into the legacy `orders` field. We do NOT re-sort the
        // combined list: actives are always-present rows with no upper time bound, terminals are
        // newest-in-window first; mixing them by received_at would visually demote a fresh active
        // below an old terminal which is the opposite of what the operator wants.
        var combined = new java.util.ArrayList<OrdersRepository.DeskSnapshotRow>(
                actives.size() + terminals.size());
        combined.addAll(actives);
        combined.addAll(terminals);
        return ResponseEntity.ok(new DeskSnapshotResponse(
                combined,
                actives.size(),
                terminals.size(),
                activeCap,
                terminalLim,
                effectiveSince,
                until,
                terminalLim));
    }

    private static Instant parseInstantOrNull(String raw) {
        if (raw == null) return null;
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) return null;
        return Instant.parse(trimmed);
    }
}
