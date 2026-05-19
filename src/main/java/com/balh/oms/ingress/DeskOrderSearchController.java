package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.persistence.OrdersRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Operator-driven historical order search. Distinct from
 * {@link DeskSnapshotController} — the snapshot is the always-on live blotter feed (bounded recent
 * actives + terminals); search is for cross-account history without a date floor.
 *
 * <h2>Why a separate endpoint</h2>
 *
 * <p>The snapshot's response shape, date-window clamp, and active/terminal split are tuned for
 * "show me the live blotter," not "find me the FILLED AAPL order from three months ago." A search
 * surface needs:
 * <ul>
 *   <li>Cursor pagination (offset shifts under concurrent inserts and would corrupt page
 *       boundaries on a hot table)</li>
 *   <li>Multi-status / multi-account filters AND-combined with optional symbol / side / type /
 *       client-key dimensions</li>
 *   <li>No hard floor on {@code received_at} — operator searches are episodic and bounded by
 *       {@code limit + cursor}, not by age window</li>
 *   <li>A canonical ordering ({@code received_at DESC, id DESC}) for stable cursor walks</li>
 * </ul>
 *
 * <p>Bolting any of that onto the snapshot would have collapsed two distinct read patterns into
 * one over-parameterised SQL.
 *
 * <h2>Cursor encoding</h2>
 *
 * <p>The cursor is opaque base64 of {@code <epochNanos>:<uuid>}, derived from the
 * {@code (received_at, id)} of the last row of the previous page. Format chosen for grep-ability
 * (epochNanos is a number; UUID is hex), not for cryptographic opacity — clients should not
 * synthesise cursors by hand. Roundtrip is exact: nanos preserve {@code timestamptz} precision,
 * UUID preserves order-id identity.
 *
 * <h2>Auth</h2>
 *
 * <p>Same {@link ApiKeyFilter} guard as the snapshot endpoint — {@code X-OMS-Internal-Key} matches
 * {@code OMS_HTTP_INTERNAL_API_KEY}. No per-actor auth today; operator identity comes from the
 * BFF's audit log line. A future slice can wire {@code X-Admin-Actor} through.
 */
@RestController
@RequestMapping("/internal/v1/desk/orders")
public class DeskOrderSearchController {

    /**
     * Hard maximum on free-text filter fields ({@code symbol}, {@code clientRequestKey}) to bound
     * the bind-parameter size and prevent accidental wire abuse. Comfortably above the longest
     * legitimate values (symbol ~16 chars max; clientRequestKey is OMS-bounded at
     * {@link com.balh.oms.cluster.OmsClusterWireFormat#MAX_STRING_BYTES} = 128).
     */
    public static final int MAX_FILTER_FIELD_LEN = 256;

    /**
     * Hard maximum on the {@code status} multi-select before we 400. Eight is the count of
     * {@link OrderStatus} values today; we allow a small margin in case the enum grows during a
     * client / server upgrade lap.
     */
    public static final int MAX_STATUS_LIST_LEN = 16;

    private final OmsConfig omsConfig;
    private final OrdersRepository ordersRepository;

    public DeskOrderSearchController(OmsConfig omsConfig, OrdersRepository ordersRepository) {
        this.omsConfig = omsConfig;
        this.ordersRepository = ordersRepository;
    }

    public record DeskOrderSearchResponse(
            List<OrdersRepository.DeskSnapshotRow> orders,
            String nextCursor,
            int limit,
            Map<String, Object> filters) {}

    @GetMapping("/search")
    public ResponseEntity<?> search(
            @RequestParam(name = "accountId", required = false) String accountIdRaw,
            @RequestParam(name = "symbol", required = false) String symbolRaw,
            @RequestParam(name = "status", required = false) String statusRaw,
            @RequestParam(name = "side", required = false) String sideRaw,
            @RequestParam(name = "ordType", required = false) String ordTypeRaw,
            @RequestParam(name = "receivedFrom", required = false) String receivedFromRaw,
            @RequestParam(name = "receivedTo", required = false) String receivedToRaw,
            @RequestParam(name = "clientRequestKey", required = false) String clientRequestKeyRaw,
            @RequestParam(name = "cursor", required = false) String cursorRaw,
            @RequestParam(name = "limit", required = false) Integer limitRaw) {

        if (!omsConfig.getDesk().isSearchEnabled()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of(
                    "error", "desk_search_disabled",
                    "message", "Set OMS_DESK_SEARCH_ENABLED=true for desk order search"));
        }

        int cap = omsConfig.getDesk().getSearchMaxLimit();
        int lim = limitRaw == null ? Math.min(50, cap) : Math.min(Math.max(1, limitRaw), cap);

        UUID accountId;
        try {
            accountId = accountIdRaw == null || accountIdRaw.isBlank() ? null : UUID.fromString(accountIdRaw.trim());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_account_id",
                    "message", "`accountId` must be a UUID"));
        }

        String symbol = trimToNullWithCap(symbolRaw);
        String clientRequestKey = trimToNullWithCap(clientRequestKeyRaw);
        if (symbol != null && symbol.length() > MAX_FILTER_FIELD_LEN) {
            return ResponseEntity.badRequest().body(filterTooLong("symbol"));
        }
        if (clientRequestKey != null && clientRequestKey.length() > MAX_FILTER_FIELD_LEN) {
            return ResponseEntity.badRequest().body(filterTooLong("clientRequestKey"));
        }

        String[] statusList;
        try {
            statusList = parseStatusList(statusRaw);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_status",
                    "message", e.getMessage(),
                    "allowed", java.util.Arrays.stream(OrderStatus.values()).map(Enum::name).toList()));
        }
        if (statusList != null && statusList.length > MAX_STATUS_LIST_LEN) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "too_many_statuses",
                    "message", "At most " + MAX_STATUS_LIST_LEN + " status values per query"));
        }

        String side = normaliseSide(sideRaw);
        if (sideRaw != null && !sideRaw.isBlank() && side == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_side",
                    "message", "`side` must be BUY or SELL"));
        }

        String ordType = normaliseOrdType(ordTypeRaw);
        if (ordTypeRaw != null && !ordTypeRaw.isBlank() && ordType == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_ord_type",
                    "message", "`ordType` must be MARKET or LIMIT"));
        }

        Instant receivedFrom;
        Instant receivedTo;
        try {
            receivedFrom = parseInstantOrNull(receivedFromRaw);
            receivedTo = parseInstantOrNull(receivedToRaw);
        } catch (DateTimeException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_instant",
                    "message", "`receivedFrom` / `receivedTo` must be ISO-8601 instants"));
        }
        if (receivedFrom != null && receivedTo != null && !receivedTo.isAfter(receivedFrom)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_window",
                    "message", "`receivedTo` must be strictly after `receivedFrom`"));
        }

        Cursor cursor;
        try {
            cursor = decodeCursor(cursorRaw);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_cursor",
                    "message", "`cursor` is opaque to the client; pass back the `nextCursor` from the prior response"));
        }

        var params = new OrdersRepository.SearchParams(
                accountId,
                symbol,
                statusList,
                side,
                ordType,
                receivedFrom,
                receivedTo,
                clientRequestKey,
                cursor == null ? null : cursor.receivedAt(),
                cursor == null ? null : cursor.id(),
                // Fetch one extra row so we can detect "more results exist" without a COUNT(*).
                // We slice it off before returning; the truncated `id` of the LAST returned row
                // becomes the next cursor.
                lim + 1);

        List<OrdersRepository.DeskSnapshotRow> rows = ordersRepository.searchOrders(params);
        String nextCursor = null;
        if (rows.size() > lim) {
            rows = rows.subList(0, lim);
            var last = rows.get(rows.size() - 1);
            nextCursor = encodeCursor(last.receivedAt(), last.id());
        }

        Map<String, Object> echoed = new LinkedHashMap<>();
        if (accountId != null) echoed.put("accountId", accountId.toString());
        if (symbol != null) echoed.put("symbol", symbol);
        if (statusList != null) echoed.put("status", List.of(statusList));
        if (side != null) echoed.put("side", side);
        if (ordType != null) echoed.put("ordType", ordType);
        if (receivedFrom != null) echoed.put("receivedFrom", receivedFrom.toString());
        if (receivedTo != null) echoed.put("receivedTo", receivedTo.toString());
        if (clientRequestKey != null) echoed.put("clientRequestKey", clientRequestKey);

        return ResponseEntity.ok(new DeskOrderSearchResponse(rows, nextCursor, lim, echoed));
    }

    private static Map<String, String> filterTooLong(String field) {
        return Map.of(
                "error", "filter_too_long",
                "message", "`" + field + "` must be at most " + MAX_FILTER_FIELD_LEN + " characters");
    }

    private static String trimToNullWithCap(String raw) {
        if (raw == null) return null;
        String t = raw.trim();
        return t.isEmpty() ? null : t;
    }

    /**
     * Comma-separated {@link OrderStatus} names → uppercased array, or null if no filter. Each
     * value is validated against the enum so the SQL never sees an unknown literal.
     */
    private static String[] parseStatusList(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String[] parts = raw.split(",");
        String[] out = new String[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i].trim().toUpperCase();
            if (p.isEmpty()) {
                throw new IllegalArgumentException("Empty status value in `status`");
            }
            try {
                OrderStatus.valueOf(p);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown status `" + p + "`");
            }
            out[i] = p;
        }
        return out;
    }

    private static String normaliseSide(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String s = raw.trim().toUpperCase();
        return ("BUY".equals(s) || "SELL".equals(s)) ? s : null;
    }

    private static String normaliseOrdType(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String s = raw.trim().toUpperCase();
        return ("MARKET".equals(s) || "LIMIT".equals(s)) ? s : null;
    }

    private static Instant parseInstantOrNull(String raw) {
        if (raw == null) return null;
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) return null;
        return Instant.parse(trimmed);
    }

    private record Cursor(Instant receivedAt, UUID id) {}

    private static Cursor decodeCursor(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String decoded;
        try {
            decoded = new String(Base64.getUrlDecoder().decode(raw), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("not base64");
        }
        int colon = decoded.indexOf(':');
        if (colon <= 0 || colon == decoded.length() - 1) {
            throw new IllegalArgumentException("malformed");
        }
        long epochNanos;
        try {
            epochNanos = Long.parseLong(decoded.substring(0, colon));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a numeric epoch");
        }
        UUID id;
        try {
            id = UUID.fromString(decoded.substring(colon + 1));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("not a uuid");
        }
        long seconds = Math.floorDiv(epochNanos, 1_000_000_000L);
        int nanos = (int) Math.floorMod(epochNanos, 1_000_000_000L);
        return new Cursor(Instant.ofEpochSecond(seconds, nanos), id);
    }

    private static String encodeCursor(Instant receivedAt, UUID id) {
        long epochNanos = Math.multiplyExact(receivedAt.getEpochSecond(), 1_000_000_000L) + receivedAt.getNano();
        String raw = epochNanos + ":" + id.toString();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }
}
