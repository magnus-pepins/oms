package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calls Ledger {@code POST /transactions/bulk} with {@code inflight=true} and {@code atomic=false}
 * (Phase 4 slice 4q). Each item carries the canonical {@code oms:order:{uuid}} reference so
 * Ledger-side failure messages can be attributed back to the originating OMS order.
 *
 * <p>Response parsing covers both {@code 201 applied / partial} and the {@code 400 all-failed}
 * shape returned by {@code transaction.routes.ts}. Errors arrive as strings of the form
 * {@code "Transaction <reference>: <message>"}; we match {@code oms:order:{UUID}} occurrences
 * in those strings to drive the per-item failure set. Any other transport / parsing failure
 * surfaces as {@link LedgerInflightBulkException} so the coalescer routes the whole batch to
 * the outbox fallback path.
 */
public final class RestLedgerInflightBulkDispatcher implements LedgerInflightBulkDispatcher {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String REFERENCE_PREFIX = "oms:order:";
    private static final Pattern ORDER_ID_PATTERN =
            Pattern.compile("oms:order:([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})");

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;
    private final String destinationBalanceId;
    private final String currency;
    private final int precision;

    public RestLedgerInflightBulkDispatcher(
            RestClient http,
            String apiKey,
            ObjectMapper objectMapper,
            String destinationBalanceId,
            String currency,
            int precision) {
        this.http = http;
        this.apiKey = apiKey;
        this.objectMapper = objectMapper;
        this.destinationBalanceId = destinationBalanceId;
        this.currency = currency;
        this.precision = precision;
    }

    @Override
    public Result dispatch(List<HoldItem> items) throws LedgerInflightBulkException {
        if (items.isEmpty()) {
            return new Result(0, 0, Set.of());
        }
        ObjectNode body = objectMapper.createObjectNode();
        body.put("inflight", true);
        body.put("atomic", false);
        ArrayNode txns = body.putArray("transactions");
        for (HoldItem item : items) {
            BigDecimal holdAmount = item.holdAmount();
            if (holdAmount.compareTo(BigDecimal.ZERO) <= 0) {
                throw new LedgerInflightBulkException(
                        "inflight hold amount must be positive (orderId=" + item.orderId() + ")");
            }
            ObjectNode t = txns.addObject();
            t.put("source", item.sourceBalanceId());
            t.put("destination", destinationBalanceId);
            t.put("amount", holdAmount.doubleValue());
            t.put("currency", currency);
            t.put("reference", REFERENCE_PREFIX + item.orderId());
            t.put("description", "OMS buy intent hold (bulk)");
            t.put("precision", precision);
        }

        ResponseEntity<String> response;
        try {
            response = http.post()
                    .uri("/transactions/bulk")
                    .header(AUTHORIZATION_HEADER, BEARER_PREFIX + apiKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .onStatus(status -> status.value() == 400, (req, res) -> {
                        // 400 = all failed; the body still carries an errors[] we want to parse.
                        // Don't let RestClient throw here.
                    })
                    .toEntity(String.class);
        } catch (RestClientException e) {
            throw new LedgerInflightBulkException("ledger bulk HTTP error: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new LedgerInflightBulkException("ledger bulk unexpected error: " + e.getMessage(), e);
        }
        int status = response.getStatusCode().value();
        if (status != 200 && status != 201 && status != 400) {
            throw new LedgerInflightBulkException(
                    "ledger bulk POST failed: status=" + status + " body=" + response.getBody());
        }
        return parseResponse(items, response.getBody(), status);
    }

    /**
     * Visible for testing — given a response body and the items submitted, returns the
     * per-item success / failure breakdown. Robust against missing or partial fields: anything
     * we can't parse falls back to "all failed" so the coalescer takes the safe outbox path.
     */
    Result parseResponse(List<HoldItem> items, String body, int status) throws LedgerInflightBulkException {
        Set<UUID> requestedIds = new HashSet<>(items.size());
        for (HoldItem item : items) {
            requestedIds.add(item.orderId());
        }
        Set<UUID> failed = new HashSet<>();
        if (body == null || body.isBlank()) {
            if (status == 400) {
                throw new LedgerInflightBulkException("ledger bulk 400 with empty body");
            }
            return new Result(items.size(), items.size(), Set.of());
        }
        // declared here so it is in scope after the errors[] parsing block below
        Map<UUID, String> ledgerTxnIdByOrderId = new HashMap<>();
        JsonNode root;
        try {
            root = objectMapper.readTree(body);
        } catch (Exception e) {
            throw new LedgerInflightBulkException("ledger bulk response not parseable: " + e.getMessage(), e);
        }
        JsonNode errors = root.path("errors");
        if (errors.isArray()) {
            for (JsonNode err : errors) {
                String text = err.asText("");
                Matcher m = ORDER_ID_PATTERN.matcher(text);
                while (m.find()) {
                    try {
                        UUID id = UUID.fromString(m.group(1));
                        if (requestedIds.contains(id)) {
                            failed.add(id);
                        }
                    } catch (IllegalArgumentException ignore) {
                        // Pattern guarantees UUID shape; defensive.
                    }
                }
            }
        }
        if (status == 400 && failed.isEmpty()) {
            // 400 with no parsable per-item errors: treat as whole-batch failure so the
            // coalescer routes everything to the outbox fallback.
            throw new LedgerInflightBulkException(
                    "ledger bulk 400 without parsable errors; body=" + truncate(body));
        }
        // results[] carries {reference, transactionId} per created transaction. Map each back to
        // its OMS order id via the oms:order:{uuid} reference so the caller can persist the
        // Ledger txn id and later commit/void the hold. Absent on older Ledger builds (left empty).
        JsonNode results = root.path("results");
        if (results.isArray()) {
            for (JsonNode item : results) {
                String reference = item.path("reference").asText("");
                String txnId = item.path("transactionId").asText("");
                if (txnId.isEmpty()) {
                    continue;
                }
                Matcher m = ORDER_ID_PATTERN.matcher(reference);
                if (m.find()) {
                    try {
                        UUID id = UUID.fromString(m.group(1));
                        if (requestedIds.contains(id) && !failed.contains(id)) {
                            ledgerTxnIdByOrderId.put(id, txnId);
                        }
                    } catch (IllegalArgumentException ignore) {
                        // Pattern guarantees UUID shape; defensive.
                    }
                }
            }
        }
        int succeeded = items.size() - failed.size();
        return new Result(items.size(), succeeded, failed, ledgerTxnIdByOrderId);
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 512 ? s : s.substring(0, 512) + "…";
    }
}
