package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code POST /transactions} with {@code sync: true}, {@code inflight: true}.
 *
 * <p>Currency is resolved <strong>per source balance</strong> by hitting Ledger
 * {@code GET /balances/{id}} once per unseen balance (cached for the process
 * lifetime — Ledger balance currencies are immutable once created, see
 * plans/oms-multi-currency-invest-accounts.md §8.4 "OMS accept"). On lookup
 * failure we fall back to the constructor-supplied {@code currency} (the
 * legacy single-currency default, {@code OMS_TRADE_CASH_CURRENCY=USD} on the
 * demo stack) and emit a warn log — the hold still goes through, just at the
 * default currency. A mismatch there will surface as a Ledger 4xx on the
 * POST below.
 *
 * <p>This is the single-file pivot that unlocks multi-currency inflight holds
 * for ALL paths (sync ingress, async outbox reconciler, coalescer) without
 * changing the {@link LedgerInflightReservationClient} interface or the
 * outbox schema. Callers continue to pass {@code sourceBalanceId} and
 * {@code holdAmount}; this client picks the currency.
 */
public final class RestLedgerInflightReservationClient implements LedgerInflightReservationClient {

    private static final Logger log = LoggerFactory.getLogger(RestLedgerInflightReservationClient.class);

    private static final String LEDGER_KEY_HEADER = "X-Ledger-Key";
    private static final String REFERENCE_PREFIX = "oms:order:";
    /**
     * Wed-demo: JSON field name on the Ledger {@code POST /transactions} response carrying the
     * Ledger-assigned {@code txn_<uuid>} addressing key. Pinned to a constant so a Ledger response
     * shape change is a single-line diff here rather than a magic-string search.
     */
    private static final String TRANSACTION_ID_FIELD = "transactionId";
    /** Field name on the Ledger {@code GET /balances/{id}} response carrying the balance currency. */
    private static final String BALANCE_CURRENCY_FIELD = "currency";

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;
    private final String destinationBalanceId;
    private final String defaultCurrency;
    private final int precision;
    /**
     * Per-balance currency cache. Ledger balance currencies are immutable once
     * the balance is created, so a process-lifetime cache is safe; a JVM
     * restart re-hits Ledger which is cheap (the verify path uses
     * {@code with_queued=false} = Redis cache hit, ~1 ms).
     */
    private final ConcurrentHashMap<String, String> balanceCurrencyCache = new ConcurrentHashMap<>();

    public RestLedgerInflightReservationClient(
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
        this.defaultCurrency = currency;
        this.precision = precision;
    }

    @Override
    public String placeBuyFundsHold(UUID orderId, String sourceBalanceId, BigDecimal holdAmount)
            throws LedgerReservationException {
        if (holdAmount == null || holdAmount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new LedgerReservationException("inflight hold amount must be positive");
        }
        BigDecimal notional = holdAmount;
        String currency = resolveBalanceCurrency(sourceBalanceId);
        ObjectNode body = objectMapper.createObjectNode();
        body.put("source", sourceBalanceId);
        body.put("destination", destinationBalanceId);
        body.put("amount", notional.doubleValue());
        body.put("currency", currency);
        body.put("reference", REFERENCE_PREFIX + orderId);
        body.put("description", "OMS buy intent hold");
        body.put("sync", true);
        body.put("inflight", true);
        body.put("precision", precision);
        try {
            ResponseEntity<String> response = http.post()
                    .uri("/transactions")
                    .header(LEDGER_KEY_HEADER, apiKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toEntity(String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new LedgerReservationException(
                        "ledger inflight POST failed: status=%s body=%s"
                                .formatted(response.getStatusCode(), response.getBody()));
            }
            return extractTransactionId(response.getBody(), orderId);
        } catch (LedgerReservationException e) {
            throw e;
        } catch (RestClientException e) {
            throw new LedgerReservationException("ledger inflight HTTP error: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new LedgerReservationException("ledger inflight unexpected error: " + e.getMessage(), e);
        }
    }

    /**
     * Best-effort parse of the Ledger response body to extract the {@code transactionId}.
     * Returns {@code null} on any structural problem (missing field, non-JSON body, IO error).
     *
     * <p>Why "best-effort": the hold POST has already committed at the Ledger by the time we
     * reach this method (2xx response). Throwing because the response body wasn't shaped as
     * expected would force the OMS to treat a successful hold as a failure — the worse
     * outcome. Returning null instead lets the caller persist an outbox row without a
     * {@code ledger_txn_id}, the lifecycle reconciler skips it (cannot settle without the id),
     * and the Ledger's expiry sweep eventually releases the hold. The {@code log.warn} is the
     * operator signal that something is off (Ledger response schema drift, gateway munging,
     * etc.).
     */
    /**
     * Resolves the currency of the {@code sourceBalanceId} via Ledger
     * {@code GET /balances/{id}?with_queued=false} (cache-hit path, ~1 ms on
     * Pop). Cached for the JVM lifetime — balance currencies do not change.
     *
     * <p>Returns {@link #defaultCurrency} on any of:
     * <ul>
     *   <li>{@code balanceId} null or blank (legacy callers / sync paths that
     *       only pass the destination — must keep working).</li>
     *   <li>Ledger returns 4xx / 5xx / empty body / missing {@code currency}
     *       field (warn-logged once per balance).</li>
     *   <li>Network error talking to Ledger.</li>
     * </ul>
     * The hold then targets {@code defaultCurrency} ({@code OMS_TRADE_CASH_CURRENCY},
     * USD on the demo). A mismatch with the actual balance currency surfaces as
     * a Ledger 4xx on the {@code POST /transactions} below, which is the
     * correct loud failure for the cross-currency case.
     */
    String resolveBalanceCurrency(String balanceId) {
        if (balanceId == null || balanceId.isBlank()) {
            return defaultCurrency;
        }
        String cached = balanceCurrencyCache.get(balanceId);
        if (cached != null) {
            return cached;
        }
        try {
            ResponseEntity<String> response = http.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/balances/{id}")
                            .queryParam("with_queued", false)
                            .build(balanceId))
                    .header(LEDGER_KEY_HEADER, apiKey)
                    .retrieve()
                    .toEntity(String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                log.warn("[ledger-inflight] balance currency lookup non-2xx balanceId={} status={}; falling back to default={}",
                        balanceId, response.getStatusCode(), defaultCurrency);
                return defaultCurrency;
            }
            String body = response.getBody();
            if (body == null || body.isBlank()) {
                log.warn("[ledger-inflight] balance currency lookup empty body balanceId={}; falling back to default={}",
                        balanceId, defaultCurrency);
                return defaultCurrency;
            }
            JsonNode root = objectMapper.readTree(body);
            JsonNode ccyNode = root.get(BALANCE_CURRENCY_FIELD);
            if (ccyNode == null || !ccyNode.isTextual() || ccyNode.asText().isBlank()) {
                log.warn("[ledger-inflight] balance currency lookup missing field balanceId={}; falling back to default={}",
                        balanceId, defaultCurrency);
                return defaultCurrency;
            }
            String resolved = ccyNode.asText().trim().toUpperCase();
            balanceCurrencyCache.put(balanceId, resolved);
            return resolved;
        } catch (HttpStatusCodeException e) {
            // 404 = balance doesn't exist in Ledger — caller's bug, but we
            // still let the hold attempt go through (Ledger will 4xx the POST
            // too with a clearer error). Don't cache.
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("[ledger-inflight] balance not found balanceId={}; the POST will likely 4xx too", balanceId);
            } else {
                log.warn("[ledger-inflight] balance lookup HTTP {} balanceId={}; falling back to default={}",
                        e.getStatusCode(), balanceId, defaultCurrency);
            }
            return defaultCurrency;
        } catch (RestClientException e) {
            log.warn("[ledger-inflight] balance lookup network error balanceId={}; falling back to default={}: {}",
                    balanceId, defaultCurrency, e.getMessage());
            return defaultCurrency;
        } catch (Exception e) {
            log.warn("[ledger-inflight] balance lookup unexpected error balanceId={}; falling back to default={}",
                    balanceId, defaultCurrency, e);
            return defaultCurrency;
        }
    }

    /** Package-private hook for tests that want to seed the per-balance currency cache. */
    void primeBalanceCurrencyCache(String balanceId, String currency) {
        if (balanceId == null || balanceId.isBlank()) return;
        if (currency == null || currency.isBlank()) return;
        balanceCurrencyCache.put(balanceId, currency.trim().toUpperCase());
    }

    /** Package-private accessor for tests. */
    String defaultCurrency() {
        return defaultCurrency;
    }

    private String extractTransactionId(String responseBody, UUID orderId) {
        if (responseBody == null || responseBody.isBlank()) {
            log.warn("ledger inflight POST returned empty body orderId={}; lifecycle reconciler will skip this hold", orderId);
            return null;
        }
        try {
            JsonNode root = objectMapper.readTree(responseBody);
            JsonNode txnIdNode = root.get(TRANSACTION_ID_FIELD);
            if (txnIdNode == null || !txnIdNode.isTextual() || txnIdNode.asText().isBlank()) {
                log.warn("ledger inflight POST response missing transactionId field orderId={}; lifecycle reconciler will skip this hold", orderId);
                return null;
            }
            return txnIdNode.asText();
        } catch (IOException e) {
            log.warn("ledger inflight POST response parse failed orderId={}; lifecycle reconciler will skip this hold", orderId, e);
            return null;
        }
    }
}
