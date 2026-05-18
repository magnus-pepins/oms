package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * {@code POST /transactions} with {@code sync: true}, {@code inflight: true}.
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

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;
    private final String destinationBalanceId;
    private final String currency;
    private final int precision;

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
        this.currency = currency;
        this.precision = precision;
    }

    @Override
    public String placeBuyNotionalHold(UUID orderId, String sourceBalanceId, BigDecimal quantity, BigDecimal limitPrice)
            throws LedgerReservationException {
        BigDecimal notional = quantity.multiply(limitPrice);
        if (notional.compareTo(BigDecimal.ZERO) <= 0) {
            throw new LedgerReservationException("inflight hold notional must be positive");
        }
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
