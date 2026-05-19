package com.balh.oms.ledger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * POSTs settlement outbox events to a configurable Ledger path (defaults in {@code OmsConfig}).
 */
public final class RestLedgerSettlementPostingClient implements LedgerSettlementPostingClient {

    private static final String LEDGER_KEY_HEADER = "X-Ledger-Key";

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;
    private final String httpPath;

    public RestLedgerSettlementPostingClient(
            RestClient http, String apiKey, ObjectMapper objectMapper, String settlementPostingHttpPath) {
        this.http = http;
        this.apiKey = apiKey;
        this.objectMapper = objectMapper;
        String p = settlementPostingHttpPath == null ? "" : settlementPostingHttpPath.trim();
        if (p.isEmpty()) {
            throw new IllegalStateException("oms.ledger.settlement-posting-http-path is required for settlement outbox delivery");
        }
        this.httpPath = p.startsWith("/") ? p : "/" + p;
    }

    @Override
    public void postSettlementOutbox(
            long outboxId, long executionId, String toSettlementStatus, String legKind, String payloadJson)
            throws LedgerSettlementPostingException {
        ObjectNode body = objectMapper.createObjectNode();
        body.put("outboxId", outboxId);
        body.put("executionId", executionId);
        body.put("toSettlementStatus", toSettlementStatus);
        body.put("legKind", legKind == null ? "" : legKind);
        try {
            body.set(
                    "payload",
                    objectMapper.readTree(payloadJson == null || payloadJson.isBlank() ? "{}" : payloadJson));
        } catch (JsonProcessingException e) {
            throw new LedgerSettlementPostingException("invalid payload_json for outbox", e);
        }
        try {
            ResponseEntity<String> response = http.post()
                    .uri(httpPath)
                    .header(LEDGER_KEY_HEADER, apiKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toEntity(String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new LedgerSettlementPostingException(
                        "ledger settlement POST failed: status=%s body=%s"
                                .formatted(response.getStatusCode(), response.getBody()));
            }
        } catch (LedgerSettlementPostingException e) {
            throw e;
        } catch (RestClientException e) {
            throw new LedgerSettlementPostingException("ledger settlement HTTP error: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new LedgerSettlementPostingException("ledger settlement unexpected error: " + e.getMessage(), e);
        }
    }
}
