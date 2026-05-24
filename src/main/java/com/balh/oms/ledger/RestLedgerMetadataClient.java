package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClient;

/** POST {@code /{balanceId}/metadata} for ISK pending_position_count sync. */
public final class RestLedgerMetadataClient implements LedgerMetadataClient {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;

    public RestLedgerMetadataClient(RestClient http, String apiKey, ObjectMapper objectMapper) {
        this.http = http;
        this.apiKey = apiKey;
        this.objectMapper = objectMapper;
    }

    @Override
    public void patchBalanceMetadata(String balanceId, Map<String, Object> patch)
            throws LedgerMetadataClient.LedgerMetadataException {
        if (balanceId == null || balanceId.isBlank()) {
            throw new LedgerMetadataClient.LedgerMetadataException("balanceId required");
        }
        Map<String, Object> merged = new HashMap<>();
        try {
            String body = http.get()
                    .uri("/balances/{id}", balanceId.trim())
                    .header(AUTHORIZATION_HEADER, BEARER_PREFIX + apiKey)
                    .retrieve()
                    .body(String.class);
            if (body != null && !body.isBlank()) {
                JsonNode root = objectMapper.readTree(body);
                JsonNode meta = root.get("metaData");
                if (meta != null && meta.isObject()) {
                    merged.putAll(objectMapper.convertValue(meta, Map.class));
                }
            }
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
                throw new LedgerMetadataClient.LedgerMetadataException(
                        "ledger balance read failed: " + e.getStatusCode(), e);
            }
        } catch (Exception e) {
            throw new LedgerMetadataClient.LedgerMetadataException("ledger balance read failed: " + e.getMessage(), e);
        }
        merged.putAll(patch);
        try {
            http.post()
                    .uri("/{entityId}/metadata", balanceId.trim())
                    .header(AUTHORIZATION_HEADER, BEARER_PREFIX + apiKey)
                    .body(Map.of("metaData", merged))
                    .retrieve()
                    .toBodilessEntity();
        } catch (Exception e) {
            throw new LedgerMetadataClient.LedgerMetadataException("metadata patch failed: " + e.getMessage(), e);
        }
    }
}
