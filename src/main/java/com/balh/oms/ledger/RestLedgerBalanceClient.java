package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;

/**
 * GET {@code /balances/{id}?with_queued=true} with {@code X-Ledger-Key}.
 */
public final class RestLedgerBalanceClient implements LedgerBalanceClient {

    private static final String LEDGER_KEY_HEADER = "X-Ledger-Key";

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;

    public RestLedgerBalanceClient(RestClient http, String apiKey, ObjectMapper objectMapper) {
        this.http = http;
        this.apiKey = apiKey;
        this.objectMapper = objectMapper;
    }

    @Override
    public BigDecimal fetchAvailableBalance(String balanceId) throws LedgerBalanceClient.LedgerServiceException {
        try {
            JsonNode root = fetchBalanceRoot(balanceId);
            JsonNode ab = root.get("availableBalance");
            if (ab == null || ab.isNull()) {
                throw new LedgerBalanceClient.LedgerServiceException("ledger response missing availableBalance");
            }
            return new BigDecimal(ab.asText());
        } catch (LedgerBalanceClient.LedgerServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new LedgerBalanceClient.LedgerServiceException("failed to parse ledger balance: " + e.getMessage(), e);
        }
    }

    @Override
    public LedgerBalanceReadModel fetchBalanceReadModel(String balanceId) throws LedgerBalanceClient.LedgerServiceException {
        try {
            JsonNode root = fetchBalanceRoot(balanceId);
            JsonNode ab = root.get("availableBalance");
            if (ab == null || ab.isNull()) {
                throw new LedgerBalanceClient.LedgerServiceException("ledger response missing availableBalance");
            }
            BigDecimal available = new BigDecimal(ab.asText());
            JsonNode bb = root.get("balance");
            BigDecimal booked = bb == null || bb.isNull() ? BigDecimal.ZERO : new BigDecimal(bb.asText());
            JsonNode cur = root.get("currency");
            String currency = cur != null && cur.isTextual() ? cur.asText() : "";
            String identityId = readIdentityId(root);
            return new LedgerBalanceReadModel(
                    balanceId, available, booked, currency, identityId == null ? "" : identityId);
        } catch (LedgerBalanceClient.LedgerServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new LedgerBalanceClient.LedgerServiceException(
                    "failed to parse ledger balance read model: " + e.getMessage(), e);
        }
    }

    @Override
    public String fetchIdentityIdForBalance(String balanceId) throws LedgerBalanceClient.LedgerServiceException {
        try {
            JsonNode root = fetchBalanceRoot(balanceId);
            String id = readIdentityId(root);
            if (id == null || id.isBlank()) {
                throw new LedgerBalanceClient.LedgerServiceException("ledger response missing identityId");
            }
            return id.trim();
        } catch (LedgerBalanceClient.LedgerServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new LedgerBalanceClient.LedgerServiceException("failed to read ledger identity: " + e.getMessage(), e);
        }
    }

    private JsonNode fetchBalanceRoot(String balanceId) throws LedgerBalanceClient.LedgerServiceException {
        try {
            ResponseEntity<String> response = http.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/balances/{id}")
                            .queryParam("with_queued", true)
                            .build(balanceId))
                    .header(LEDGER_KEY_HEADER, apiKey)
                    .retrieve()
                    .toEntity(String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new LedgerBalanceClient.LedgerServiceException(
                        "ledger balance GET failed: status=%s".formatted(response.getStatusCode()));
            }
            String body = response.getBody();
            if (body == null || body.isBlank()) {
                throw new LedgerBalanceClient.LedgerServiceException("empty ledger balance response");
            }
            return objectMapper.readTree(body);
        } catch (LedgerBalanceClient.LedgerServiceException e) {
            throw e;
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                throw new LedgerBalanceClient.LedgerServiceException("ledger balance not found");
            }
            throw new LedgerBalanceClient.LedgerServiceException(
                    "ledger balance GET failed: status=%s".formatted(e.getStatusCode()), e);
        } catch (RestClientException e) {
            throw new LedgerBalanceClient.LedgerServiceException("ledger HTTP client error: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new LedgerBalanceClient.LedgerServiceException("failed to parse ledger balance: " + e.getMessage(), e);
        }
    }

    private static String readIdentityId(JsonNode root) {
        JsonNode top = root.get("identityId");
        if (top != null && top.isTextual()) {
            String v = top.asText();
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        JsonNode identity = root.get("identity");
        if (identity != null && identity.isObject()) {
            JsonNode nested = identity.get("identityId");
            if (nested != null && nested.isTextual()) {
                String v = nested.asText();
                if (v != null && !v.isBlank()) {
                    return v;
                }
            }
        }
        return null;
    }
}
