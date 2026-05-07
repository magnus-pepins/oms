package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
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
            JsonNode root = objectMapper.readTree(body);
            JsonNode ab = root.get("availableBalance");
            if (ab == null || ab.isNull()) {
                throw new LedgerBalanceClient.LedgerServiceException("ledger response missing availableBalance");
            }
            return new BigDecimal(ab.asText());
        } catch (LedgerBalanceClient.LedgerServiceException e) {
            throw e;
        } catch (RestClientException e) {
            throw new LedgerBalanceClient.LedgerServiceException("ledger HTTP client error: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new LedgerBalanceClient.LedgerServiceException("failed to parse ledger balance: " + e.getMessage(), e);
        }
    }
}
