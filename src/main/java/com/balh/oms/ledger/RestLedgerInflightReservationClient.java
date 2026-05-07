package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * {@code POST /transactions} with {@code sync: true}, {@code inflight: true}.
 */
public final class RestLedgerInflightReservationClient implements LedgerInflightReservationClient {

    private static final String LEDGER_KEY_HEADER = "X-Ledger-Key";
    private static final String REFERENCE_PREFIX = "oms:order:";

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
    public void placeBuyNotionalHold(UUID orderId, String sourceBalanceId, BigDecimal quantity, BigDecimal limitPrice)
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
        } catch (LedgerReservationException e) {
            throw e;
        } catch (RestClientException e) {
            throw new LedgerReservationException("ledger inflight HTTP error: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new LedgerReservationException("ledger inflight unexpected error: " + e.getMessage(), e);
        }
    }
}
