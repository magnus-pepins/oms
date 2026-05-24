package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

/** HTTP client for {@code GET /internal/v1/isk/accounts/{id}/deposits}. */
public final class RestLedgerIskReadClient implements LedgerIskReadClient {

    private final RestClient http;
    private final String elevatedBearerToken;
    private final ObjectMapper objectMapper;

    public RestLedgerIskReadClient(RestClient http, String elevatedBearerToken, ObjectMapper objectMapper) {
        this.http = http;
        this.elevatedBearerToken = elevatedBearerToken;
        this.objectMapper = objectMapper;
    }

    @Override
    public List<DepositRow> listDeposits(String iskAccountId, Instant from, Instant to)
            throws LedgerIskReadException {
        if (iskAccountId == null || iskAccountId.isBlank()) {
            return List.of();
        }
        try {
            StringBuilder path =
                    new StringBuilder("/internal/v1/isk/accounts/")
                            .append(iskAccountId.trim())
                            .append("/deposits");
            String sep = "?";
            if (from != null) {
                path.append(sep).append("from=").append(from);
                sep = "&";
            }
            if (to != null) {
                path.append(sep).append("to=").append(to);
            }
            String body =
                    http.get()
                            .uri(path.toString())
                            .header("Authorization", "Bearer " + elevatedBearerToken)
                            .retrieve()
                            .body(String.class);
            return parseDeposits(body);
        } catch (RestClientResponseException e) {
            HttpStatusCode status = e.getStatusCode();
            if (status.value() == 404) {
                return List.of();
            }
            throw new LedgerIskReadException("ledger ISK deposits HTTP " + status.value() + ": " + e.getMessage(), e);
        } catch (Exception e) {
            throw new LedgerIskReadException("ledger ISK deposits read failed: " + e.getMessage(), e);
        }
    }

    private List<DepositRow> parseDeposits(String body) throws LedgerIskReadException {
        try {
            JsonNode root = objectMapper.readTree(body == null ? "{}" : body);
            JsonNode deposits = root.get("deposits");
            if (deposits == null || !deposits.isArray()) {
                return List.of();
            }
            List<DepositRow> rows = new ArrayList<>();
            for (JsonNode node : deposits) {
                rows.add(
                        new DepositRow(
                                node.path("amountMinor").asLong(0L),
                                node.path("currency").asText("SEK"),
                                node.path("depositClass").asText(""),
                                node.path("countsTowardKapitalunderlag").asBoolean(false),
                                Instant.parse(node.path("effectiveAt").asText())));
            }
            return rows;
        } catch (Exception e) {
            throw new LedgerIskReadException("failed to parse ledger ISK deposits: " + e.getMessage(), e);
        }
    }
}
