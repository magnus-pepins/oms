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
 * GET {@code /balances/{id}} with {@code X-Ledger-Key}.
 *
 * <p>Sends {@code with_queued=true} only when the caller actually needs queued debit/credit
 * amounts ({@link #fetchAvailableBalance(String)}, {@link #fetchBalanceReadModel(String)}).
 * The verify-only path ({@link #fetchIdentityIdForBalance(String)}) sends
 * {@code with_queued=false}, because it reads only the durable {@code identityId} and the
 * Ledger {@code BalanceCache} (Redis, 60 s TTL — see
 * {@code ledger/src/services/balance.service.ts:84} and {@code ledger/src/cache/balance-cache.ts})
 * is bypassed when {@code withQueued === true}. Tier 2.5 phase C-3 measurement on Pop! showed
 * <ul>
 *   <li>{@code ?with_queued=true}: 2–28 ms single call, ~100 ms under 50 parallel calls.</li>
 *   <li>{@code ?with_queued=false}: 0.8–1.1 ms single call, ~0.27 ms under 50 parallel calls
 *       (≈375× faster on the cache-hit path).</li>
 * </ul>
 * Sending {@code true} on the verify path also runs the {@code getQueuedAmounts} SELECT on
 * {@code transactions} for every request, which served no purpose for OMS verify and consumed
 * a Postgres connection that mattered under load. See
 * {@code oms/docs/runbooks/local-multi-jvm-bench.md} "Tier 2.5 phase C-3" for the full evidence.
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
            JsonNode root = fetchBalanceRoot(balanceId, true);
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
            JsonNode root = fetchBalanceRoot(balanceId, true);
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
            JsonNode ind = root.get("indicator");
            String indicator = ind != null && ind.isTextual() ? ind.asText() : "";
            return new LedgerBalanceReadModel(
                    balanceId, available, booked, currency,
                    identityId == null ? "" : identityId, indicator);
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
            // withQueued=false: the (balanceId -> identityId) binding is durable, so we want this
            // call to hit Ledger's Redis cache (which is bypassed when with_queued=true). See
            // class-level Javadoc for measured impact.
            JsonNode root = fetchBalanceRoot(balanceId, false);
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

    private JsonNode fetchBalanceRoot(String balanceId, boolean withQueued)
            throws LedgerBalanceClient.LedgerServiceException {
        try {
            ResponseEntity<String> response = http.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/balances/{id}")
                            .queryParam("with_queued", withQueued)
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
