package com.balh.oms.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.Objects;

/**
 * Wed-demo (V32): {@code PUT /transactions/inflight/{txID}} REST client. Targets the
 * ledger-cluster shim's {@code InflightController} (mirror of the TS Ledger's same endpoint).
 *
 * <p>Body shape per the cluster shim's {@code UpdateInflightRequest}: {@code {"status": "commit"}}
 * for fills, {@code {"status": "void"}} for cancels/rejects/expiries. The Ledger looks up the
 * inflight transaction by the {@code txID} path-param.
 *
 * <p>Idempotency is provided at the Ledger side — a second commit/void on an already-settled
 * hold returns 2xx via the Ledger's "already in requested state" branch. The reconciler does
 * not rely on this for happy-path correctness (the working-set query filters out settled rows)
 * but it does mean a reconciler restart that re-fetches a row mid-call is safe.
 */
public final class RestLedgerInflightLifecycleClient implements LedgerInflightLifecycleClient {

    private static final String LEDGER_KEY_HEADER = "X-Ledger-Key";
    private static final String INFLIGHT_PATH = "/transactions/inflight/{txID}";
    static final String ACTION_COMMIT = "commit";
    static final String ACTION_VOID = "void";

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;

    public RestLedgerInflightLifecycleClient(RestClient http, String apiKey, ObjectMapper objectMapper) {
        this.http = Objects.requireNonNull(http, "http");
        this.apiKey = Objects.requireNonNull(apiKey, "apiKey");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    @Override
    public void commitHold(String ledgerTxnId) throws LedgerLifecycleException {
        callPut(ledgerTxnId, ACTION_COMMIT);
    }

    @Override
    public void voidHold(String ledgerTxnId) throws LedgerLifecycleException {
        callPut(ledgerTxnId, ACTION_VOID);
    }

    private void callPut(String ledgerTxnId, String action) throws LedgerLifecycleException {
        if (ledgerTxnId == null || ledgerTxnId.isBlank()) {
            throw new LedgerLifecycleException("ledger_txn_id is required");
        }
        ObjectNode body = objectMapper.createObjectNode();
        body.put("status", action);
        try {
            ResponseEntity<String> response = http.put()
                    .uri(INFLIGHT_PATH, ledgerTxnId)
                    .header(LEDGER_KEY_HEADER, apiKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toEntity(String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new LedgerLifecycleException(
                        "ledger inflight " + action + " PUT failed: status=%s body=%s"
                                .formatted(response.getStatusCode(), response.getBody()));
            }
        } catch (LedgerLifecycleException e) {
            throw e;
        } catch (RestClientException e) {
            throw new LedgerLifecycleException(
                    "ledger inflight " + action + " HTTP error: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new LedgerLifecycleException(
                    "ledger inflight " + action + " unexpected error: " + e.getMessage(), e);
        }
    }
}
