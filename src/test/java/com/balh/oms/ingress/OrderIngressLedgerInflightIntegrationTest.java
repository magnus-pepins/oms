package com.balh.oms.ingress;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ledger sync inflight hold on BUY ingress when {@code oms.ledger.inflight-reservation-enabled=true}.
 */
class OrderIngressLedgerInflightIntegrationTest extends AbstractPostgresIntegrationTest {

    private static volatile WireMockServer ledgerWireMock;

    @DynamicPropertySource
    static void registerLedgerInflight(DynamicPropertyRegistry registry) {
        synchronized (OrderIngressLedgerInflightIntegrationTest.class) {
            if (ledgerWireMock == null) {
                ledgerWireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
                ledgerWireMock.start();
            }
        }
        registry.add("oms.ledger.enabled", () -> "true");
        registry.add("oms.ledger.base-url", () -> "http://127.0.0.1:" + ledgerWireMock.port());
        registry.add("oms.ledger.api-key", () -> "it-key");
        registry.add("oms.ledger.inflight-reservation-enabled", () -> "true");
        registry.add("oms.ledger.inflight-hold-destination-balance-id", () -> "hold_dest_balance");
    }

    @AfterAll
    static void stopLedgerWireMock() {
        if (ledgerWireMock != null) {
            ledgerWireMock.stop();
            ledgerWireMock = null;
        }
    }

    @LocalServerPort int port;
    @Autowired TestRestTemplate http;

    @BeforeEach
    void resetStubs() {
        ledgerWireMock.resetAll();
    }

    @Test
    void rejectsBuyWhenLedgerIdentityDoesNotMatchClaim() {
        // Verify path sends with_queued=false (Tier 2.5 phase C-3, see RestLedgerBalanceClient).
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/cust_balance_1"))
                .withQueryParam("with_queued", equalTo("false"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"availableBalance\":\"999\",\"identityId\":\"ledger-actual-owner\"}")));

        UUID accountId = UUID.randomUUID();
        ResponseEntity<ApiErrorResponse> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(jsonBody(accountId, "inflight-bad-id", "cust_balance_1", "wrong-claim-id"), authHeaders()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().message()).isEqualTo("ledger_identity_mismatch");
    }

    @Test
    void buyOrderWithInflightPostsLedgerSyncTransaction() {
        ledgerWireMock.stubFor(get(urlPathEqualTo("/balances/cust_balance_1"))
                .withQueryParam("with_queued", equalTo("false"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"availableBalance\":\"999\",\"identityId\":\"ident-inflight-it\"}")));
        ledgerWireMock.stubFor(post(urlPathEqualTo("/transactions"))
                .withHeader("Authorization", equalTo("Bearer it-key"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"transactionId\":\"inflight-tx-1\"}")));

        UUID accountId = UUID.randomUUID();
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(jsonBody(accountId, "inflight-it-1", "cust_balance_1", "ident-inflight-it"), authHeaders()),
                new ParameterizedTypeReference<>() {});

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ledgerWireMock.verify(1, postRequestedFor(urlPathEqualTo("/transactions")));
        ledgerWireMock.verify(1, getRequestedFor(urlPathEqualTo("/balances/cust_balance_1")));
    }

    private static HttpHeaders authHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
    }

    private static String jsonBody(UUID accountId, String idempotencyKey, String ledgerBalanceId, String ledgerIdentityId) {
        return """
                {
                  "accountId": "%s",
                  "clientIdempotencyKey": "%s",
                  "side": "BUY",
                  "instrumentSymbol": "AAPL",
                  "quantity": "2",
                  "limitPrice": "10.00",
                  "timeInForce": "DAY",
                  "ledgerBalanceId": "%s",
                  "ledgerIdentityId": "%s"
                }
                """.formatted(accountId, idempotencyKey, ledgerBalanceId, ledgerIdentityId);
    }
}
