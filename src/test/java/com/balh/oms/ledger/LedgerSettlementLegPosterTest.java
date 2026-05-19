package com.balh.oms.ledger;

import com.balh.oms.ledger.LedgerSettlementPostingClient.LedgerSettlementPostingException;
import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit-level coverage of the leg dispatch and Ledger transaction body shape.
 * The settlement reconciler integration test exercises Postgres + Spring; this
 * suite isolates the per-leg routing logic against a WireMock'd Ledger.
 */
class LedgerSettlementLegPosterTest {

    private static final String OK_RESPONSE = "{\"transactionId\":\"txn-stub\"}";

    private WireMockServer ledger;
    private LedgerSettlementLegPoster poster;

    @BeforeEach
    void start() {
        ledger = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        ledger.start();
        RestClient http = RestClient.builder().baseUrl("http://127.0.0.1:" + ledger.port()).build();
        poster = new LedgerSettlementLegPoster(http, "test-key", new ObjectMapper());
        ledger.stubFor(post(urlPathEqualTo("/transactions"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody(OK_RESPONSE)));
    }

    @AfterEach
    void stop() {
        ledger.stop();
    }

    @Test
    void buyCashSingleCurrencyDebitsCustomerCreditsNostro() throws Exception {
        String payload = """
                {"schemaVersion":2,"leg":"cash","executionId":111,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"10","price":"5.50",
                 "tradeCurrency":"USD","cashCurrency":"USD","notional":"55.00"}""";

        poster.postSettlementOutbox(42L, 111L, "settled", LedgerSettlementOutboxRepository.LEG_CASH, payload);

        ledger.verify(1, postWith()
                .withHeader("X-Ledger-Key", equalTo("test-key"))
                .withRequestBody(containing("\"source\":\"inv-a0000000-0000-4000-8000-000000000001-USD\""))
                .withRequestBody(containing("\"destination\":\"@Nostro-USD-Bank\""))
                .withRequestBody(containing("\"amount\":55.0"))
                .withRequestBody(containing("\"currency\":\"USD\""))
                .withRequestBody(containing("\"reference\":\"settlement-42-cash\""))
                .withRequestBody(containing("\"sync\":true")));
    }

    @Test
    void sellCashSingleCurrencyDebitsNostroCreditsCustomer() throws Exception {
        String payload = """
                {"schemaVersion":2,"leg":"cash","executionId":222,
                 "accountId":"a0000000-0000-4000-8000-000000000002","side":"SELL",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"3","price":"100",
                 "tradeCurrency":"USD","cashCurrency":"USD","notional":"300"}""";

        poster.postSettlementOutbox(7L, 222L, "settled", LedgerSettlementOutboxRepository.LEG_CASH, payload);

        ledger.verify(1, postWith()
                .withRequestBody(containing("\"source\":\"@Nostro-USD-Bank\""))
                .withRequestBody(containing("\"destination\":\"inv-a0000000-0000-4000-8000-000000000002-USD\""))
                .withRequestBody(containing("\"reference\":\"settlement-7-cash\"")));
    }

    @Test
    void cashLegRejectsCrossCurrencyPayload() {
        String payload = """
                {"schemaVersion":2,"leg":"cash","executionId":1,"accountId":"a0000000-0000-4000-8000-000000000001",
                 "side":"BUY","instrumentSymbol":"AAPL","market":"US","quantity":"1","price":"5",
                 "tradeCurrency":"USD","cashCurrency":"EUR","notional":"5"}""";

        assertThatThrownBy(() ->
                        poster.postSettlementOutbox(1, 1, "settled", LedgerSettlementOutboxRepository.LEG_CASH, payload))
                .isInstanceOf(LedgerSettlementPostingException.class)
                .hasMessageContaining("use cash-base/cash-quote");
        ledger.verify(0, postRequestedFor(urlPathEqualTo("/transactions")));
    }

    @Test
    void feeLegDebitsCustomerCreditsFeesRevenue() throws Exception {
        String payload = """
                {"schemaVersion":2,"leg":"fee","executionId":111,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","feeAmount":"1.50","feeCurrency":"USD",
                 "feeBalanceIndicator":"@Fees-USD","notional":"100.00"}""";

        poster.postSettlementOutbox(99L, 111L, "settled", LedgerSettlementOutboxRepository.LEG_FEE, payload);

        ledger.verify(1, postWith()
                .withRequestBody(containing("\"source\":\"inv-a0000000-0000-4000-8000-000000000001-USD\""))
                .withRequestBody(containing("\"destination\":\"@Fees-USD\""))
                .withRequestBody(containing("\"amount\":1.5"))
                .withRequestBody(containing("\"reference\":\"settlement-99-fee\"")));
    }

    @Test
    void feeLegSkippedWhenAmountIsZero() throws Exception {
        String payload = """
                {"schemaVersion":2,"leg":"fee","executionId":1,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","feeAmount":"0","feeCurrency":"USD",
                 "feeBalanceIndicator":"@Fees-USD","notional":"5"}""";

        poster.postSettlementOutbox(1, 1, "settled", LedgerSettlementOutboxRepository.LEG_FEE, payload);

        ledger.verify(0, postRequestedFor(urlPathEqualTo("/transactions")));
    }

    @Test
    void cashBaseLegPostsCustomerToFxSuspenseInCashCurrency() throws Exception {
        String payload = """
                {"schemaVersion":2,"leg":"cash-base","executionId":300,
                 "accountId":"a0000000-0000-4000-8000-000000000003","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"10","price":"5",
                 "tradeCurrency":"USD","cashCurrency":"EUR","notional":"50.00","cashAmount":"46.30"}""";

        poster.postSettlementOutbox(
                12L, 300L, "settled", LedgerSettlementOutboxRepository.LEG_CASH_BASE, payload);

        ledger.verify(1, postWith()
                .withRequestBody(containing("\"source\":\"inv-a0000000-0000-4000-8000-000000000003-EUR\""))
                .withRequestBody(containing("\"destination\":\"@FX-Suspense-EUR\""))
                .withRequestBody(containing("\"amount\":46.3"))
                .withRequestBody(containing("\"currency\":\"EUR\""))
                .withRequestBody(containing("\"reference\":\"settlement-12-cash-base\"")));
    }

    @Test
    void cashQuoteLegPostsFxSuspenseToNostroInTradeCurrency() throws Exception {
        String payload = """
                {"schemaVersion":2,"leg":"cash-quote","executionId":300,
                 "accountId":"a0000000-0000-4000-8000-000000000003","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"10","price":"5",
                 "tradeCurrency":"USD","cashCurrency":"EUR","notional":"50.00","cashAmount":"46.30"}""";

        poster.postSettlementOutbox(
                12L, 300L, "settled", LedgerSettlementOutboxRepository.LEG_CASH_QUOTE, payload);

        ledger.verify(1, postWith()
                .withRequestBody(containing("\"source\":\"@FX-Suspense-USD\""))
                .withRequestBody(containing("\"destination\":\"@Nostro-USD-Bank\""))
                .withRequestBody(containing("\"amount\":50.0"))
                .withRequestBody(containing("\"currency\":\"USD\""))
                .withRequestBody(containing("\"reference\":\"settlement-12-cash-quote\"")));
    }

    @Test
    void unknownLegKindIsRejected() {
        assertThatThrownBy(() -> poster.postSettlementOutbox(1, 1, "settled", "weird", "{}"))
                .isInstanceOf(LedgerSettlementPostingException.class)
                .hasMessageContaining("unknown legKind");
    }

    @Test
    void ledgerErrorIsWrappedSoReconcilerCanRetry() {
        ledger.resetAll();
        ledger.stubFor(post(urlPathEqualTo("/transactions"))
                .willReturn(aResponse().withStatus(500).withBody("ledger down")));
        String payload = """
                {"schemaVersion":2,"leg":"cash","executionId":1,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"1","price":"1",
                 "tradeCurrency":"USD","cashCurrency":"USD","notional":"1"}""";

        assertThatThrownBy(() ->
                        poster.postSettlementOutbox(1, 1, "settled", LedgerSettlementOutboxRepository.LEG_CASH, payload))
                .isInstanceOf(LedgerSettlementPostingException.class)
                .hasMessageContaining("HTTP 500");
    }

    @Test
    void missingApiKeyAtConstructionIsRejected() {
        assertThatThrownBy(() ->
                        new LedgerSettlementLegPoster(
                                RestClient.builder().baseUrl("http://localhost").build(), "", new ObjectMapper()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("api-key is required");
    }

    private static RequestPatternBuilder postWith() {
        return postRequestedFor(urlPathEqualTo("/transactions"));
    }

    // Quiet unused-import linter — UrlPattern is illustrative for future filter additions.
    @SuppressWarnings("unused")
    private static final UrlPattern TRANSACTIONS_URL = urlPathEqualTo("/transactions");
}
