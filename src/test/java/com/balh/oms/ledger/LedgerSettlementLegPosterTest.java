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
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
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
        // Default resolver stub: any indicator lookup returns a single-row response with a
        // deterministic balanceId so the leg poster has a concrete id to send into the
        // {@code POST /transactions}. Each test that asserts on the resolved id stubs a
        // more specific pattern; this default keeps the resolver from 500-ing on a miss.
        ledger.stubFor(get(urlPathEqualTo("/balances"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("[{\"balanceId\":\"balance_default\",\"currency\":\"USD\"}]")));
    }

    /**
     * Stubs the {@code GET /balances?indicator=<indicator>} resolver lookup to return a
     * single-row response carrying {@code balanceId}. Tests use this to make the
     * customer-side {@code source}/{@code destination} on the resulting
     * {@code POST /transactions} predictable.
     */
    private void stubCustomerResolution(String indicator, String currency, String balanceId) {
        ledger.stubFor(get(urlPathEqualTo("/balances"))
                .withQueryParam("indicator", equalTo(indicator))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("[{\"balanceId\":\"" + balanceId
                                + "\",\"currency\":\"" + currency + "\",\"indicator\":\""
                                + indicator + "\"}]")));
    }

    @AfterEach
    void stop() {
        ledger.stop();
    }

    @Test
    void buyCashSingleCurrencyDebitsCustomerCreditsNostro() throws Exception {
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000001-USD", "USD", "balance_cust_001_usd");
        String payload = """
                {"schemaVersion":2,"leg":"cash","executionId":111,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"10","price":"5.50",
                 "tradeCurrency":"USD","cashCurrency":"USD","notional":"55.00"}""";

        poster.postSettlementOutbox(42L, 111L, "settled", LedgerSettlementOutboxRepository.LEG_CASH, payload);

        ledger.verify(1, getRequestedFor(urlPathEqualTo("/balances"))
                .withQueryParam("indicator", equalTo("inv-a0000000-0000-4000-8000-000000000001-USD")));
        ledger.verify(1, postWith()
                .withHeader("Authorization", equalTo("Bearer test-key"))
                .withRequestBody(containing("\"source\":\"balance_cust_001_usd\""))
                .withRequestBody(containing("\"destination\":\"@Nostro-USD-Bank\""))
                .withRequestBody(containing("\"amount\":55.0"))
                .withRequestBody(containing("\"currency\":\"USD\""))
                .withRequestBody(containing("\"reference\":\"settlement-42-cash\""))
                .withRequestBody(containing("\"sync\":true")));
    }

    @Test
    void sellCashSingleCurrencyDebitsNostroCreditsCustomer() throws Exception {
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000002-USD", "USD", "balance_cust_002_usd");
        String payload = """
                {"schemaVersion":2,"leg":"cash","executionId":222,
                 "accountId":"a0000000-0000-4000-8000-000000000002","side":"SELL",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"3","price":"100",
                 "tradeCurrency":"USD","cashCurrency":"USD","notional":"300"}""";

        poster.postSettlementOutbox(7L, 222L, "settled", LedgerSettlementOutboxRepository.LEG_CASH, payload);

        ledger.verify(1, postWith()
                .withRequestBody(containing("\"source\":\"@Nostro-USD-Bank\""))
                .withRequestBody(containing("\"destination\":\"balance_cust_002_usd\""))
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
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000001-USD", "USD", "balance_cust_001_usd");
        String payload = """
                {"schemaVersion":2,"leg":"fee","executionId":111,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","feeAmount":"1.50","feeCurrency":"USD",
                 "feeBalanceIndicator":"@Fees-USD","notional":"100.00"}""";

        poster.postSettlementOutbox(99L, 111L, "settled", LedgerSettlementOutboxRepository.LEG_FEE, payload);

        ledger.verify(1, postWith()
                .withRequestBody(containing("\"source\":\"balance_cust_001_usd\""))
                .withRequestBody(containing("\"destination\":\"@Fees-USD\""))
                .withRequestBody(containing("\"amount\":1.5"))
                .withRequestBody(containing("\"reference\":\"settlement-99-fee\"")));
    }

    @Test
    void feeLegNormalizesLegacyPlatformRevenueIndicator() throws Exception {
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000001-USD", "USD", "balance_cust_001_usd");
        String payload = """
                {"schemaVersion":2,"leg":"fee","executionId":112,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","feeAmount":"0.15","feeCurrency":"USD",
                 "feeBalanceIndicator":"@Platform-Revenue","notional":"50.00"}""";

        poster.postSettlementOutbox(100L, 112L, "settled", LedgerSettlementOutboxRepository.LEG_FEE, payload);

        ledger.verify(1, postWith()
                .withRequestBody(containing("\"destination\":\"@Platform-Revenue-USD\""))
                .withRequestBody(containing("\"reference\":\"settlement-100-fee\"")));
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
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000003-EUR", "EUR", "balance_cust_003_eur");
        String payload = """
                {"schemaVersion":2,"leg":"cash-base","executionId":300,
                 "accountId":"a0000000-0000-4000-8000-000000000003","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"10","price":"5",
                 "tradeCurrency":"USD","cashCurrency":"EUR","notional":"50.00","cashAmount":"46.30"}""";

        poster.postSettlementOutbox(
                12L, 300L, "settled", LedgerSettlementOutboxRepository.LEG_CASH_BASE, payload);

        ledger.verify(1, postWith()
                .withRequestBody(containing("\"source\":\"balance_cust_003_eur\""))
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
    void ledger404IndicatorNotFoundMapsToSkippedReason() {
        ledger.resetAll();
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000001-USD", "USD", "balance_cust_001_usd");
        ledger.stubFor(post(urlPathEqualTo("/transactions"))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withHeader("Content-Type", "application/json")
                        .withBody(
                                "{\"error\":\"Balance not found: indicator=@Platform-Revenue, currency=USD\",\"code\":\"NOT_FOUND\"}")));
        String payload = """
                {"schemaVersion":2,"leg":"fee","executionId":92,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "feeCurrency":"USD","feeAmount":"0.15","feeBalanceIndicator":"@Platform-Revenue"}""";

        assertThatThrownBy(() ->
                        poster.postSettlementOutbox(
                                42, 92, "settled", LedgerSettlementOutboxRepository.LEG_FEE, payload))
                .isInstanceOf(LedgerSettlementPostingException.class)
                .satisfies(ex -> assertThat(((LedgerSettlementPostingException) ex).reason())
                        .isEqualTo(LedgerSettlementPostingException.Reason.SKIPPED_INDICATOR_NOT_FOUND));
    }

    @Test
    void ledgerErrorIsWrappedSoReconcilerCanRetry() {
        ledger.resetAll();
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000001-USD", "USD", "balance_cust_001_usd");
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
    void duplicateReferencePreCheckSkipsPost() throws Exception {
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000001-USD", "USD", "balance_cust_001_usd");
        ledger.stubFor(get(urlPathEqualTo("/transactions"))
                .withQueryParam("reference", equalTo("settlement-42-cash"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"transactionId\":\"txn-existing\"}")));
        String payload = """
                {"schemaVersion":2,"leg":"cash","executionId":111,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"10","price":"5.50",
                 "tradeCurrency":"USD","cashCurrency":"USD","notional":"55.00"}""";

        poster.postSettlementOutbox(42L, 111L, "settled", LedgerSettlementOutboxRepository.LEG_CASH, payload);

        ledger.verify(1, getRequestedFor(urlPathEqualTo("/transactions"))
                .withQueryParam("reference", equalTo("settlement-42-cash")));
        ledger.verify(0, postRequestedFor(urlPathEqualTo("/transactions")));
    }

    @Test
    void duplicateReference409IsTreatedAsSuccess() throws Exception {
        stubCustomerResolution(
                "inv-a0000000-0000-4000-8000-000000000001-USD", "USD", "balance_cust_001_usd");
        ledger.stubFor(get(urlPathEqualTo("/transactions"))
                .withQueryParam("reference", equalTo("settlement-42-cash"))
                .inScenario("dup409")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(404))
                .willSetStateTo("Posted"));
        ledger.stubFor(get(urlPathEqualTo("/transactions"))
                .withQueryParam("reference", equalTo("settlement-42-cash"))
                .inScenario("dup409")
                .whenScenarioStateIs("Posted")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"transactionId\":\"txn-existing\"}")));
        ledger.stubFor(post(urlPathEqualTo("/transactions"))
                .willReturn(aResponse()
                        .withStatus(409)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"code\":\"CONFLICT\",\"error\":\"Reference already used\"}")));
        String payload = """
                {"schemaVersion":2,"leg":"cash","executionId":111,
                 "accountId":"a0000000-0000-4000-8000-000000000001","side":"BUY",
                 "instrumentSymbol":"AAPL","market":"US","quantity":"10","price":"5.50",
                 "tradeCurrency":"USD","cashCurrency":"USD","notional":"55.00"}""";

        poster.postSettlementOutbox(42L, 111L, "settled", LedgerSettlementOutboxRepository.LEG_CASH, payload);

        ledger.verify(2, getRequestedFor(urlPathEqualTo("/transactions"))
                .withQueryParam("reference", equalTo("settlement-42-cash")));
        ledger.verify(1, postRequestedFor(urlPathEqualTo("/transactions")));
    }

    @Test
    void caDividendUsesLedgerBalanceIdFromPayload() throws Exception {
        String payload = """
                {"schemaVersion":1,"leg":"dividend","accountId":"a0000000-0000-4000-8000-000000000004",
                 "currency":"SEK","netAmount":"12.50","ledgerBalanceId":"balance_isk_sek_1",
                 "iskDepositClass":"dividend","nostroIndicator":"@Nostro-SEK"}""";

        poster.postCorporateActionOutbox(55L, "dividend", payload);

        ledger.verify(0, getRequestedFor(urlPathEqualTo("/balances")));
        ledger.verify(1, postWith()
                .withRequestBody(containing("\"destination\":\"balance_isk_sek_1\""))
                .withRequestBody(containing("\"reference\":\"ca-dividend-55\"")));
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
