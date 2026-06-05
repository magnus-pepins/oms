package com.balh.oms.ingress;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterShardRouter;
import com.balh.oms.cluster.OrderAcceptedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.fx.FxCustomerFlowNettingService;
import com.balh.oms.fx.FxQuoteService;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.LedgerInflightCoalescer;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.balh.oms.observability.PiiHash;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
// Instant kept for the CachedQuote fixture below.
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * §8.4 quote-lock recall guard tests. Verifies the {@link OrderIngressService}
 * accept path
 * <ul>
 *   <li>skips the recall entirely when {@code oms.fx.accept-use-quoter.enabled=false}
 *       (regardless of whether the request carries a {@code fxQuoteId}) — the
 *       single-currency byte-identity guarantee;</li>
 *   <li>skips the recall when the flag is on but the request has no
 *       {@code fxQuoteId} — single-currency orders forwarded by an
 *       FX-aware BFF still flow through unmodified;</li>
 *   <li>rejects with {@code RISK_FX_QUOTE_EXPIRED} + HTTP 422 when the recall
 *       returns null (= quote missing or already expired in the cache);</li>
 *   <li>passes through to the cluster admit when the recall hits.</li>
 * </ul>
 */
class OrderIngressServiceFxQuoteLockTest {

    private OrdersRepository orders;
    private PiiHash piiHash;
    private OrderControlAdmission orderControlAdmission;
    private OmsClusterIngressClient cluster;
    private FxQuoteService fxSvc;

    private OmsConfig config;
    private OrderIngressService service;
    private VenueAdmissionGate venueAdmissionGate;
    private PredictionMarketTickGate predictionMarketTickGate;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        orders = mock(OrdersRepository.class);
        piiHash = mock(PiiHash.class);
        orderControlAdmission = mock(OrderControlAdmission.class);
        cluster = mock(OmsClusterIngressClient.class);
        fxSvc = mock(FxQuoteService.class);

        config = new OmsConfig();
        config.getShard().setCount(1);
        config.getCluster().getClient().setEnabled(true);
        config.getCluster().getClient().setSubmitTimeoutMs(2_000L);

        when(piiHash.hash(any())).thenReturn("hash");
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenAnswer(inv -> {
                    AcceptOrderCommand cmd = inv.getArgument(0);
                    return new AdmissionResult.Accepted(
                            new OrderAcceptedEvent(
                                    cmd.correlationId(),
                                    cmd.orderId(),
                                    /* version = */ 0,
                                    /* duplicate = */ false,
                                    /* acceptedAtMillis = */ 1L));
                });

        ObjectProvider<LedgerInflightReservationClient> ledgerInflight =
                (ObjectProvider<LedgerInflightReservationClient>) mock(ObjectProvider.class);
        when(ledgerInflight.getIfAvailable()).thenReturn(null);
        ObjectProvider<LedgerInflightCoalescer> ledgerInflightCoalescer =
                (ObjectProvider<LedgerInflightCoalescer>) mock(ObjectProvider.class);
        when(ledgerInflightCoalescer.getIfAvailable()).thenReturn(null);
        ObjectProvider<LedgerBalanceClient> ledgerBalance =
                (ObjectProvider<LedgerBalanceClient>) mock(ObjectProvider.class);
        when(ledgerBalance.getIfAvailable()).thenReturn(null);
        ObjectProvider<FxQuoteService> fxProvider =
                (ObjectProvider<FxQuoteService>) mock(ObjectProvider.class);
        when(fxProvider.getIfAvailable()).thenReturn(fxSvc);
        ObjectProvider<FxCustomerFlowNettingService> nettingProvider =
                (ObjectProvider<FxCustomerFlowNettingService>) mock(ObjectProvider.class);
        when(nettingProvider.getIfAvailable()).thenReturn(null);

        OmsClusterShardRouter router = new OmsClusterShardRouter(1, Map.of(0, cluster));
        // AAPL orders only; gate short-circuits before the lag snapshot (mock keeps it safe).
        venueAdmissionGate = new VenueAdmissionGate(
                config, mock(OmsVenueEgressLagPublisher.class), new SimpleMeterRegistry());
        predictionMarketTickGate = new PredictionMarketTickGate(
                config,
                mock(com.balh.oms.predictionmarket.PredictionMarketContractRepository.class),
                new SimpleMeterRegistry());
        service = new OrderIngressService(
                orders, config, piiHash,
                ledgerInflight, ledgerInflightCoalescer, ledgerBalance, fxProvider, nettingProvider,
                new SimpleMeterRegistry(), orderControlAdmission, router, venueAdmissionGate,
                predictionMarketTickGate);
    }

    @Test
    void flagOff_skipsRecall_evenWhenQuoteIdPresent() {
        // Default: oms.fx.accept-use-quoter.enabled = false
        CreateOrderRequest req = buildRequest("q_legacy", new BigDecimal("100.00"));

        OrderIngressService.IngressResult res = service.persistAccepted(req);

        assertThat(res.order().instrumentSymbol()).isEqualTo("AAPL");
        verify(fxSvc, never()).recall(any());
    }

    @Test
    void flagOn_singleCurrency_skipsRecall() {
        config.getFx().setAcceptUseQuoterEnabled(true);
        CreateOrderRequest req = buildRequest(/* fxQuoteId = */ null, /* cashHoldAmount = */ null);

        OrderIngressService.IngressResult res = service.persistAccepted(req);

        assertThat(res.order().instrumentSymbol()).isEqualTo("AAPL");
        verify(fxSvc, never()).recall(any());
    }

    @Test
    void flagOn_recallMiss_throwsFxQuoteLockExceptionAs422() {
        config.getFx().setAcceptUseQuoterEnabled(true);
        when(fxSvc.recall("q_gone")).thenReturn(null);
        CreateOrderRequest req = buildRequest("q_gone", new BigDecimal("100.00"));

        assertThatThrownBy(() -> service.persistAccepted(req))
                .isInstanceOfSatisfying(FxQuoteLockException.class, ex -> {
                    assertThat(ex.getHttpStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
                    assertThat(ex.getRejectCode()).isEqualTo(RejectCode.RISK_FX_QUOTE_EXPIRED);
                    assertThat(ex.getErrorCode()).isEqualTo("fx_quote_expired");
                    assertThat(ex.getMessage()).contains("q_gone");
                });
        verify(fxSvc, times(1)).recall("q_gone");
    }

    @Test
    void flagOn_recallHit_passesThroughToClusterAdmit() throws Exception {
        config.getFx().setAcceptUseQuoterEnabled(true);
        Instant now = Instant.now();
        when(fxSvc.recall("q_ok")).thenReturn(new FxQuoteService.CachedQuote(
                "q_ok", "EURUSD", "basic",
                new BigDecimal("1.0820"), new BigDecimal("1.0880"), new BigDecimal("1.0850"),
                now, now.plusSeconds(30)));
        // No cashHoldAmount on this request → integrity check short-circuits.
        // limitPrice is null (MARKET BUY without ref price) so the request
        // doesn't trigger any inflight-hold path either.
        CreateOrderRequest req = buildRequest("q_ok", /* cashHoldAmount = */ null);

        OrderIngressService.IngressResult res = service.persistAccepted(req);

        assertThat(res.created()).isTrue();
        assertThat(res.order().instrumentSymbol()).isEqualTo("AAPL");
        verify(fxSvc, times(1)).recall("q_ok");
        verify(cluster, times(1)).submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class));
    }

    @Test
    void flagOn_recallHit_integrityCheckPasses_withinTolerance() throws Exception {
        // qty=10 px=100 USD / bid=1.0820 EURUSD = 924.21441… EUR (10 dp)
        // Provide 924.214 (off by 0.0004 EUR ≈ 0.04 bps) → within default 5 bps tolerance.
        config.getFx().setAcceptUseQuoterEnabled(true);
        Instant now = Instant.now();
        when(fxSvc.recall("q_ok")).thenReturn(new FxQuoteService.CachedQuote(
                "q_ok", "EURUSD", "basic",
                new BigDecimal("1.0820"), new BigDecimal("1.0880"), new BigDecimal("1.0850"),
                now, now.plusSeconds(30)));
        CreateOrderRequest req = buildRequestFull("q_ok",
                /* limitPrice = */ new BigDecimal("100"),
                /* cashHoldAmount = */ new BigDecimal("924.214"));

        OrderIngressService.IngressResult res = service.persistAccepted(req);

        assertThat(res.created()).isTrue();
        verify(fxSvc, times(1)).recall("q_ok");
    }

    @Test
    void flagOn_recallHit_integrityCheckFails_outsideTolerance() {
        // qty=10 px=100 / bid=1.0820 = 924.21441… expected.
        // Provide 100 EUR (BFF bug or tampering) → drift ≈ 89% → reject.
        config.getFx().setAcceptUseQuoterEnabled(true);
        Instant now = Instant.now();
        when(fxSvc.recall("q_tamper")).thenReturn(new FxQuoteService.CachedQuote(
                "q_tamper", "EURUSD", "basic",
                new BigDecimal("1.0820"), new BigDecimal("1.0880"), new BigDecimal("1.0850"),
                now, now.plusSeconds(30)));
        CreateOrderRequest req = buildRequestFull("q_tamper",
                new BigDecimal("100"),
                /* cashHoldAmount = */ new BigDecimal("100.00"));

        assertThatThrownBy(() -> service.persistAccepted(req))
                .isInstanceOfSatisfying(FxQuoteLockException.class, ex -> {
                    assertThat(ex.getHttpStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
                    assertThat(ex.getRejectCode()).isEqualTo(RejectCode.RISK_FX_QUOTE_EXPIRED);
                    assertThat(ex.getErrorCode()).isEqualTo("fx_cash_amount_mismatch");
                    assertThat(ex.getMessage()).contains("drifts");
                });
    }

    @Test
    void flagOn_integrityCheck_usesAskForSell() {
        // SELL with cashHoldAmount triggers the integrity check using the ASK
        // side (= what customer would actually receive in EUR proceeds at
        // settlement). qty=10 px=100 / ask=1.0880 = 919.117647 EUR expected;
        // provide 100 → drift ≈ 89% → reject with fx_cash_amount_mismatch.
        // (In practice SELL trades won't carry cashHoldAmount — share
        // reservation, not funds hold — but the math + reject still apply
        // if a BFF mis-wires the side.)
        config.getFx().setAcceptUseQuoterEnabled(true);
        Instant now = Instant.now();
        when(fxSvc.recall("q_sell")).thenReturn(new FxQuoteService.CachedQuote(
                "q_sell", "EURUSD", "basic",
                new BigDecimal("1.0820"), new BigDecimal("1.0880"), new BigDecimal("1.0850"),
                now, now.plusSeconds(30)));
        CreateOrderRequest req = new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000111"),
                "idem-sell-tampered", Side.SELL, "AAPL", new BigDecimal("10"),
                new BigDecimal("100"), "DAY", "LIMIT", null, null,
                "q_sell", new BigDecimal("100.00"));

        assertThatThrownBy(() -> service.persistAccepted(req))
                .isInstanceOfSatisfying(FxQuoteLockException.class, ex -> {
                    assertThat(ex.getErrorCode()).isEqualTo("fx_cash_amount_mismatch");
                    assertThat(ex.getMessage()).contains("919.117");
                });
    }

    @Test
    void flagOn_butFxBeanMissing_throwsFxQuoteLockExceptionAs500() {
        // Operator mis-config: flag on but no FxQuoteService bean.
        // We rebuild service with a provider that returns null, so the
        // recall path can't run even though the request asks for a lock.
        config.getFx().setAcceptUseQuoterEnabled(true);

        @SuppressWarnings("unchecked")
        ObjectProvider<FxQuoteService> noFx = (ObjectProvider<FxQuoteService>) mock(ObjectProvider.class);
        when(noFx.getIfAvailable()).thenReturn(null);
        @SuppressWarnings("unchecked")
        ObjectProvider<FxCustomerFlowNettingService> nettingProvider =
                (ObjectProvider<FxCustomerFlowNettingService>) mock(ObjectProvider.class);
        when(nettingProvider.getIfAvailable()).thenReturn(null);

        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerInflightReservationClient> ledgerInflight =
                (ObjectProvider<LedgerInflightReservationClient>) mock(ObjectProvider.class);
        when(ledgerInflight.getIfAvailable()).thenReturn(null);
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerInflightCoalescer> coal =
                (ObjectProvider<LedgerInflightCoalescer>) mock(ObjectProvider.class);
        when(coal.getIfAvailable()).thenReturn(null);
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> bal =
                (ObjectProvider<LedgerBalanceClient>) mock(ObjectProvider.class);
        when(bal.getIfAvailable()).thenReturn(null);

        OmsClusterShardRouter router = new OmsClusterShardRouter(1, Map.of(0, cluster));
        OrderIngressService noFxService = new OrderIngressService(
                orders, config, piiHash, ledgerInflight, coal, bal, noFx, nettingProvider,
                new SimpleMeterRegistry(), orderControlAdmission, router, venueAdmissionGate,
                predictionMarketTickGate);

        CreateOrderRequest req = buildRequest("q_test", new BigDecimal("100.00"));

        assertThatThrownBy(() -> noFxService.persistAccepted(req))
                .isInstanceOfSatisfying(FxQuoteLockException.class, ex -> {
                    assertThat(ex.getHttpStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
                    assertThat(ex.getRejectCode()).isEqualTo(RejectCode.RISK_FX_QUOTE_EXPIRED);
                    assertThat(ex.getErrorCode()).isEqualTo("fx_quoter_not_wired");
                });
    }

    @Test
    void cashHoldAmountOnly_failsBeanValidation_butAcceptShapeChecked() {
        // This test documents the contract via record validation only — full
        // jakarta validation runs in the controller; here we just confirm the
        // shape predicate on the record itself catches the "amount without
        // quoteId" shape.
        CreateOrderRequest bad = new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000111"),
                "idem-x", Side.BUY, "AAPL", new BigDecimal("1"),
                new BigDecimal("100"), "DAY", "LIMIT", null, null,
                /* fxQuoteId = */ null, /* cashHoldAmount = */ new BigDecimal("90"));
        assertThat(bad.isFxQuoteLockShapeValid()).isFalse();

        CreateOrderRequest good = new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000111"),
                "idem-y", Side.BUY, "AAPL", new BigDecimal("1"),
                new BigDecimal("100"), "DAY", "LIMIT", null, null,
                /* fxQuoteId = */ "q_x", /* cashHoldAmount = */ new BigDecimal("90"));
        assertThat(good.isFxQuoteLockShapeValid()).isTrue();
    }

    private static CreateOrderRequest buildRequest(String fxQuoteId, BigDecimal cashHoldAmount) {
        return new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000111"),
                "idem-" + (fxQuoteId == null ? "single" : fxQuoteId),
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                /* limitPrice = */ null,
                "DAY",
                /* orderType = */ null,
                /* ledgerBalanceId = */ null,
                /* ledgerIdentityId = */ null,
                fxQuoteId,
                cashHoldAmount);
    }

    /**
     * Variant with limitPrice populated so the §8.4 integrity check has the
     * notional (qty × limitPrice) it needs to recompute expected_cash off the
     * recalled rate.
     */
    private static CreateOrderRequest buildRequestFull(
            String fxQuoteId, BigDecimal limitPrice, BigDecimal cashHoldAmount) {
        return new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000111"),
                "idem-" + (fxQuoteId == null ? "single" : fxQuoteId),
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                limitPrice,
                "DAY",
                /* orderType = */ "LIMIT",
                /* ledgerBalanceId = */ null,
                /* ledgerIdentityId = */ null,
                fxQuoteId,
                cashHoldAmount);
    }
}
