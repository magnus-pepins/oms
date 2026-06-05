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
import com.balh.oms.ledger.LedgerInflightLifecycleClient;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.balh.oms.observability.PiiHash;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Buying-power hold must run before cluster admit; insufficient funds reject at ingress and
 * never reach the venue egress path.
 */
class OrderIngressServiceLedgerHoldTest {

    private OmsClusterIngressClient cluster;
    private LedgerInflightReservationClient ledgerClient;
    private LedgerInflightLifecycleClient lifecycleClient;
    private LedgerInflightOutboxRepository outboxRepository;
    private LedgerBalanceClient ledgerBalanceClient;
    private OrderIngressService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        OrdersRepository orders = mock(OrdersRepository.class);
        PiiHash piiHash = mock(PiiHash.class);
        OrderControlAdmission orderControlAdmission = mock(OrderControlAdmission.class);
        cluster = mock(OmsClusterIngressClient.class);
        ledgerClient = mock(LedgerInflightReservationClient.class);
        lifecycleClient = mock(LedgerInflightLifecycleClient.class);
        outboxRepository = mock(LedgerInflightOutboxRepository.class);
        ledgerBalanceClient = mock(LedgerBalanceClient.class);

        OmsConfig config = new OmsConfig();
        config.getShard().setCount(1);
        config.getCluster().getClient().setEnabled(true);
        config.getCluster().getClient().setSubmitTimeoutMs(2_000L);
        config.getLedger().setEnabled(true);
        config.getLedger().setInflightReservationEnabled(true);
        config.getLedger().setInflightAsyncEnabled(true);

        when(piiHash.hash(any())).thenReturn("hash");
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenAnswer(inv -> {
                    AcceptOrderCommand cmd = inv.getArgument(0);
                    return new AdmissionResult.Accepted(
                            new OrderAcceptedEvent(
                                    cmd.correlationId(),
                                    cmd.orderId(),
                                    0,
                                    false,
                                    1L));
                });

        ObjectProvider<LedgerInflightReservationClient> ledgerInflight =
                (ObjectProvider<LedgerInflightReservationClient>) mock(ObjectProvider.class);
        when(ledgerInflight.getIfAvailable()).thenReturn(ledgerClient);
        ObjectProvider<LedgerInflightCoalescer> ledgerInflightCoalescer =
                (ObjectProvider<LedgerInflightCoalescer>) mock(ObjectProvider.class);
        when(ledgerInflightCoalescer.getIfAvailable()).thenReturn(null);
        ObjectProvider<LedgerInflightOutboxRepository> ledgerInflightOutbox =
                (ObjectProvider<LedgerInflightOutboxRepository>) mock(ObjectProvider.class);
        when(ledgerInflightOutbox.getIfAvailable()).thenReturn(outboxRepository);
        ObjectProvider<LedgerInflightLifecycleClient> ledgerInflightLifecycle =
                (ObjectProvider<LedgerInflightLifecycleClient>) mock(ObjectProvider.class);
        when(ledgerInflightLifecycle.getIfAvailable()).thenReturn(lifecycleClient);
        ObjectProvider<LedgerBalanceClient> ledgerBalance =
                (ObjectProvider<LedgerBalanceClient>) mock(ObjectProvider.class);
        when(ledgerBalance.getIfAvailable()).thenReturn(ledgerBalanceClient);
        when(ledgerBalanceClient.fetchIdentityIdForBalance("balance-1")).thenReturn("identity-1");
        ObjectProvider<FxQuoteService> fxProvider =
                (ObjectProvider<FxQuoteService>) mock(ObjectProvider.class);
        when(fxProvider.getIfAvailable()).thenReturn(null);
        ObjectProvider<FxCustomerFlowNettingService> nettingProvider =
                (ObjectProvider<FxCustomerFlowNettingService>) mock(ObjectProvider.class);
        when(nettingProvider.getIfAvailable()).thenReturn(null);

        OmsClusterShardRouter router = new OmsClusterShardRouter(1, Map.of(0, cluster));
        VenueAdmissionGate venueAdmissionGate = new VenueAdmissionGate(
                config, mock(OmsVenueEgressLagPublisher.class), new SimpleMeterRegistry());
        PredictionMarketTickGate predictionMarketTickGate = new PredictionMarketTickGate(
                config,
                mock(com.balh.oms.predictionmarket.PredictionMarketContractRepository.class),
                new SimpleMeterRegistry());

        service = new OrderIngressService(
                orders, config, piiHash,
                ledgerInflight, ledgerInflightCoalescer, ledgerInflightOutbox, ledgerInflightLifecycle,
                ledgerBalance, fxProvider, nettingProvider,
                new SimpleMeterRegistry(), orderControlAdmission, router, venueAdmissionGate,
                predictionMarketTickGate);
    }

    @Test
    void insufficientFunds_rejectsBeforeClusterAdmit() throws Exception {
        doThrow(new LedgerInflightReservationClient.LedgerReservationException(
                "ledger inflight hold failed: HTTP 422 {\"code\":\"INSUFFICIENT_FUNDS\"}"))
                .when(ledgerClient)
                .placeBuyFundsHold(any(UUID.class), any(), any(BigDecimal.class));

        CreateOrderRequest req = new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000111"),
                "idem-insufficient",
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                new BigDecimal("100"),
                "DAY",
                "LIMIT",
                "balance-1",
                "identity-1",
                null,
                null);

        assertThatThrownBy(() -> service.persistAccepted(req))
                .isInstanceOfSatisfying(LedgerInflightHoldException.class, ex -> {
                    assertThat(ex.getHttpStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
                    assertThat(ex.getRejectCode()).isEqualTo(RejectCode.RISK_BUYING_POWER);
                    assertThat(ex.getErrorCode()).isEqualTo("insufficient_funds");
                });

        verify(ledgerClient).placeBuyFundsHold(any(UUID.class), any(), any(BigDecimal.class));
        verify(cluster, never()).submitAcceptOrder(any(), any());
    }

    @Test
    void duplicateAfterPreAdmitHold_voidsHoldAndSkipsLifecycleRow() throws Exception {
        UUID originalOrderId = UUID.fromString("00000000-0000-4000-8000-00000000aaaa");
        when(ledgerClient.placeBuyFundsHold(any(UUID.class), any(), any(BigDecimal.class)))
                .thenReturn("txn_dup_1");
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenAnswer(inv -> {
                    AcceptOrderCommand cmd = inv.getArgument(0);
                    return new AdmissionResult.Accepted(
                            new OrderAcceptedEvent(
                                    cmd.correlationId(),
                                    originalOrderId,
                                    0,
                                    true,
                                    1L));
                });

        CreateOrderRequest req = new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000222"),
                "idem-dup-hold",
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                new BigDecimal("100"),
                "DAY",
                "LIMIT",
                "balance-1",
                "identity-1",
                null,
                null);

        OrderIngressService.IngressResult result = service.persistAccepted(req);

        assertThat(result.created()).isFalse();
        assertThat(result.order().id()).isEqualTo(originalOrderId);
        verify(lifecycleClient, times(1)).voidHold("txn_dup_1");
        verify(outboxRepository, never()).upsertPublishedWithTxnId(any(), anyString(), anyString(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void asyncCoalescerPath_doesNotWaitForAckFuture() throws Exception {
        OmsConfig cfg = new OmsConfig();
        cfg.getShard().setCount(1);
        cfg.getCluster().getClient().setEnabled(true);
        cfg.getCluster().getClient().setSubmitTimeoutMs(2_000L);
        cfg.getLedger().setEnabled(true);
        cfg.getLedger().setInflightReservationEnabled(true);
        cfg.getLedger().setInflightAsyncEnabled(true);
        cfg.getLedger().setInflightCoalescerEnabled(true);

        OrdersRepository orders = mock(OrdersRepository.class);
        PiiHash pii = mock(PiiHash.class);
        when(pii.hash(any())).thenReturn("hash");
        OrderControlAdmission control = mock(OrderControlAdmission.class);
        OmsClusterIngressClient localCluster = mock(OmsClusterIngressClient.class);
        when(localCluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenAnswer(inv -> {
                    AcceptOrderCommand cmd = inv.getArgument(0);
                    return new AdmissionResult.Accepted(
                            new OrderAcceptedEvent(cmd.correlationId(), cmd.orderId(), 0, false, 1L));
                });
        LedgerInflightCoalescer coalescer = mock(LedgerInflightCoalescer.class);
        when(coalescer.submit(any(UUID.class), anyString(), any(BigDecimal.class)))
                .thenReturn(new CompletableFuture<>());

        ObjectProvider<LedgerInflightReservationClient> inflight =
                (ObjectProvider<LedgerInflightReservationClient>) mock(ObjectProvider.class);
        when(inflight.getIfAvailable()).thenReturn(ledgerClient);
        ObjectProvider<LedgerInflightCoalescer> coalescerProvider =
                (ObjectProvider<LedgerInflightCoalescer>) mock(ObjectProvider.class);
        when(coalescerProvider.getIfAvailable()).thenReturn(coalescer);
        ObjectProvider<LedgerInflightOutboxRepository> outboxProvider =
                (ObjectProvider<LedgerInflightOutboxRepository>) mock(ObjectProvider.class);
        when(outboxProvider.getIfAvailable()).thenReturn(outboxRepository);
        ObjectProvider<LedgerInflightLifecycleClient> lifecycleProvider =
                (ObjectProvider<LedgerInflightLifecycleClient>) mock(ObjectProvider.class);
        when(lifecycleProvider.getIfAvailable()).thenReturn(lifecycleClient);
        ObjectProvider<LedgerBalanceClient> balanceProvider =
                (ObjectProvider<LedgerBalanceClient>) mock(ObjectProvider.class);
        when(balanceProvider.getIfAvailable()).thenReturn(ledgerBalanceClient);
        when(ledgerBalanceClient.fetchIdentityIdForBalance("balance-1")).thenReturn("identity-1");
        ObjectProvider<FxQuoteService> fxProvider =
                (ObjectProvider<FxQuoteService>) mock(ObjectProvider.class);
        when(fxProvider.getIfAvailable()).thenReturn(null);
        ObjectProvider<FxCustomerFlowNettingService> nettingProvider =
                (ObjectProvider<FxCustomerFlowNettingService>) mock(ObjectProvider.class);
        when(nettingProvider.getIfAvailable()).thenReturn(null);

        OmsClusterShardRouter router = new OmsClusterShardRouter(1, Map.of(0, localCluster));
        VenueAdmissionGate venueAdmissionGate = new VenueAdmissionGate(
                cfg, mock(OmsVenueEgressLagPublisher.class), new SimpleMeterRegistry());
        PredictionMarketTickGate predictionMarketTickGate = new PredictionMarketTickGate(
                cfg,
                mock(com.balh.oms.predictionmarket.PredictionMarketContractRepository.class),
                new SimpleMeterRegistry());
        OrderIngressService localService = new OrderIngressService(
                orders, cfg, pii,
                inflight, coalescerProvider, outboxProvider, lifecycleProvider,
                balanceProvider, fxProvider, nettingProvider,
                new SimpleMeterRegistry(), control, router, venueAdmissionGate, predictionMarketTickGate);

        CreateOrderRequest req = new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000333"),
                "idem-coalescer-async",
                Side.BUY,
                "AAPL",
                new BigDecimal("1"),
                new BigDecimal("100"),
                "DAY",
                "LIMIT",
                "balance-1",
                "identity-1",
                null,
                null);

        OrderIngressService.IngressResult result = localService.persistAccepted(req);

        assertThat(result.created()).isTrue();
        verify(coalescer, times(1)).submit(any(UUID.class), anyString(), any(BigDecimal.class));
        verify(localCluster, times(1)).submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class));
    }
}
