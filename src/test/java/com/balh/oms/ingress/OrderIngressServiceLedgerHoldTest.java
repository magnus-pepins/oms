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
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Buying-power hold must run before cluster admit; insufficient funds reject at ingress and
 * never reach the venue egress path.
 */
class OrderIngressServiceLedgerHoldTest {

    private OmsClusterIngressClient cluster;
    private LedgerInflightReservationClient ledgerClient;
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
                config,
                mock(com.balh.oms.projector.AeronProjectorCursorRepository.class),
                mock(com.balh.oms.venueegress.OmsVenueEgressCursorRepository.class),
                new SimpleMeterRegistry());
        PredictionMarketTickGate predictionMarketTickGate = new PredictionMarketTickGate(
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
}
