package com.balh.oms.venuereconcile;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.persistence.OrdersRepository.VenueReconcileCandidate;
import com.balh.oms.venue.VenueRouteOrderClient;
import com.balh.oms.venue.VenueRouteTransportException;
import com.balh.venue.grpc.v1.OrderLiveness;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the golden-copy venue order reconciler. Verifies it terminates only orders the
 * venue authoritatively reports NOT_LIVE, never touches LIVE / inconclusive / non-venue orders, and
 * routes the termination through the {@code EXEC_TYPE_VENUE_REJECT} cluster command.
 */
class VenueOrderReconcilerTest {

    private static final String VENUE_SYMBOL = "PREDMKT-WC26_CURACAO_BEAT_GERMANY_JUN14";

    private OmsConfig config;
    private OrdersRepository ordersRepository;
    private VenueRouteOrderClient venueClient;
    private OmsClusterIngressClient clusterIngressClient;
    private VenueOrderReconciler reconciler;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getRouting().setVenueSymbolPrefixRoutingEnabled(true);
        config.getCluster().getVenueReconciler().setEnabled(true);
        ordersRepository = mock(OrdersRepository.class);
        venueClient = mock(VenueRouteOrderClient.class);
        clusterIngressClient = mock(OmsClusterIngressClient.class);
        reconciler = new VenueOrderReconciler(
                config,
                ordersRepository,
                venueClient,
                clusterIngressClient,
                Clock.fixed(Instant.parse("2026-06-04T18:00:00Z"), ZoneOffset.UTC),
                new SimpleMeterRegistry());
    }

    @Test
    void terminatesOrderTheVenueReportsNotLive() throws Exception {
        UUID orphan = UUID.randomUUID();
        when(ordersRepository.findWorkingVenueReconcileCandidates(any(Instant.class), anyInt()))
                .thenReturn(List.of(new VenueReconcileCandidate(orphan, VENUE_SYMBOL, 4)));
        when(venueClient.queryOrderStatus(orphan, VENUE_SYMBOL))
                .thenReturn(OrderLiveness.ORDER_LIVENESS_NOT_LIVE);

        int terminated = reconciler.reconcileOnce();

        assertThat(terminated).isEqualTo(1);
        ArgumentCaptor<ApplyExecutionReportCommand> cmd = ArgumentCaptor.forClass(ApplyExecutionReportCommand.class);
        verify(clusterIngressClient).submitApplyExecutionReport(cmd.capture(), any(Duration.class));
        assertThat(cmd.getValue().orderId()).isEqualTo(orphan);
        assertThat(cmd.getValue().execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT);
        assertThat(cmd.getValue().venueExecRef()).isEqualTo("venue-reconcile-" + orphan + "-v4");
        assertThat(cmd.getValue().rawEnvelopeJson()).contains(VenueOrderReconciler.REASON_NOT_LIVE);
    }

    @Test
    void leavesLiveOrderUntouched() throws Exception {
        UUID live = UUID.randomUUID();
        when(ordersRepository.findWorkingVenueReconcileCandidates(any(Instant.class), anyInt()))
                .thenReturn(List.of(new VenueReconcileCandidate(live, VENUE_SYMBOL, 1)));
        when(venueClient.queryOrderStatus(live, VENUE_SYMBOL)).thenReturn(OrderLiveness.ORDER_LIVENESS_LIVE);

        assertThat(reconciler.reconcileOnce()).isZero();
        verify(clusterIngressClient, never()).submitApplyExecutionReport(any(), any());
    }

    @Test
    void skipsOnInconclusiveTransportFailure() throws Exception {
        UUID order = UUID.randomUUID();
        when(ordersRepository.findWorkingVenueReconcileCandidates(any(Instant.class), anyInt()))
                .thenReturn(List.of(new VenueReconcileCandidate(order, VENUE_SYMBOL, 2)));
        when(venueClient.queryOrderStatus(order, VENUE_SYMBOL))
                .thenThrow(new VenueRouteTransportException("boom", null));

        assertThat(reconciler.reconcileOnce()).isZero();
        verify(clusterIngressClient, never()).submitApplyExecutionReport(any(), any());
    }

    @Test
    void skipsUnspecifiedLiveness() throws Exception {
        UUID order = UUID.randomUUID();
        when(ordersRepository.findWorkingVenueReconcileCandidates(any(Instant.class), anyInt()))
                .thenReturn(List.of(new VenueReconcileCandidate(order, VENUE_SYMBOL, 2)));
        when(venueClient.queryOrderStatus(order, VENUE_SYMBOL))
                .thenReturn(OrderLiveness.ORDER_LIVENESS_UNSPECIFIED);

        assertThat(reconciler.reconcileOnce()).isZero();
        verify(clusterIngressClient, never()).submitApplyExecutionReport(any(), any());
    }

    @Test
    void ignoresNonVenueRoutedOrders() throws Exception {
        UUID equity = UUID.randomUUID();
        when(ordersRepository.findWorkingVenueReconcileCandidates(any(Instant.class), anyInt()))
                .thenReturn(List.of(new VenueReconcileCandidate(equity, "AAPL", 1)));

        assertThat(reconciler.reconcileOnce()).isZero();
        verifyNoInteractions(venueClient);
        verify(clusterIngressClient, never()).submitApplyExecutionReport(any(), any());
    }

    @Test
    void queriesUsingTheConfiguredAgeCutoff() throws Exception {
        config.getCluster().getVenueReconciler().setMinOrderAgeMs(120_000L);
        when(ordersRepository.findWorkingVenueReconcileCandidates(any(Instant.class), anyInt()))
                .thenReturn(List.of());

        reconciler.reconcileOnce();

        ArgumentCaptor<Instant> cutoff = ArgumentCaptor.forClass(Instant.class);
        verify(ordersRepository).findWorkingVenueReconcileCandidates(cutoff.capture(), eq(200));
        assertThat(cutoff.getValue()).isEqualTo(Instant.parse("2026-06-04T18:00:00Z").minusMillis(120_000L));
    }
}
