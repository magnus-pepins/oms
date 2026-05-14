package com.balh.oms.ingress;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OrderAcceptedEvent;
import com.balh.oms.cluster.OrderRejectedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
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
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 1c slice B / Phase 2 slice 2c / Phase 3 slice 3f / Phase 4 Tier 2.5 phase D-3 / D-9
 * unit test: verifies {@link OrderIngressService} routes admission through the (now mandatory)
 * {@link OmsClusterIngressClient}, surfaces cluster failures as the right
 * {@link ClusterAdmissionException} HTTP status, does NOT write an {@code orders} row from
 * the ingress (slice 2c — projector owns it), does NOT write a {@code control_outbox} row
 * (slice 3f — table is gone), does NOT write a {@code domain_event_outbox} row (slice D-3 —
 * projector emits {@code OrderAccepted} from the cluster log), and (slice D-9) does NOT write
 * a {@code ledger_inflight_outbox} row from the ingress either — the projector promotes the
 * D-1 idempotent backfill to the only writer of that table for the BUY-async path. Net: zero
 * Postgres I/O on the ingress hot path on every branch through this method.
 *
 * <p>Plan: {@code system-documentation/plans/oms-aeron-cluster-substrate.md}.
 */
class OrderIngressServiceClusterGateTest {

    private OrdersRepository orders;
    private PiiHash piiHash;
    private OrderControlAdmission orderControlAdmission;
    private OmsClusterIngressClient cluster;

    private OmsConfig config;
    private OrderIngressService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        orders = mock(OrdersRepository.class);
        piiHash = mock(PiiHash.class);
        orderControlAdmission = mock(OrderControlAdmission.class);
        cluster = mock(OmsClusterIngressClient.class);

        config = new OmsConfig();
        config.getShard().setCount(1);
        config.getCluster().getClient().setEnabled(true);
        config.getCluster().getClient().setSubmitTimeoutMs(2_000L);

        when(piiHash.hash(any())).thenReturn("hash");

        ObjectProvider<LedgerInflightReservationClient> ledgerInflight =
                (ObjectProvider<LedgerInflightReservationClient>) mock(ObjectProvider.class);
        when(ledgerInflight.getIfAvailable()).thenReturn(null);
        ObjectProvider<LedgerInflightCoalescer> ledgerInflightCoalescer =
                (ObjectProvider<LedgerInflightCoalescer>) mock(ObjectProvider.class);
        when(ledgerInflightCoalescer.getIfAvailable()).thenReturn(null);
        ObjectProvider<LedgerBalanceClient> ledgerBalance =
                (ObjectProvider<LedgerBalanceClient>) mock(ObjectProvider.class);
        when(ledgerBalance.getIfAvailable()).thenReturn(null);

        // Phase 4 Tier 2.5 phase D-9: OrderIngressService no longer depends on
        // LedgerInflightOutboxRepository or ObjectMapper — the BUY-async ledger_inflight_outbox
        // INSERT moved to the projector (which materialises the row from the cluster's
        // OrderAdmittedEvent via insertIfAbsent). The class now opens zero Postgres
        // connections on the hot path on every branch.
        service = new OrderIngressService(
                orders,
                config,
                piiHash,
                ledgerInflight,
                ledgerInflightCoalescer,
                ledgerBalance,
                new SimpleMeterRegistry(),
                orderControlAdmission,
                cluster);
    }

    @Test
    void freshAccept_callsCluster_doesNotInsertOrders_doesNotWriteDomainOutbox() throws Exception {
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    AcceptOrderCommand cmd = invocation.getArgument(0);
                    return new AdmissionResult.Accepted(
                            new OrderAcceptedEvent(
                                    cmd.correlationId(),
                                    cmd.orderId(),
                                    /* version = */ 0,
                                    /* duplicate = */ false,
                                    /* acceptedAtMillis = */ 1L));
                });

        OrderIngressService.IngressResult result = service.persistAccepted(buyRequest());

        assertThat(result.created())
                .as("cluster reported duplicate=false → IngressResult.created=true (HTTP 201)")
                .isTrue();
        verify(cluster).submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class));
        verify(orders, never()).insert(any(Order.class));
        // D-3 + D-9: ingress no longer writes either the OrderAccepted envelope or the
        // ledger_inflight_outbox row — both materialise on the projector from the cluster's
        // OrderAdmittedEvent. The class no longer holds either repository reference.
    }

    @Test
    void duplicateAcceptWithDifferentClusterOrderId_returnsClusterOrderId_skipsSideTables() throws Exception {
        UUID clusterOriginalOrderId = UUID.fromString("00000000-0000-4000-8000-00000000aaaa");

        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    AcceptOrderCommand cmd = invocation.getArgument(0);
                    return new AdmissionResult.Accepted(
                            new OrderAcceptedEvent(
                                    cmd.correlationId(),
                                    clusterOriginalOrderId,
                                    /* version = */ 0,
                                    /* duplicate = */ true,
                                    /* acceptedAtMillis = */ 1L));
                });

        OrderIngressService.IngressResult result = service.persistAccepted(buyRequest());

        assertThat(result.created())
                .as("cluster reported duplicate=true → IngressResult.created=false (HTTP 200)")
                .isFalse();
        assertThat(result.order().id())
                .as("response must echo the cluster's original orderId so callers see a single identity")
                .isEqualTo(clusterOriginalOrderId);
        verify(orders, never()).insert(any(Order.class));
    }

    @Test
    void clusterRejected_throws422_doesNotTouchPostgres() throws Exception {
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    AcceptOrderCommand cmd = invocation.getArgument(0);
                    return new AdmissionResult.Rejected(
                            new OrderRejectedEvent(
                                    cmd.correlationId(),
                                    cmd.orderId(),
                                    /* rejectCodeOrdinal = */ 0,
                                    /* rejectedAtNanos = */ 1L,
                                    "policy violated"));
                });

        assertThatThrownBy(() -> service.persistAccepted(buyRequest()))
                .isInstanceOfSatisfying(ClusterAdmissionException.class, e -> {
                    assertThat(e.getHttpStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
                    assertThat(e.getErrorCode()).isEqualTo("cluster_rejected");
                });
        verify(orders, never()).insert(any());
    }

    @Test
    void clusterTimeout_throws503_doesNotTouchPostgres() throws Exception {
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenThrow(new TimeoutException("cluster slow"));

        assertThatThrownBy(() -> service.persistAccepted(buyRequest()))
                .isInstanceOfSatisfying(ClusterAdmissionException.class, e -> {
                    assertThat(e.getHttpStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(e.getErrorCode()).isEqualTo("cluster_admission_timeout");
                });
        verify(orders, never()).insert(any());
    }

    @Test
    void clusterNotConnected_throws503_doesNotTouchPostgres() throws Exception {
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenThrow(new IllegalStateException("not connected"));

        assertThatThrownBy(() -> service.persistAccepted(buyRequest()))
                .isInstanceOfSatisfying(ClusterAdmissionException.class, e -> {
                    assertThat(e.getHttpStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(e.getErrorCode()).isEqualTo("cluster_unavailable");
                });
        verify(orders, never()).insert(any());
    }

    private static CreateOrderRequest buyRequest() {
        return new CreateOrderRequest(
                UUID.fromString("00000000-0000-4000-8000-000000000111"),
                "idem-1",
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                /* limitPrice = */ null,
                "DAY",
                /* ledgerBalanceId = */ null,
                /* ledgerIdentityId = */ null);
    }
}
