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
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Phase 1c slice B / Phase 2 slice 2c / Phase 3 slice 3f / Phase 4 Tier 2.5 phase D-3 unit
 * test: verifies {@link OrderIngressService} routes admission through the (now mandatory)
 * {@link OmsClusterIngressClient}, surfaces cluster failures as the right
 * {@link ClusterAdmissionException} HTTP status, does NOT write an {@code orders} row from
 * the ingress (slice 2c — projector owns it), does NOT write a {@code control_outbox} row
 * (slice 3f — table is gone), and (slice D-3) does NOT write a {@code domain_event_outbox}
 * row either — the projector now emits {@code OrderAccepted} from the cluster's
 * {@link com.balh.oms.cluster.OrderAdmittedEvent}, gated on a fresh {@code orders} insert.
 *
 * <p>Plan: {@code system-documentation/plans/oms-aeron-cluster-substrate.md}.
 */
class OrderIngressServiceClusterGateTest {

    private OrdersRepository orders;
    private ObjectMapper objectMapper;
    private PiiHash piiHash;
    private LedgerInflightOutboxRepository ledgerInflightOutbox;
    private OrderControlAdmission orderControlAdmission;
    private OmsClusterIngressClient cluster;

    private OmsConfig config;
    private OrderIngressService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        orders = mock(OrdersRepository.class);
        objectMapper = mock(ObjectMapper.class);
        piiHash = mock(PiiHash.class);
        ledgerInflightOutbox = mock(LedgerInflightOutboxRepository.class);
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

        // Phase 4 Tier 2.5 phase D-3: OrderIngressService no longer takes a
        // PlatformTransactionManager — the only Postgres write that may still happen on
        // the ingress hot path is the optional BUY-async ledger_inflight_outbox INSERT,
        // which auto-commits via Hikari's default autoCommit=true. The DomainEventOutbox
        // and DomainEventEnvelopeCodec dependencies are gone for the same reason.
        service = new OrderIngressService(
                orders,
                config,
                objectMapper,
                piiHash,
                ledgerInflight,
                ledgerInflightCoalescer,
                ledgerBalance,
                ledgerInflightOutbox,
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
        // D-3: ingress no longer writes the OrderAccepted envelope here. The buyRequest()
        // also has limitPrice=null and ledgerBalanceId=null, so maybePlaceBuyLedgerInflightHold
        // is a no-op and no Postgres conn is acquired at all on this path.
        verify(ledgerInflightOutbox, never()).insert(any(), any());
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
        verify(ledgerInflightOutbox, never()).insert(any(), any());
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
        verify(ledgerInflightOutbox, never()).insert(any(), any());
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
        verify(ledgerInflightOutbox, never()).insert(any(), any());
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
        verify(ledgerInflightOutbox, never()).insert(any(), any());
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
