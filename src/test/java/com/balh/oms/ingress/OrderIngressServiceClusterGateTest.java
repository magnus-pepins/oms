package com.balh.oms.ingress;

import com.balh.oms.chronicle.ControlChroniclePayloadCodec;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OrderAcceptedEvent;
import com.balh.oms.cluster.OrderRejectedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.balh.oms.observability.PiiHash;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.persistence.ControlOutboxRepository;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.tailer.OrderControlAdmission;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase 1b unit test: verifies {@link OrderIngressService} routes admission
 * through {@link OmsClusterIngressClient} when the cluster bean is present,
 * and surfaces cluster failures as the right
 * {@link ClusterAdmissionException} HTTP status.
 *
 * <p>Plan: {@code system-documentation/plans/oms-aeron-cluster-substrate.md}
 * §Phase 1. The full HTTP + Postgres + cluster integration test rides on
 * Postgres testcontainers and lives in
 * {@link OrderIngressLedgerInflightIntegrationTest}-style setups in CI; this
 * test exercises the cluster-gate logic in process so it runs locally without
 * Docker and gives fast feedback on the gate semantics.
 */
class OrderIngressServiceClusterGateTest {

    private OrdersRepository orders;
    private ControlOutboxRepository controlOutbox;
    private DomainEventOutboxRepository domainEventOutbox;
    private DomainEventEnvelopeCodec domainEventEnvelopeCodec;
    private ControlChroniclePayloadCodec controlPayloadCodec;
    private ObjectMapper objectMapper;
    private PiiHash piiHash;
    private LedgerInflightOutboxRepository ledgerInflightOutbox;
    private IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder;
    private OrderControlAdmission orderControlAdmission;
    private OmsClusterIngressClient cluster;

    private OmsConfig config;
    private OrderIngressService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        orders = mock(OrdersRepository.class);
        controlOutbox = mock(ControlOutboxRepository.class);
        domainEventOutbox = mock(DomainEventOutboxRepository.class);
        domainEventEnvelopeCodec = mock(DomainEventEnvelopeCodec.class);
        controlPayloadCodec = mock(ControlChroniclePayloadCodec.class);
        objectMapper = mock(ObjectMapper.class);
        piiHash = mock(PiiHash.class);
        ledgerInflightOutbox = mock(LedgerInflightOutboxRepository.class);
        ingressToFixNosLatencyRecorder = mock(IngressToFixNosLatencyRecorder.class);
        orderControlAdmission = mock(OrderControlAdmission.class);
        cluster = mock(OmsClusterIngressClient.class);

        config = new OmsConfig();
        config.getShard().setCount(1);
        config.getCluster().getClient().setEnabled(true);
        config.getCluster().getClient().setSubmitTimeoutMs(2_000L);

        when(piiHash.hash(any())).thenReturn("hash");
        when(controlPayloadCodec.outboxPayloadJson(any())).thenReturn("{}");
        when(controlOutbox.insert(any(), any(Integer.class), any(), any()))
                .thenReturn(new ControlOutboxRepository.InsertResult(1L, java.time.Instant.now()));

        ObjectProvider<LedgerInflightReservationClient> ledgerInflight =
                (ObjectProvider<LedgerInflightReservationClient>) mock(ObjectProvider.class);
        when(ledgerInflight.getIfAvailable()).thenReturn(null);
        ObjectProvider<LedgerBalanceClient> ledgerBalance =
                (ObjectProvider<LedgerBalanceClient>) mock(ObjectProvider.class);
        when(ledgerBalance.getIfAvailable()).thenReturn(null);
        ObjectProvider<IngressControlChroniclePublisher> chroniclePublisher =
                (ObjectProvider<IngressControlChroniclePublisher>) mock(ObjectProvider.class);
        when(chroniclePublisher.getIfAvailable()).thenReturn(null);
        ObjectProvider<OmsClusterIngressClient> clusterProvider =
                (ObjectProvider<OmsClusterIngressClient>) mock(ObjectProvider.class);
        when(clusterProvider.getIfAvailable()).thenReturn(cluster);

        service = new OrderIngressService(
                orders,
                controlOutbox,
                domainEventOutbox,
                domainEventEnvelopeCodec,
                config,
                controlPayloadCodec,
                objectMapper,
                piiHash,
                ledgerInflight,
                ledgerBalance,
                ledgerInflightOutbox,
                new SimpleMeterRegistry(),
                ingressToFixNosLatencyRecorder,
                chroniclePublisher,
                orderControlAdmission,
                clusterProvider);
    }

    @Test
    void freshAccept_callsClusterAndProceedsToInsert() throws Exception {
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenAnswer(invocation -> {
                    AcceptOrderCommand cmd = invocation.getArgument(0);
                    return new AdmissionResult.Accepted(
                            new OrderAcceptedEvent(
                                    cmd.correlationId(),
                                    cmd.orderId(),
                                    /* version = */ 0,
                                    /* duplicate = */ false,
                                    /* acceptedAtNanos = */ 1L));
                });

        OrderIngressService.IngressResult result = service.persistAccepted(buyRequest());

        assertThat(result.created()).isTrue();
        ArgumentCaptor<Order> insertedOrder = ArgumentCaptor.forClass(Order.class);
        verify(orders).insert(insertedOrder.capture());
        verify(cluster).submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class));
        assertThat(insertedOrder.getValue().id()).isEqualTo(result.order().id());
    }

    @Test
    void duplicateAcceptWithDifferentClusterOrderId_rebuildsOrderWithClusterId() throws Exception {
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
                                    /* acceptedAtNanos = */ 1L));
                });

        service.persistAccepted(buyRequest());

        ArgumentCaptor<Order> insertedOrder = ArgumentCaptor.forClass(Order.class);
        verify(orders).insert(insertedOrder.capture());
        assertThat(insertedOrder.getValue().id())
                .as("when cluster says duplicate=true, ingress must use the cluster-returned orderId, not its tentative UUID")
                .isEqualTo(clusterOriginalOrderId);
    }

    @Test
    void clusterRejected_throws422AndDoesNotInsertPostgresRow() throws Exception {
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
    void clusterTimeout_throws503AndDoesNotInsertPostgresRow() throws Exception {
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
    void clusterNotConnected_throws503AndDoesNotInsertPostgresRow() throws Exception {
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenThrow(new IllegalStateException("not connected"));

        assertThatThrownBy(() -> service.persistAccepted(buyRequest()))
                .isInstanceOfSatisfying(ClusterAdmissionException.class, e -> {
                    assertThat(e.getHttpStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(e.getErrorCode()).isEqualTo("cluster_unavailable");
                });
        verify(orders, never()).insert(any());
    }

    @Test
    void clusterAbsent_doesNotConsultCluster() throws Exception {
        @SuppressWarnings("unchecked")
        ObjectProvider<OmsClusterIngressClient> emptyProvider =
                (ObjectProvider<OmsClusterIngressClient>) mock(ObjectProvider.class);
        when(emptyProvider.getIfAvailable()).thenReturn(null);
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerInflightReservationClient> ledgerInflight =
                (ObjectProvider<LedgerInflightReservationClient>) mock(ObjectProvider.class);
        when(ledgerInflight.getIfAvailable()).thenReturn(null);
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> ledgerBalance =
                (ObjectProvider<LedgerBalanceClient>) mock(ObjectProvider.class);
        when(ledgerBalance.getIfAvailable()).thenReturn(null);
        @SuppressWarnings("unchecked")
        ObjectProvider<IngressControlChroniclePublisher> chroniclePublisher =
                (ObjectProvider<IngressControlChroniclePublisher>) mock(ObjectProvider.class);
        when(chroniclePublisher.getIfAvailable()).thenReturn(null);

        OrderIngressService noCluster = new OrderIngressService(
                orders,
                controlOutbox,
                domainEventOutbox,
                domainEventEnvelopeCodec,
                config,
                controlPayloadCodec,
                objectMapper,
                piiHash,
                ledgerInflight,
                ledgerBalance,
                ledgerInflightOutbox,
                new SimpleMeterRegistry(),
                ingressToFixNosLatencyRecorder,
                chroniclePublisher,
                orderControlAdmission,
                emptyProvider);

        when(orders.findByIdempotency(any(), any())).thenReturn(Optional.empty());

        noCluster.persistAccepted(buyRequest());

        verify(cluster, never()).submitAcceptOrder(any(), any());
        verify(orders).insert(any());
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
