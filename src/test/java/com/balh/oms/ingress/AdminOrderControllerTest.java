package com.balh.oms.ingress;

import com.balh.oms.cluster.CancelOrderCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.persistence.OrdersRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link AdminOrderController#forceCancel}. Mocks {@link OrdersRepository}
 * + {@link OmsClusterIngressClient} so the tests run without an Aeron cluster or Postgres.
 *
 * <p>Why this controller previously had zero tests: the original slice shipped it as a
 * fire-and-forget {@code 202 Accepted}, leaving the operator with no way to tell whether
 * the cluster actually applied the cancel. The known-issues handover dated 2026-05-20
 * documents the gap; this revision adds a post-submit observation poll that returns
 * {@code 200} on observed apply or {@code 410 Gone} when the cluster swallowed the command
 * (typical cause: in-memory {@code orderIndex} miss after a journal wipe). These tests
 * lock in the new behaviour for all four endpoint outcomes.
 *
 * <p>The observation timeout is shortened via {@link OmsConfig.Admin#setCancelObservationTimeoutMs}
 * so the cluster-forgot test does not spend the production default (2000 ms) per run.
 */
@ExtendWith(MockitoExtension.class)
class AdminOrderControllerTest {

    private static final UUID ORDER_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final UUID ACCOUNT_ID = UUID.fromString("aaaaaaaa-bbbb-4ccc-9ddd-eeeeeeeeeeee");

    /**
     * Test-only observation timeout: long enough for several poll iterations to fire (so the
     * mocked {@code findById} sequence can flip from WORKING → CANCELLED), short enough that the
     * negative-path test (cluster-forgot) completes in well under a second.
     */
    private static final long TEST_OBSERVATION_TIMEOUT_MS = 200L;
    private static final long TEST_OBSERVATION_POLL_MS = 10L;

    @Mock private OmsClusterIngressClient cluster;
    @Mock private OrdersRepository orders;

    private OmsConfig config;
    private AdminOrderController controller;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getAdmin().setCancelObservationTimeoutMs(TEST_OBSERVATION_TIMEOUT_MS);
        config.getAdmin().setCancelObservationPollIntervalMs(TEST_OBSERVATION_POLL_MS);
        controller = new AdminOrderController(config, cluster, orders, new SimpleMeterRegistry());
    }

    @Test
    void forceCancel_unknownOrder_returns404_withoutClusterSubmit() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.empty());

        ResponseEntity<?> resp = controller.forceCancel(ORDER_ID, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body).containsEntry("error", "order_not_found");
        verifyNoInteractions(cluster);
    }

    @Test
    void forceCancel_alreadyTerminal_returns409_withoutClusterSubmit() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(orderWithStatus(OrderStatus.FILLED, 1)));

        ResponseEntity<?> resp = controller.forceCancel(ORDER_ID, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body)
                .containsEntry("error", "order_already_terminal")
                .containsEntry("orderStatus", "FILLED");
        verifyNoInteractions(cluster);
    }

    /**
     * Happy path: pre-check sees a WORKING order, submit succeeds, observation poll eventually
     * sees the projector flip status → CANCELLED with a higher version. Returns 200 with the
     * observed terminal status.
     *
     * <p>Mockito's {@code thenReturn(a, b, c)} stubs successive calls to the same method: the
     * first call (pre-check) returns the working order at v=1; a couple of poll iterations
     * still see v=1; then the simulated projector update lands and subsequent polls see
     * CANCELLED at v=2.
     */
    @Test
    void forceCancel_clusterAppliesAndProjectorObserves_returns200_appliedStatus() throws Exception {
        Order working = orderWithStatus(OrderStatus.WORKING, 1);
        Order cancelled = orderWithStatus(OrderStatus.CANCELLED, 2);
        when(orders.findById(ORDER_ID))
                .thenReturn(Optional.of(working))
                .thenReturn(Optional.of(working))
                .thenReturn(Optional.of(working))
                .thenReturn(Optional.of(cancelled));
        when(cluster.nextCorrelationId()).thenReturn(7L);

        ResponseEntity<?> resp = controller.forceCancel(
                ORDER_ID, new AdminOrderController.ForceCancelRequestBody("post-journal-wipe"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        AdminOrderController.AdminResponse body = (AdminOrderController.AdminResponse) resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo("force_cancel_applied");
        assertThat(body.orderStatus()).isEqualTo("CANCELLED");

        // The submitted command must carry the admin prefix + operator-supplied reason so cluster
        // log readers / audit can distinguish admin force-cancels from FIX-mediated cancels.
        ArgumentCaptor<CancelOrderCommand> captor = ArgumentCaptor.forClass(CancelOrderCommand.class);
        verify(cluster).submitCancelOrder(captor.capture(), any());
        CancelOrderCommand cmd = captor.getValue();
        assertThat(cmd.orderId()).isEqualTo(ORDER_ID);
        assertThat(cmd.correlationId()).isEqualTo(7L);
        assertThat(cmd.reason())
                .startsWith(AdminOrderController.REASON_PREFIX)
                .endsWith("post-journal-wipe");
    }

    /**
     * Cluster-forgot path: the controller submits successfully, but the cluster's apply is a
     * silent no-op (orderIndex miss, e.g. after a journal wipe), so the projector never sees an
     * {@code OrderCancelAppliedEvent} and the orders row never bumps version. The observation
     * poll runs out the clock and the endpoint returns 410 Gone with {@code cluster_forgot_order}.
     */
    @Test
    void forceCancel_clusterForgotOrder_pollTimesOut_returns410() throws Exception {
        Order working = orderWithStatus(OrderStatus.WORKING, 1);
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(working));
        when(cluster.nextCorrelationId()).thenReturn(99L);

        ResponseEntity<?> resp = controller.forceCancel(ORDER_ID, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.GONE);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body)
                .containsEntry("error", "cluster_forgot_order")
                .containsEntry("orderStatus", "WORKING")
                .containsEntry("observationTimeoutMs", TEST_OBSERVATION_TIMEOUT_MS);
        // We polled at least the pre-check + one poll iteration; exact count depends on host
        // scheduling, so we only assert ≥ 2.
        verify(orders, atLeast(2)).findById(ORDER_ID);
        // The cluster submit was still made — the 410 reflects observation, not refusal to try.
        verify(cluster).submitCancelOrder(any(), any());
    }

    @Test
    void forceCancel_clusterTimeout_returns503() throws Exception {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(orderWithStatus(OrderStatus.WORKING, 1)));
        when(cluster.nextCorrelationId()).thenReturn(1L);
        doThrow(new TimeoutException("back-pressure"))
                .when(cluster).submitCancelOrder(any(), any());

        ResponseEntity<?> resp = controller.forceCancel(ORDER_ID, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body).containsEntry("error", "cluster_submit_timeout");
    }

    @Test
    void forceCancel_clusterUnavailable_returns503() throws Exception {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(orderWithStatus(OrderStatus.WORKING, 1)));
        when(cluster.nextCorrelationId()).thenReturn(1L);
        doThrow(new IllegalStateException("cluster client not connected"))
                .when(cluster).submitCancelOrder(any(), any());

        ResponseEntity<?> resp = controller.forceCancel(ORDER_ID, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body).containsEntry("error", "cluster_unavailable");
    }

    /**
     * Operator-supplied reason is trimmed to {@link AdminOrderController#MAX_REASON_LEN} after the
     * {@link AdminOrderController#REASON_PREFIX} is applied. The cap exists so the cluster
     * command's bounded reason field is never truncated mid-multibyte sequence on UTF-8 encode.
     */
    @Test
    void forceCancel_overlongReason_trimmedBeforeSubmit() throws Exception {
        // Pre-check is the only findById we need (we error before the poll loop in this test);
        // we don't bother stubbing further calls — the test ends before the post-submit poll
        // would matter, asserted by the captured CancelOrderCommand below.
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(orderWithStatus(OrderStatus.WORKING, 1)));
        when(cluster.nextCorrelationId()).thenReturn(1L);
        String longReason = "x".repeat(AdminOrderController.MAX_REASON_LEN + 50);

        controller.forceCancel(
                ORDER_ID, new AdminOrderController.ForceCancelRequestBody(longReason));

        ArgumentCaptor<CancelOrderCommand> captor = ArgumentCaptor.forClass(CancelOrderCommand.class);
        verify(cluster).submitCancelOrder(captor.capture(), any());
        String reason = captor.getValue().reason();
        assertThat(reason).startsWith(AdminOrderController.REASON_PREFIX);
        // Trimmed length = prefix + MAX_REASON_LEN (the cap applies to the operator-supplied tail
        // only; the prefix is fixed and bounded).
        assertThat(reason.length())
                .isEqualTo(AdminOrderController.REASON_PREFIX.length() + AdminOrderController.MAX_REASON_LEN);
    }

    @Test
    void forceMarkCancelledPostgresOnly_unknownOrder_returns404() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.empty());

        ResponseEntity<?> resp = controller.forceMarkCancelledPostgresOnly(ORDER_ID, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        verifyNoInteractions(cluster);
        verifyNoMoreInteractions(orders);
    }

    @Test
    void forceMarkCancelledPostgresOnly_alreadyTerminal_returns409() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(orderWithStatus(OrderStatus.CANCELLED, 2)));

        ResponseEntity<?> resp = controller.forceMarkCancelledPostgresOnly(ORDER_ID, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        verifyNoInteractions(cluster);
        verify(orders).findById(ORDER_ID);
        verifyNoMoreInteractions(orders);
    }

    @Test
    void forceMarkCancelledPostgresOnly_casConflict_returns409() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(orderWithStatus(OrderStatus.WORKING, 1)));
        when(orders.updateWithCas(
                        eq(ORDER_ID),
                        eq(1),
                        eq(OrderStatus.CANCELLED),
                        eq(null),
                        eq(null),
                        any(Instant.class)))
                .thenReturn(false);

        ResponseEntity<?> resp = controller.forceMarkCancelledPostgresOnly(ORDER_ID, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body).containsEntry("error", "version_conflict");
        verifyNoInteractions(cluster);
    }

    @Test
    void forceMarkCancelledPostgresOnly_applied_returns200_withoutCluster() {
        Order working = orderWithStatus(OrderStatus.WORKING, 1);
        Order cancelled = orderWithStatus(OrderStatus.CANCELLED, 2);
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(working)).thenReturn(Optional.of(cancelled));
        when(orders.updateWithCas(
                        eq(ORDER_ID),
                        eq(1),
                        eq(OrderStatus.CANCELLED),
                        eq(null),
                        eq(null),
                        any(Instant.class)))
                .thenReturn(true);

        ResponseEntity<?> resp =
                controller.forceMarkCancelledPostgresOnly(
                        ORDER_ID, new AdminOrderController.ForceCancelRequestBody("journal-loss"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        AdminOrderController.AdminResponse body = (AdminOrderController.AdminResponse) resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo("postgres_cancel_applied");
        assertThat(body.orderStatus()).isEqualTo("CANCELLED");
        verifyNoInteractions(cluster);
    }

    private static Order orderWithStatus(OrderStatus status, int version) {
        return new Order(
                ORDER_ID,
                ACCOUNT_ID,
                "client-key",
                /* shardId = */ 0,
                version,
                status,
                /* terminalReason = */ null,
                Side.BUY,
                "AAPL",
                new BigDecimal("1.0"),
                new BigDecimal("100.0"),
                "DAY",
                Instant.EPOCH,
                Instant.EPOCH,
                /* terminalAt = */ status.isTerminal() ? Instant.EPOCH : null,
                /* accountIdHash = */ "hash",
                /* ledgerBalanceId = */ null,
                BigDecimal.ZERO);
    }
}
