package com.balh.oms.ingress;

import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.RequestCancelOrderCommand;
import com.balh.oms.cluster.RequestReplaceOrderCommand;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link OrderLifecycleController}. Mocks {@link OrdersRepository} +
 * {@link OmsClusterIngressClient}; asserts:
 *
 * <ul>
 *   <li>happy-path submits a {@link RequestCancelOrderCommand} /
 *       {@link RequestReplaceOrderCommand} with the right {@code clientRequestKey} precedence
 *       (body > header > synthesized fallback) and scaled quantity / price values;</li>
 *   <li>terminal orders return 409 without touching the cluster;</li>
 *   <li>unknown orders return 404 without touching the cluster;</li>
 *   <li>negative / zero / fractional-precision inputs return 400 before any cluster submit;</li>
 *   <li>cluster timeout / connection failures surface as 503 (not propagated).</li>
 * </ul>
 *
 * <p>End-to-end IT coverage (HTTP → cluster → FIX-egress → acceptor) lives in
 * {@code OmsFixEgressBrokerIT}'s upcoming cancel + replace IT cases.
 */
@ExtendWith(MockitoExtension.class)
class OrderLifecycleControllerTest {

    private static final UUID ORDER_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");

    @Mock private OmsClusterIngressClient cluster;
    @Mock private OrdersRepository orders;

    private OmsConfig config;
    private OrderLifecycleController controller;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        controller = new OrderLifecycleController(config, cluster, orders, new SimpleMeterRegistry());
    }

    // ---- Cancel ----------------------------------------------------------------------

    @Test
    void cancel_happyPath_submitsCommandAndReturns202() throws Exception {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(workingOrder()));
        when(cluster.nextCorrelationId()).thenReturn(42L);

        ResponseEntity<?> resp = controller.cancel(
                ORDER_ID,
                new OrderLifecycleController.CancelRequestBody("idem-cancel-1", "user-cancel"),
                /* idempotencyKeyParam = */ null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        ArgumentCaptor<RequestCancelOrderCommand> captor =
                ArgumentCaptor.forClass(RequestCancelOrderCommand.class);
        verify(cluster).submitRequestCancelOrder(captor.capture(), any());
        RequestCancelOrderCommand cmd = captor.getValue();
        assertThat(cmd.correlationId()).isEqualTo(42L);
        assertThat(cmd.orderId()).isEqualTo(ORDER_ID);
        assertThat(cmd.clientRequestKey()).isEqualTo("idem-cancel-1");
        assertThat(cmd.reason()).isEqualTo("user-cancel");
    }

    @Test
    void cancel_terminalOrder_returns409_withoutClusterSubmit() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(orderWithStatus(OrderStatus.FILLED)));

        ResponseEntity<?> resp = controller.cancel(ORDER_ID, null, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body).containsEntry("orderStatus", "FILLED");
        verifyNoInteractions(cluster);
    }

    @Test
    void cancel_unknownOrder_returns404_withoutClusterSubmit() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.empty());

        ResponseEntity<?> resp = controller.cancel(ORDER_ID, null, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        verifyNoInteractions(cluster);
    }

    @Test
    void cancel_keyPrecedence_bodyBeatsHeaderBeatsFallback() throws Exception {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(workingOrder()));
        when(cluster.nextCorrelationId()).thenReturn(1L, 2L, 3L);

        controller.cancel(
                ORDER_ID,
                new OrderLifecycleController.CancelRequestBody("body-key", null),
                "header-key");
        controller.cancel(ORDER_ID, null, "header-key");
        controller.cancel(ORDER_ID, null, null);

        ArgumentCaptor<RequestCancelOrderCommand> captor =
                ArgumentCaptor.forClass(RequestCancelOrderCommand.class);
        verify(cluster, org.mockito.Mockito.times(3))
                .submitRequestCancelOrder(captor.capture(), any());
        assertThat(captor.getAllValues().get(0).clientRequestKey()).isEqualTo("body-key");
        assertThat(captor.getAllValues().get(1).clientRequestKey()).isEqualTo("header-key");
        assertThat(captor.getAllValues().get(2).clientRequestKey())
                .as("missing both body+header keys ⇒ controller synthesizes a deterministic fallback")
                .isEqualTo(ORDER_ID + ":cancel:fallback");
    }

    @Test
    void cancel_clusterTimeout_returns503() throws Exception {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(workingOrder()));
        when(cluster.nextCorrelationId()).thenReturn(1L);
        doThrow(new TimeoutException("back-pressure"))
                .when(cluster).submitRequestCancelOrder(any(), any());

        ResponseEntity<?> resp = controller.cancel(ORDER_ID, null, null);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    // ---- Replace ---------------------------------------------------------------------

    @Test
    void replace_happyPath_scalesQtyAndPrice_andSubmits() throws Exception {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(workingOrder()));
        when(cluster.nextCorrelationId()).thenReturn(99L);

        ResponseEntity<?> resp = controller.replace(
                ORDER_ID,
                new OrderLifecycleController.ReplaceRequestBody(
                        "idem-rep-1",
                        new BigDecimal("1.5"),
                        new BigDecimal("150.5"),
                        "operator-resize"),
                /* idempotencyKeyParam = */ null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        ArgumentCaptor<RequestReplaceOrderCommand> captor =
                ArgumentCaptor.forClass(RequestReplaceOrderCommand.class);
        verify(cluster).submitRequestReplaceOrder(captor.capture(), any());
        RequestReplaceOrderCommand cmd = captor.getValue();
        assertThat(cmd.correlationId()).isEqualTo(99L);
        assertThat(cmd.orderId()).isEqualTo(ORDER_ID);
        assertThat(cmd.newQuantityScaled()).isEqualTo(1_500_000_000L);
        assertThat(cmd.newLimitPriceScaledOrZero()).isEqualTo(150_500_000L);
        assertThat(cmd.clientRequestKey()).isEqualTo("idem-rep-1");
        assertThat(cmd.reason()).isEqualTo("operator-resize");
    }

    @Test
    void replace_qtyOnlyModify_priceZeroOnWire_meansKeepExistingPrice() throws Exception {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(workingOrder()));
        when(cluster.nextCorrelationId()).thenReturn(1L);

        controller.replace(
                ORDER_ID,
                new OrderLifecycleController.ReplaceRequestBody(
                        "k", new BigDecimal("2"), /* newLimitPrice = */ null, ""),
                null);

        ArgumentCaptor<RequestReplaceOrderCommand> captor =
                ArgumentCaptor.forClass(RequestReplaceOrderCommand.class);
        verify(cluster).submitRequestReplaceOrder(captor.capture(), any());
        assertThat(captor.getValue().newLimitPriceScaledOrZero())
                .as("null/0 newLimitPrice ⇒ wire carries 0; FIX builder copies the original price")
                .isZero();
    }

    @Test
    void replace_zeroQuantity_returns400_beforeAnyDbOrClusterCall() {
        // Body-shape validation runs before the orders lookup, so the OrdersRepository is also
        // never consulted — both downstream deps stay untouched.
        ResponseEntity<?> resp = controller.replace(
                ORDER_ID,
                new OrderLifecycleController.ReplaceRequestBody("k", BigDecimal.ZERO, null, ""),
                null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        verifyNoInteractions(orders);
        verifyNoInteractions(cluster);
    }

    @Test
    void replace_negativePrice_returns400_beforeAnyDbOrClusterCall() {
        ResponseEntity<?> resp = controller.replace(
                ORDER_ID,
                new OrderLifecycleController.ReplaceRequestBody(
                        "k", new BigDecimal("1"), new BigDecimal("-0.01"), ""),
                null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        verifyNoInteractions(orders);
        verifyNoInteractions(cluster);
    }

    @Test
    void replace_fractionalQtyPrecisionLoss_returns400_viaScaleException() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(workingOrder()));

        // 1.5e-15 cannot be represented in 1e9 fixed-point scale → BadInputException.
        BigDecimal tooPrecise = new BigDecimal("0.0000000000000015");
        ResponseEntity<?> resp = controller.handleBadInput(
                catchBadInput(() -> controller.replace(
                        ORDER_ID,
                        new OrderLifecycleController.ReplaceRequestBody(
                                "k", tooPrecise, null, ""),
                        null)));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertThat(body).containsEntry("error", "new_quantity_invalid_scale");
    }

    @Test
    void replace_terminalOrder_returns409_withoutClusterSubmit() {
        when(orders.findById(ORDER_ID)).thenReturn(Optional.of(orderWithStatus(OrderStatus.CANCELLED)));

        ResponseEntity<?> resp = controller.replace(
                ORDER_ID,
                new OrderLifecycleController.ReplaceRequestBody(
                        "k", new BigDecimal("1"), new BigDecimal("100"), ""),
                null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        verifyNoInteractions(cluster);
    }

    private static OrderLifecycleController.BadInputException catchBadInput(Runnable r) {
        try {
            r.run();
            throw new AssertionError("expected BadInputException, got none");
        } catch (OrderLifecycleController.BadInputException e) {
            return e;
        }
    }

    private static Order workingOrder() {
        return orderWithStatus(OrderStatus.WORKING);
    }

    private static Order orderWithStatus(OrderStatus status) {
        return new Order(
                ORDER_ID,
                UUID.fromString("aaaaaaaa-bbbb-4ccc-9ddd-eeeeeeeeeeee"),
                "client-key",
                /* shardId = */ 0,
                /* version = */ 1,
                status,
                /* terminalReason = */ null,
                Side.BUY,
                "AAPL",
                new BigDecimal("1.0"),
                new BigDecimal("100.0"),
                "DAY",
                Instant.EPOCH,
                Instant.EPOCH,
                /* terminalAt = */ null,
                /* accountIdHash = */ "hash",
                /* ledgerBalanceId = */ null,
                BigDecimal.ZERO);
    }
}
