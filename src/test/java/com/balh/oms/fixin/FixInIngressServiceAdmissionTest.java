package com.balh.oms.fixin;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterShardRouter;
import com.balh.oms.cluster.OrderAcceptedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.fixin.persistence.FixInAccountBindingRepository;
import com.balh.oms.fixin.persistence.FixInAccountBindingRow;
import com.balh.oms.fixin.persistence.FixInOrderMapRepository;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import com.balh.oms.persistence.OrdersRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import quickfix.SessionID;
import quickfix.field.ClOrdID;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.NewOrderSingle;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FixInIngressServiceAdmissionTest {

    @Mock private OmsClusterShardRouter clusterShardRouter;
    @Mock private OmsClusterIngressClient cluster;
    @Mock private FixInAccountBindingRepository accountBindingRepository;
    @Mock private FixInOrderMapRepository orderMapRepository;
    @Mock private FixInSessionRegistry sessionRegistry;
    @Mock private FixInWireDelivery wireDelivery;
    @Mock private OrdersRepository ordersRepository;

    private FixInIngressService ingressService;
    private UUID sessionUuid = UUID.fromString("00000001-0000-4000-8000-000000000001");
    private SessionID wireSessionId = new SessionID("FIX.4.4", "BALH_OMS", "LOOPBACK_CLIENT");

    @BeforeEach
    void setUp() {
        OmsConfig omsConfig = new OmsConfig();
        omsConfig.getCluster().getClient().setSubmitTimeoutMs(5_000);
        omsConfig.getFixIn().setSymbolMapJson("{}");
        ingressService = new FixInIngressService(
                omsConfig,
                clusterShardRouter,
                new FixInNewOrderSingleParser(new FixInSymbolMapper(omsConfig, new com.fasterxml.jackson.databind.ObjectMapper())),
                new FixInOrderCancelRequestParser(new FixInSymbolMapper(omsConfig, new com.fasterxml.jackson.databind.ObjectMapper())),
                new FixInOrderCancelReplaceRequestParser(new FixInSymbolMapper(omsConfig, new com.fasterxml.jackson.databind.ObjectMapper())),
                new IngressAcceptOrderCommandFactory(new com.balh.oms.observability.PiiHash(omsConfig)),
                new IngressLifecycleCommandFactory(),
                new FixInExecutionReportBuilder(),
                accountBindingRepository,
                orderMapRepository,
                sessionRegistry,
                wireDelivery,
                ordersRepository);
        when(sessionRegistry.find(wireSessionId))
                .thenReturn(Optional.of(new FixInSessionRow(
                        sessionUuid,
                        UUID.randomUUID(),
                        "UAT",
                        "ORDER_ENTRY",
                        "LOOPBACK_CLIENT",
                        "BALH_OMS",
                        null,
                        null,
                        null,
                        30,
                        true)));
    }

    @Test
    void newOrderSingle_clusterAccept_sendsNewExecutionReport() throws Exception {
        UUID accountId = UUID.fromString("a0000001-0000-4000-8000-000000000002");
        UUID bindingId = UUID.randomUUID();
        when(accountBindingRepository.findDefaultForSession(sessionUuid))
                .thenReturn(Optional.of(new FixInAccountBindingRow(
                        bindingId, sessionUuid, "", accountId, null, null, true, true)));
        when(clusterShardRouter.routeAdmit(accountId)).thenReturn(cluster);
        when(cluster.nextCorrelationId()).thenReturn(99L);
        UUID orderId = UUID.randomUUID();
        when(cluster.submitAcceptOrder(any(AcceptOrderCommand.class), any(Duration.class)))
                .thenReturn(new AdmissionResult.Accepted(new OrderAcceptedEvent(99L, orderId, 1, false, 1L)));

        NewOrderSingle nos = new NewOrderSingle();
        nos.set(new ClOrdID("LOOP-1"));
        nos.set(new Symbol("AAPL"));
        nos.set(new Side(Side.BUY));
        nos.set(new OrdType(OrdType.LIMIT));
        nos.set(new OrderQty(10));
        nos.set(new Price(100));
        nos.set(new TimeInForce(TimeInForce.DAY));

        ingressService.handleNewOrderSingle(nos, wireSessionId);

        verify(orderMapRepository).insertIfAbsent(sessionUuid, "LOOP-1", orderId, bindingId);
        verify(wireDelivery).sendToSession(eq(sessionUuid), any(ExecutionReport.class), any(), any());
    }
}
