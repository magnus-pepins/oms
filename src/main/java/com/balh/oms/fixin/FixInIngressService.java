package com.balh.oms.fixin;

import com.balh.oms.cluster.AdmissionResult;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterShardRouter;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.persistence.FixInAccountBindingRepository;
import com.balh.oms.fixin.persistence.FixInAccountBindingRow;
import com.balh.oms.fixin.persistence.FixInOrderMapRepository;
import com.balh.oms.fixin.persistence.FixInOrderMapRow;
import com.balh.oms.fixin.persistence.FixInSessionRow;
import com.balh.oms.persistence.OrdersRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.SessionID;
import quickfix.field.MsgType;
import quickfix.fix44.BusinessMessageReject;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.OrderCancelReject;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Service
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInIngressService {

    private static final Logger log = LoggerFactory.getLogger(FixInIngressService.class);

    private final OmsConfig omsConfig;
    private final OmsClusterShardRouter clusterShardRouter;
    private final FixInNewOrderSingleParser newOrderParser;
    private final FixInOrderCancelRequestParser cancelParser;
    private final FixInOrderCancelReplaceRequestParser replaceParser;
    private final IngressAcceptOrderCommandFactory commandFactory;
    private final IngressLifecycleCommandFactory lifecycleCommandFactory;
    private final FixInExecutionReportBuilder executionReportBuilder;
    private final FixInAccountBindingRepository accountBindingRepository;
    private final FixInOrderMapRepository orderMapRepository;
    private final FixInSessionRegistry sessionRegistry;
    private final FixInWireDelivery wireDelivery;
    private final OrdersRepository ordersRepository;

    public FixInIngressService(
            OmsConfig omsConfig,
            OmsClusterShardRouter clusterShardRouter,
            FixInNewOrderSingleParser newOrderParser,
            FixInOrderCancelRequestParser cancelParser,
            FixInOrderCancelReplaceRequestParser replaceParser,
            IngressAcceptOrderCommandFactory commandFactory,
            IngressLifecycleCommandFactory lifecycleCommandFactory,
            FixInExecutionReportBuilder executionReportBuilder,
            FixInAccountBindingRepository accountBindingRepository,
            FixInOrderMapRepository orderMapRepository,
            FixInSessionRegistry sessionRegistry,
            FixInWireDelivery wireDelivery,
            OrdersRepository ordersRepository) {
        this.omsConfig = omsConfig;
        this.clusterShardRouter = clusterShardRouter;
        this.newOrderParser = newOrderParser;
        this.cancelParser = cancelParser;
        this.replaceParser = replaceParser;
        this.commandFactory = commandFactory;
        this.lifecycleCommandFactory = lifecycleCommandFactory;
        this.executionReportBuilder = executionReportBuilder;
        this.accountBindingRepository = accountBindingRepository;
        this.orderMapRepository = orderMapRepository;
        this.sessionRegistry = sessionRegistry;
        this.wireDelivery = wireDelivery;
        this.ordersRepository = ordersRepository;
    }

    public void handleNewOrderSingle(Message message, SessionID sessionId) throws FieldNotFound {
        FixInSessionRow session = requireOrderEntrySession(sessionId);
        wireDelivery.auditInbound(message, session, null);

        FixInParsedNewOrder parsed;
        try {
            parsed = newOrderParser.parse(message);
        } catch (FixInParseException e) {
            sendRejectedWithoutCluster(session, parsedOrNull(message), e.getMessage());
            return;
        }

        Optional<FixInAccountBindingRow> binding =
                resolveAccountBinding(session.id(), parsed.fixAccountTagOrEmpty());
        if (binding.isEmpty()) {
            sendRejected(session, parsed, UUID.randomUUID(), "account_binding_not_found");
            return;
        }

        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        OmsClusterIngressClient cluster = clusterShardRouter.routeAdmit(binding.get().omsAccountId());
        long correlationId = cluster.nextCorrelationId();
        var cmd = commandFactory.buildNewOrder(
                correlationId,
                orderId,
                session.id(),
                parsed,
                binding.get(),
                omsConfig.getShard().getCount(),
                now);

        AdmissionResult result = submitAccept(cluster, cmd);
        if (result == null) {
            sendRejected(session, parsed, orderId, "cluster_unavailable");
            return;
        }
        if (result instanceof AdmissionResult.Rejected rejected) {
            sendRejected(session, parsed, orderId, rejected.event().reason());
            return;
        }

        AdmissionResult.Accepted accepted = (AdmissionResult.Accepted) result;
        UUID admittedOrderId = accepted.event().orderId();
        if (!accepted.event().duplicate()) {
            orderMapRepository.insertIfAbsent(
                    session.id(), parsed.clientClOrdId(), admittedOrderId, binding.get().id());
        }
        sendExecutionReport(session, executionReportBuilder.buildNew(admittedOrderId, parsed));
    }

    public void handleOrderCancelRequest(Message message, SessionID sessionId) throws FieldNotFound {
        FixInSessionRow session = requireOrderEntrySession(sessionId);
        wireDelivery.auditInbound(message, session, null);
        FixInParsedCancel parsed = cancelParser.parse(message);
        Optional<FixInOrderMapRow> mapping =
                orderMapRepository.findBySessionAndClientClOrdId(session.id(), parsed.origClientClOrdId());
        if (mapping.isEmpty()) {
            OrderCancelReject reject = executionReportBuilder.buildCancelReject(
                    UUID.randomUUID(),
                    parsed.clientClOrdId(),
                    parsed.origClientClOrdId(),
                    "unknown_order");
            wireDelivery.sendToSession(
                    session.id(), reject, FixInWireDelivery.sessionRoleOrderEntry(), null);
            return;
        }
        OmsClusterIngressClient cluster = clusterForOrder(mapping.get().omsOrderId());
        var cmd = lifecycleCommandFactory.buildCancel(
                cluster.nextCorrelationId(),
                mapping.get().omsOrderId(),
                session.id(),
                parsed.clientClOrdId(),
                "fix_in_cancel");
        submitLifecycle(cluster, cmd);
    }

    public void handleOrderCancelReplaceRequest(Message message, SessionID sessionId) throws FieldNotFound {
        FixInSessionRow session = requireOrderEntrySession(sessionId);
        wireDelivery.auditInbound(message, session, null);
        FixInParsedReplace parsed = replaceParser.parse(message);
        Optional<FixInOrderMapRow> mapping =
                orderMapRepository.findBySessionAndClientClOrdId(session.id(), parsed.origClientClOrdId());
        if (mapping.isEmpty()) {
            OrderCancelReject reject = executionReportBuilder.buildCancelReject(
                    UUID.randomUUID(),
                    parsed.clientClOrdId(),
                    parsed.origClientClOrdId(),
                    "unknown_order");
            wireDelivery.sendToSession(
                    session.id(), reject, FixInWireDelivery.sessionRoleOrderEntry(), null);
            return;
        }
        long qtyScaled = FixInNewOrderSingleParser.scaleQuantity(parsed.quantity());
        long priceScaled =
                FixInNewOrderSingleParser.scaleLimitPrice(parsed.limitPriceOrNull(), parsed.ordTypeCode());
        OmsClusterIngressClient cluster = clusterForOrder(mapping.get().omsOrderId());
        var cmd = lifecycleCommandFactory.buildReplace(
                cluster.nextCorrelationId(),
                mapping.get().omsOrderId(),
                session.id(),
                parsed.clientClOrdId(),
                qtyScaled,
                priceScaled,
                "fix_in_replace");
        submitLifecycle(cluster, cmd);
        orderMapRepository.insertIfAbsent(
                session.id(),
                parsed.clientClOrdId(),
                mapping.get().omsOrderId(),
                parsed.origClientClOrdId(),
                null);
    }

    public void rejectDropCopyOrderEntry(Message message, SessionID sessionId) throws FieldNotFound {
        FixInSessionRow session =
                sessionRegistry.find(sessionId).orElseThrow(() -> new IllegalStateException("fix_in_session_unknown"));
        BusinessMessageReject reject = new BusinessMessageReject();
        reject.setField(new quickfix.field.RefMsgType(message.getHeader().getString(MsgType.FIELD)));
        reject.setString(quickfix.field.Text.FIELD, "drop_copy_session_order_entry_forbidden");
        wireDelivery.sendToSession(
                session.id(), reject, FixInWireDelivery.sessionRoleDropCopy(), null);
    }

    private OmsClusterIngressClient clusterForOrder(UUID omsOrderId) {
        return ordersRepository
                .findById(omsOrderId)
                .map(order -> clusterShardRouter.routeAdmit(order.accountId()))
                .orElseGet(() -> clusterShardRouter.forShard(0));
    }

    private FixInSessionRow requireOrderEntrySession(SessionID sessionId) {
        FixInSessionRow session =
                sessionRegistry.find(sessionId).orElseThrow(() -> new IllegalStateException("fix_in_session_unknown"));
        if (!"ORDER_ENTRY".equalsIgnoreCase(session.sessionMode())) {
            throw new FixInParseException("session_mode_not_order_entry");
        }
        return session;
    }

    private Optional<FixInAccountBindingRow> resolveAccountBinding(UUID sessionId, String fixAccountTagOrEmpty) {
        Optional<FixInAccountBindingRow> exact =
                accountBindingRepository.findBySessionAndAccountTag(sessionId, fixAccountTagOrEmpty);
        if (exact.isPresent()) {
            return exact;
        }
        if (fixAccountTagOrEmpty.isBlank()) {
            return accountBindingRepository.findDefaultForSession(sessionId);
        }
        return Optional.empty();
    }

    private AdmissionResult submitAccept(OmsClusterIngressClient cluster, com.balh.oms.cluster.AcceptOrderCommand cmd) {
        Duration timeout = Duration.ofMillis(omsConfig.getCluster().getClient().getSubmitTimeoutMs());
        try {
            return cluster.submitAcceptOrder(cmd, timeout);
        } catch (TimeoutException e) {
            log.warn("FIX-in cluster accept timeout orderId={}", cmd.orderId(), e);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (IllegalStateException e) {
            log.warn("FIX-in cluster unavailable", e);
            return null;
        }
    }

    private void submitLifecycle(OmsClusterIngressClient cluster, Object cmd) {
        Duration timeout = Duration.ofMillis(omsConfig.getCluster().getClient().getSubmitTimeoutMs());
        try {
            if (cmd instanceof com.balh.oms.cluster.RequestCancelOrderCommand cancel) {
                cluster.submitRequestCancelOrder(cancel, timeout);
            } else if (cmd instanceof com.balh.oms.cluster.RequestReplaceOrderCommand replace) {
                cluster.submitRequestReplaceOrder(replace, timeout);
            }
        } catch (Exception e) {
            log.warn("FIX-in lifecycle submit failed", e);
        }
    }

    private void sendRejectedWithoutCluster(
            FixInSessionRow session, FixInParsedNewOrder parsedOrNull, String reason) {
        if (parsedOrNull == null) {
            log.warn("FIX-in reject without parsed order on {}: {}", session.id(), reason);
            return;
        }
        sendRejected(session, parsedOrNull, UUID.randomUUID(), reason);
    }

    private FixInParsedNewOrder parsedOrNull(Message message) {
        try {
            return newOrderParser.parse(message);
        } catch (Exception e) {
            return null;
        }
    }

    private void sendRejected(FixInSessionRow session, FixInParsedNewOrder parsed, UUID orderId, String reason) {
        sendExecutionReport(session, executionReportBuilder.buildRejected(orderId, parsed, reason));
    }

    private void sendExecutionReport(FixInSessionRow session, ExecutionReport er) {
        wireDelivery.sendToSession(session.id(), er, FixInWireDelivery.sessionRoleOrderEntry(), null);
    }
}
