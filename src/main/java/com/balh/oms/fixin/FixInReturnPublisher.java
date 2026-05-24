package com.balh.oms.fixin;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.ExecutionAppliedEvent;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.Order;
import com.balh.oms.fixin.persistence.FixInDropCopyEntitlementRepository;
import com.balh.oms.fixin.persistence.FixInOrderMapRepository;
import com.balh.oms.fixin.persistence.FixInOrderMapRow;
import com.balh.oms.fixin.persistence.FixInOutboundSentRepository;
import com.balh.oms.persistence.OrdersRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import quickfix.Message;
import quickfix.fix44.OrderCancelReject;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Maps cluster projection events to client-facing FIX 8/9. Skips duplicate {@code NEW} for
 * FIX-in-origin orders (synchronous admission path already sent {@code ExecType=NEW}).
 */
@Service
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInReturnPublisher {

    private static final Logger log = LoggerFactory.getLogger(FixInReturnPublisher.class);

    private final OrdersRepository ordersRepository;
    private final FixInOrderMapRepository orderMapRepository;
    private final FixInOutboundSentRepository outboundSentRepository;
    private final FixInDropCopyEntitlementRepository dropCopyEntitlementRepository;
    private final FixInExecutionReportBuilder executionReportBuilder;
    private final FixInWireDelivery wireDelivery;

    public FixInReturnPublisher(
            OrdersRepository ordersRepository,
            FixInOrderMapRepository orderMapRepository,
            FixInOutboundSentRepository outboundSentRepository,
            FixInDropCopyEntitlementRepository dropCopyEntitlementRepository,
            FixInExecutionReportBuilder executionReportBuilder,
            FixInWireDelivery wireDelivery) {
        this.ordersRepository = ordersRepository;
        this.orderMapRepository = orderMapRepository;
        this.outboundSentRepository = outboundSentRepository;
        this.dropCopyEntitlementRepository = dropCopyEntitlementRepository;
        this.executionReportBuilder = executionReportBuilder;
        this.wireDelivery = wireDelivery;
    }

    /** @return {@code true} when at least one outbound message was sent. */
    public boolean publishOrderAdmitted(OrderAdmittedEvent ev) {
        if (ev.fixInIngressMetadataOrNull() != null) {
            return false;
        }
        return false;
    }

    public boolean publishExecutionApplied(ExecutionAppliedEvent ev) {
        List<FixInOrderMapRow> mappings = orderMapRepository.findByOmsOrderId(ev.orderId());
        if (mappings.isEmpty()) {
            return false;
        }
        Optional<Order> orderOpt = ordersRepository.findById(ev.orderId());
        if (orderOpt.isEmpty()) {
            log.warn("FIX-in return skipped — order {} not in projection yet", ev.orderId());
            return false;
        }
        Order order = orderOpt.get();
        boolean sent = false;
        if (ev.execTypeCode() == ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT
                || ev.execTypeCode() == ApplyExecutionReportCommand.EXEC_TYPE_REPLACE_REJECT) {
            for (FixInOrderMapRow mapping : mappings) {
                String dedupeKey = dedupeKey(ev);
                if (!outboundSentRepository.tryMarkSent(mapping.sessionId(), dedupeKey)) {
                    continue;
                }
                OrderCancelReject reject = executionReportBuilder.buildCancelReject(
                        ev.orderId(),
                        mapping.clientClOrdId(),
                        mapping.origClientClOrdIdOrNull() == null
                                ? mapping.clientClOrdId()
                                : mapping.origClientClOrdIdOrNull(),
                        "broker_reject");
                sent |= wireDelivery.sendToSession(
                        mapping.sessionId(), reject, FixInWireDelivery.sessionRoleOrderEntry(), ev.orderId());
                sent |= fanoutDropCopy(order, reject, ev.orderId());
            }
            return sent;
        }
        for (FixInOrderMapRow mapping : mappings) {
            String dedupeKey = dedupeKey(ev) + ":" + mapping.sessionId();
            if (!outboundSentRepository.tryMarkSent(mapping.sessionId(), dedupeKey)) {
                continue;
            }
            Message er = executionReportBuilder.buildFromExecutionApplied(order, mapping.clientClOrdId(), ev);
            sent |= wireDelivery.sendToSession(
                    mapping.sessionId(), er, FixInWireDelivery.sessionRoleOrderEntry(), ev.orderId());
            sent |= fanoutDropCopy(order, er, ev.orderId());
        }
        return sent;
    }

    private boolean fanoutDropCopy(Order order, Message message, UUID omsOrderId) {
        boolean sent = false;
        for (UUID dropSessionId : dropCopyEntitlementRepository.findDropCopySessionIdsForAccount(order.accountId())) {
            sent |= wireDelivery.sendToSession(
                    dropSessionId, message, FixInWireDelivery.sessionRoleDropCopy(), omsOrderId);
        }
        return sent;
    }

    private static String dedupeKey(ExecutionAppliedEvent ev) {
        return ev.venueExecRef() + ":" + ev.execTypeCode();
    }
}
