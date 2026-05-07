package com.balh.oms.fix;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.persistence.OrdersRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import quickfix.Session;
import quickfix.SessionNotFound;
import quickfix.fix44.NewOrderSingle;

import java.util.UUID;

/**
 * Drains {@link FixRouteDispatcher} after logon and sends {@link NewOrderSingle} on the active session.
 */
public class FixOutboundDispatchWorker {

    private static final Logger log = LoggerFactory.getLogger(FixOutboundDispatchWorker.class);

    private final FixRouteDispatcher fixRouteDispatcher;
    private final FixSessionRegistry fixSessionRegistry;
    private final OrdersRepository ordersRepository;
    private final FixNewOrderSingleBuilder newOrderSingleBuilder;

    public FixOutboundDispatchWorker(
            FixRouteDispatcher fixRouteDispatcher,
            FixSessionRegistry fixSessionRegistry,
            OrdersRepository ordersRepository,
            FixNewOrderSingleBuilder newOrderSingleBuilder) {
        this.fixRouteDispatcher = fixRouteDispatcher;
        this.fixSessionRegistry = fixSessionRegistry;
        this.ordersRepository = ordersRepository;
        this.newOrderSingleBuilder = newOrderSingleBuilder;
    }

    @Scheduled(fixedDelayString = "${oms.fix.outbound-poll-interval-ms:100}")
    public void drainPendingOutbound() {
        if (!fixSessionRegistry.hasLoggedOnSession()) {
            return;
        }
        UUID id = fixRouteDispatcher.pollPendingOrNull();
        if (id == null) {
            return;
        }
        Order order = ordersRepository.findById(id).orElse(null);
        if (order == null || order.status() != OrderStatus.WORKING) {
            log.debug("Skipping FIX outbound for orderId={} (missing or not WORKING)", id);
            return;
        }
        try {
            NewOrderSingle nos = newOrderSingleBuilder.build(order);
            Session.sendToTarget(nos, fixSessionRegistry.sessionOrNull());
        } catch (SessionNotFound e) {
            log.warn("FIX sendToTarget failed (no session) orderId={}", id, e);
        } catch (Exception e) {
            log.error("FIX outbound send failed orderId={}", id, e);
        }
    }
}
