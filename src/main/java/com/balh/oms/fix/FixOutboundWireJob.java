package com.balh.oms.fix;

import quickfix.fix44.OrderMassCancelRequest;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Single outbound queue carries {@link quickfix.fix44.NewOrderSingle} work (by order id) and occasional
 * operator-driven {@link OrderMassCancelRequest} so QuickFIX {@code Session.sendToTarget} runs only from
 * {@link FixOutboundDispatchWorker}.
 */
public sealed interface FixOutboundWireJob permits FixOutboundWireJob.NosOrder, FixOutboundWireJob.MassCancelWire {

    record NosOrder(UUID orderId) implements FixOutboundWireJob {}

    record MassCancelWire(OrderMassCancelRequest message, CompletableFuture<Void> completion)
            implements FixOutboundWireJob {}
}
