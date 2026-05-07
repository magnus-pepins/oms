package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.persistence.OrdersRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Applies a control-plane event to Postgres after it has been read off the
 * Chronicle journal (slice 1) or — earlier in the lifecycle — directly from a
 * Disruptor handler. Both paths converge here.
 *
 * <p>Idempotent: every update goes through CAS on
 * {@code orders.version}. Re-applying the same event is a no-op.
 */
@Component
public class ControlTailer {

    private static final Logger log = LoggerFactory.getLogger(ControlTailer.class);

    private final OrdersRepository orders;
    private final StaleJobGuard stale;

    public ControlTailer(OrdersRepository orders, StaleJobGuard stale) {
        this.orders = orders;
        this.stale = stale;
    }

    public TailResult apply(PendingControlEvent event) {
        if (stale.isStale(event.orderTimestamp())) {
            boolean updated = orders.updateWithCas(
                    event.orderId(),
                    event.orderVersion(),
                    OrderStatus.REJECTED,
                    RejectCode.RISK_STALE_QUEUE,
                    null,
                    Instant.now()
            );
            return updated ? TailResult.STALE_REJECTED : TailResult.SKIPPED_VERSION_MISMATCH;
        }

        // Slice 1 ships the OrderAccepted path only. Real risk checks land in
        // slice 2 (Ledger inflight, kill switch, fat-finger, etc.).
        boolean updated = orders.updateWithCas(
                event.orderId(),
                event.orderVersion(),
                OrderStatus.WORKING,
                null,
                event.orderTimestamp(),
                null
        );
        if (!updated) {
            log.debug("CAS skipped for orderId={} expectedVersion={} (someone else won the race)",
                    event.orderId(), event.orderVersion());
            return TailResult.SKIPPED_VERSION_MISMATCH;
        }
        return TailResult.APPLIED;
    }

    public enum TailResult {
        APPLIED,
        SKIPPED_VERSION_MISMATCH,
        STALE_REJECTED
    }
}
