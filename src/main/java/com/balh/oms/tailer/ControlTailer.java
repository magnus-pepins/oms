package com.balh.oms.tailer;

import com.balh.oms.chronicle.PendingControlEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.risk.BuyingPowerAdmission;
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
    private final OmsConfig config;
    private final BuyingPowerAdmission buyingPower;

    public ControlTailer(
            OrdersRepository orders,
            StaleJobGuard stale,
            OmsConfig config,
            BuyingPowerAdmission buyingPower) {
        this.orders = orders;
        this.stale = stale;
        this.config = config;
        this.buyingPower = buyingPower;
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

        if (config.getLedger().isEnabled()) {
            var row = orders.findById(event.orderId());
            if (row.isEmpty()) {
                log.warn("Control event references unknown orderId={}", event.orderId());
                return TailResult.UNKNOWN_ORDER;
            }
            switch (buyingPower.evaluate(row.get())) {
                case REJECT_INSUFFICIENT -> {
                    boolean updated = orders.updateWithCas(
                            event.orderId(),
                            event.orderVersion(),
                            OrderStatus.REJECTED,
                            RejectCode.RISK_BUYING_POWER,
                            null,
                            Instant.now()
                    );
                    return updated ? TailResult.BUYING_POWER_REJECTED : TailResult.SKIPPED_VERSION_MISMATCH;
                }
                case REJECT_LEDGER_UNAVAILABLE -> {
                    boolean updated = orders.updateWithCas(
                            event.orderId(),
                            event.orderVersion(),
                            OrderStatus.REJECTED,
                            RejectCode.INTERNAL_ERROR,
                            null,
                            Instant.now()
                    );
                    return updated ? TailResult.LEDGER_SERVICE_REJECTED : TailResult.SKIPPED_VERSION_MISMATCH;
                }
                case PROCEED -> { /* fall through */ }
            }
        }

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
        STALE_REJECTED,
        UNKNOWN_ORDER,
        BUYING_POWER_REJECTED,
        LEDGER_SERVICE_REJECTED
    }
}
