package com.balh.oms.routing;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.returnpath.ExecutionReportApplier;
import com.balh.oms.returnpath.ExecutionTradeCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Simulated broker: drains the post-{@code WORKING} queue and emits synthetic fills (slice 3).
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "simulated")
public class SimulatedFillEngine {

    private static final Logger log = LoggerFactory.getLogger(SimulatedFillEngine.class);

    private static final int QTY_SCALE = 10;

    private final OmsConfig config;
    private final OrdersRepository orders;
    private final ExecutionReportApplier applier;
    private final BlockingQueue<UUID> queue;

    public SimulatedFillEngine(OmsConfig config, OrdersRepository orders, ExecutionReportApplier applier) {
        this.config = config;
        this.orders = orders;
        this.applier = applier;
        int cap = Math.max(1, config.getRouting().getSimulated().getQueueCapacity());
        this.queue = new LinkedBlockingQueue<>(cap);
    }

    public void enqueueWorkingOrder(UUID orderId) {
        if (!queue.offer(orderId)) {
            log.error("Simulated route queue full; dropping orderId={}", orderId);
        }
    }

    @Scheduled(
            initialDelayString = "${oms.routing.simulated.poll-interval-ms:50}",
            fixedDelayString = "${oms.routing.simulated.poll-interval-ms:50}")
    public void scheduledDrain() {
        if (!config.getRouting().getSimulated().isSchedulerEnabled()) {
            return;
        }
        drainOnceInternal();
    }

    /** Test hook: process at most one queued order through the programmed fill sequence. */
    public void drainOnceForTests() {
        drainOnceInternal();
    }

    private void drainOnceInternal() {
        UUID id = queue.poll();
        if (id == null) {
            return;
        }
        runProgrammedFills(id);
    }

    void runProgrammedFills(UUID orderId) {
        Order first = orders.findById(orderId).orElse(null);
        if (first == null || first.status() != OrderStatus.WORKING) {
            return;
        }
        String venue = config.getRouting().getSimulated().getVenueId();
        BigDecimal px = first.limitPrice() != null ? first.limitPrice() : BigDecimal.ONE;
        List<BigDecimal> chunks = splitInThirds(first.quantity());
        int seq = 0;
        for (BigDecimal chunk : chunks) {
            Order o = orders.findById(orderId).orElse(null);
            if (o == null) {
                return;
            }
            if (o.status() != OrderStatus.WORKING && o.status() != OrderStatus.PARTIALLY_FILLED) {
                return;
            }
            seq++;
            String ref = "sim-" + orderId + "-" + seq;
            Instant ts = Instant.now();
            BigDecimal newCum = o.cumFilledQuantity().add(chunk);
            BigDecimal leaves = o.quantity().subtract(newCum);
            if (leaves.signum() < 0) {
                leaves = BigDecimal.ZERO;
            }
            var cmd = new ExecutionTradeCommand(orderId, venue, ts, ref, chunk, px, leaves, newCum);
            applier.applyTrade(cmd);
        }
    }

    static List<BigDecimal> splitInThirds(BigDecimal quantity) {
        List<BigDecimal> out = new ArrayList<>(3);
        if (quantity.compareTo(BigDecimal.ZERO) <= 0) {
            return out;
        }
        if (quantity.compareTo(BigDecimal.valueOf(3)) < 0) {
            out.add(quantity.setScale(QTY_SCALE, RoundingMode.HALF_UP));
            return out;
        }
        BigDecimal third = quantity.divide(BigDecimal.valueOf(3), QTY_SCALE, RoundingMode.DOWN);
        BigDecimal remainder = quantity.subtract(third).subtract(third).setScale(QTY_SCALE, RoundingMode.DOWN);
        out.add(third);
        out.add(third);
        out.add(remainder);
        return out;
    }
}
