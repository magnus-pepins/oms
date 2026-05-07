package com.balh.oms.routing;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.returnpath.ExecutionReportApplier;
import com.balh.oms.returnpath.ExecutionTradeCommand;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Synthetic venue fills: three ER-shaped applies (⅓, ⅓, remainder) → two
 * {@code OrderPartiallyFilled} domain events then {@code OrderFilled} for typical sizes.
 */
public final class SimulatedExecutionProgram {

    private static final int QTY_SCALE = 10;

    private final OmsConfig config;
    private final OrdersRepository orders;
    private final ExecutionReportApplier applier;

    public SimulatedExecutionProgram(OmsConfig config, OrdersRepository orders, ExecutionReportApplier applier) {
        this.config = config;
        this.orders = orders;
        this.applier = applier;
    }

    /**
     * Runs the programmed partial → partial → final fill sequence for one order id.
     */
    public void runProgrammedFills(UUID orderId) {
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

    /** Visible for tests: split qty into three fills (two equal partials + final remainder). */
    public static List<BigDecimal> splitInThirds(BigDecimal quantity) {
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
