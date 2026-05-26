package com.balh.oms;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.ExecutionAppliedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.balh.oms.settlement.SettlementDateCalculator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Minimal TRADE branch of {@link com.balh.oms.projector.OmsPostgresProjector} for
 * {@link AbstractPostgresIntegrationTest.TestPostgresProjectorSingleton} — enough for prediction-market
 * resolution payout tests (positions after venue fill).
 */
final class TestPostgresProjectorTradeApplier {

    private static final long QUANTITY_SCALE = AcceptOrderCommand.QUANTITY_SCALE;
    private static final long PRICE_SCALE = AcceptOrderCommand.PRICE_SCALE;

    private final OmsConfig config;
    private final OrdersRepository ordersRepository;
    private final ExecutionsRepository executionsRepository;
    private final PositionsRepository positionsRepository;
    private final SettlementDateCalculator settlementDateCalculator;

    TestPostgresProjectorTradeApplier(
            OmsConfig config,
            OrdersRepository ordersRepository,
            ExecutionsRepository executionsRepository,
            PositionsRepository positionsRepository) {
        this.config = config;
        this.ordersRepository = ordersRepository;
        this.executionsRepository = executionsRepository;
        this.positionsRepository = positionsRepository;
        this.settlementDateCalculator = new SettlementDateCalculator("T+2");
    }

    void applyExecutionAppliedEvent(ExecutionAppliedEvent ev) {
        if (ev.execTypeCode() != ApplyExecutionReportCommand.EXEC_TYPE_TRADE) {
            return;
        }
        Optional<Order> opt = ordersRepository.findById(ev.orderId());
        if (opt.isEmpty()) {
            return;
        }
        Order order = opt.get();
        BigDecimal lastQty = scaledToBigDecimal(ev.lastQtyScaled(), QUANTITY_SCALE);
        BigDecimal lastPx = ev.lastPxScaled() == 0L
                ? null
                : scaledToBigDecimal(ev.lastPxScaled(), PRICE_SCALE);
        BigDecimal newCum = scaledToBigDecimal(ev.newCumQtyScaled(), QUANTITY_SCALE);
        BigDecimal leaves = order.quantity().subtract(newCum);

        Instant venueTs = Instant.ofEpochSecond(0L, ev.venueTsNanos());
        var tradeDate = settlementDateCalculator.computeTradeDate(venueTs);
        var expectedSettlementDate =
                settlementDateCalculator.resolveExpectedSettlementDate(tradeDate, order.instrumentSymbol());
        Optional<Long> insertedId = executionsRepository.tryInsertTrade(
                order.id(),
                order.accountId(),
                ev.venueId(),
                venueTs,
                ev.venueExecRef(),
                lastQty,
                lastPx,
                leaves,
                newCum,
                ev.rawEnvelopeJson(),
                tradeDate,
                expectedSettlementDate);
        if (insertedId.isEmpty()) {
            return;
        }
        UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
        positionsRepository.recordTradeFill(order, insertedId.get(), lastQty, custody);

        OrderStatus newStatus = OrderStatus.values()[ev.newStatusCode()];
        Instant terminalAt = newStatus == OrderStatus.FILLED ? Instant.ofEpochMilli(ev.appliedAtMillis()) : null;
        ordersRepository.updateFillOrCancelWithCas(
                order.id(), order.version(), newCum, newStatus, null, terminalAt);
    }

    private static BigDecimal scaledToBigDecimal(long scaled, long scale) {
        return BigDecimal.valueOf(scaled).divide(BigDecimal.valueOf(scale), 10, RoundingMode.UNNECESSARY);
    }
}
