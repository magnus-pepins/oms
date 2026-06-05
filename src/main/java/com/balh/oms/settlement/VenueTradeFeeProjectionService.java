package com.balh.oms.settlement;

import com.balh.oms.cluster.ExecutionAppliedEvent;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.domain.Order;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

/** Applies venue trade fees on TRADE projection (Phase E). */
@Service
public class VenueTradeFeeProjectionService {

    private final VenueFeeCalculator feeCalculator;
    private final FixInCounterpartyLookupRepository counterpartyLookup;
    private final ExecutionsRepository executionsRepository;
    private final OrdersRepository ordersRepository;
    private final ObjectMapper objectMapper;

    public VenueTradeFeeProjectionService(
            VenueFeeCalculator feeCalculator,
            FixInCounterpartyLookupRepository counterpartyLookup,
            ExecutionsRepository executionsRepository,
            OrdersRepository ordersRepository,
            ObjectMapper objectMapper) {
        this.feeCalculator = feeCalculator;
        this.counterpartyLookup = counterpartyLookup;
        this.executionsRepository = executionsRepository;
        this.ordersRepository = ordersRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Pins contract fee model + conservative taker-fee estimate on fresh admit. Resting fills may
     * accrue a lower maker fee at execution time.
     */
    public void pinFeeAtAdmit(OrderAdmittedEvent ev) {
        if (ev.instrumentSymbol() == null || !ev.instrumentSymbol().startsWith("PREDMKT")) {
            return;
        }
        boolean retail = !counterpartyLookup.isFixAccount(java.util.UUID.fromString(ev.accountId()));
        feeCalculator
                .quoteForFill(
                        ev.instrumentSymbol(),
                        java.util.UUID.fromString(ev.accountId()),
                        retail,
                        VenueLiquidityRole.TAKER,
                        ev.quantityScaled(),
                        ev.limitPriceScaledOrZero(),
                        null)
                .ifPresent(
                        quote ->
                                ordersRepository.pinPredictionMarketFeeAtAdmit(
                                        ev.orderId(),
                                        quote.modelId().name(),
                                        quote.scheduleVersion(),
                                        quote.feeAmount(),
                                        quote.feeCurrency()));
    }

    public void applyTradeFee(long executionId, ExecutionAppliedEvent ev, Order order) {
        if (ev.lastQtyScaled() <= 0) {
            return;
        }
        if (order.instrumentSymbol() == null || !order.instrumentSymbol().startsWith("PREDMKT")) {
            return;
        }
        VenueLiquidityRole role = VenueLiquidityRole.fromExecutionEnvelope(ev.rawEnvelopeJson(), objectMapper);
        boolean retail = !counterpartyLookup.isFixAccount(order.accountId());
        feeCalculator
                .quoteForFill(
                        order.instrumentSymbol(),
                        order.accountId(),
                        retail,
                        role,
                        ev.lastQtyScaled(),
                        ev.lastPxScaled(),
                        null)
                .ifPresent(
                        quote ->
                                executionsRepository.updateTradeFee(
                                        executionId,
                                        quote.liquidityRole().name(),
                                        quote.feeAmount(),
                                        quote.feeCurrency(),
                                        quote.modelId().name(),
                                        quote.scheduleVersion()));
    }
}
