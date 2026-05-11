package com.balh.oms.returnpath;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.events.DomainEventEnvelopeCodec;
import com.balh.oms.marketdata.MarketdataNbboQuote;
import com.balh.oms.marketdata.MarketdataPlatformHttpClient;
import com.balh.oms.observability.otel.IngressToFixNosLatencyLimits;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.ExecutionsRepository;
import com.balh.oms.persistence.MarketContextRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.balh.oms.persistence.SellFillPositionSplit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Applies venue execution reports to {@code orders} and {@code executions} (slice 3).
 *
 * <p>Idempotent on {@code (account_id, venue_exec_ref)} via insert-then-CAS pattern.
 */
@Service
public class ExecutionReportApplier {

    private static final Logger log = LoggerFactory.getLogger(ExecutionReportApplier.class);

    private static final String METRIC_EXECUTIONS_APPLIED = "oms_executions_applied_total";
    private static final String TAG_OUTCOME = "outcome";
    private static final String OUTCOME_INSERTED = "inserted";
    private static final String OUTCOME_DUPLICATE = "duplicate";

    private static final String METRIC_ORDER_FILLED_EVENTS = "oms_order_filled_events_published_total";
    private static final String METRIC_FREE_RIDING_ATTRIBUTION = "oms_free_riding_attribution_merges_total";
    /** Trade ER apply latency including market_context merge (best-ex evidence path). */
    private static final String TIMER_TRADE_APPLY = "oms.trade.apply";
    /** HTTP NBBO fetch latency when marketdata integration is active. */
    private static final String TIMER_NBBO_FETCH = "oms.marketdata.nbbo.fetch";

    private static final int PRICE_SCALE = 10;

    private final OrdersRepository orders;
    private final ExecutionsRepository executions;
    private final MarketContextRepository marketContext;
    private final DomainEventOutboxRepository domainEventOutbox;
    private final DomainEventEnvelopeCodec envelopeCodec;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final MeterRegistry meterRegistry;
    private final PositionsRepository positions;
    private final ObjectProvider<MarketdataPlatformHttpClient> marketdataHttp;
    private final IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder;

    public ExecutionReportApplier(
            OrdersRepository orders,
            ExecutionsRepository executions,
            MarketContextRepository marketContext,
            DomainEventOutboxRepository domainEventOutbox,
            DomainEventEnvelopeCodec envelopeCodec,
            ObjectMapper objectMapper,
            OmsConfig config,
            MeterRegistry meterRegistry,
            PositionsRepository positions,
            ObjectProvider<MarketdataPlatformHttpClient> marketdataHttp,
            IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder) {
        this.orders = orders;
        this.executions = executions;
        this.marketContext = marketContext;
        this.domainEventOutbox = domainEventOutbox;
        this.envelopeCodec = envelopeCodec;
        this.objectMapper = objectMapper;
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.positions = positions;
        this.marketdataHttp = marketdataHttp;
        this.ingressToFixNosLatencyRecorder = ingressToFixNosLatencyRecorder;
    }

    public enum TradeApplyOutcome {
        APPLIED,
        DUPLICATE,
        INVALID_STATE,
        UNKNOWN_ORDER,
        VERSION_MISMATCH
    }

    public enum CancelApplyOutcome {
        APPLIED,
        DUPLICATE,
        INVALID_STATE,
        UNKNOWN_ORDER,
        VERSION_MISMATCH
    }

    public enum VenueRejectApplyOutcome {
        APPLIED,
        DUPLICATE,
        INVALID_STATE,
        UNKNOWN_ORDER,
        VERSION_MISMATCH
    }

    @Transactional
    public TradeApplyOutcome applyTrade(ExecutionTradeCommand cmd) {
        Optional<Order> opt = orders.findById(cmd.orderId());
        if (opt.isEmpty()) {
            return TradeApplyOutcome.UNKNOWN_ORDER;
        }
        Order order = opt.get();
        if (!isOpenForTrade(order.status())) {
            log.warn("Ignoring trade ER for order {} in status {}", cmd.orderId(), order.status());
            return TradeApplyOutcome.INVALID_STATE;
        }
        return meterRegistry.timer(TIMER_TRADE_APPLY).record(() -> applyTradeAfterPreChecks(order, cmd));
    }

    private TradeApplyOutcome applyTradeAfterPreChecks(Order order, ExecutionTradeCommand cmd) {
        try {
            Optional<MarketContextVenueEvidence.NbboQuoteRef> nbbo = resolveNbbo(order, cmd);
            String patch = MarketContextVenueEvidence.toJsonPatch(objectMapper, order, cmd, nbbo);
            marketContext.mergeVenueFillEvidence(
                    order.id(),
                    cmd.venueTs(),
                    config.getRouting().getMarketContextStubJson(),
                    patch);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("market_context evidence serialisation failed", e);
        }

        String raw = rawTradeJson(cmd);
        Optional<Long> insertedId = executions.tryInsertTrade(
                order.id(),
                order.accountId(),
                cmd.venueId(),
                cmd.venueTs(),
                cmd.venueExecRef(),
                cmd.lastQuantity(),
                cmd.lastPrice(),
                cmd.leavesQuantity(),
                cmd.cumQuantityAfter(),
                raw);
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return TradeApplyOutcome.DUPLICATE;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();

        if (config.getSettlement().isFreeRidingAttributionEnabled() && order.side() == Side.BUY) {
            List<Long> funding =
                    executions.findUnsettledBuyTradeExecutionIdsForAttribution(
                            order.accountId(),
                            order.instrumentSymbol(),
                            insertedId.get(),
                            config.getSettlement().getFreeRidingAttributionMaxFundingExecutions());
            if (!funding.isEmpty()) {
                executions.appendUnsettledFundedByExecutionIds(insertedId.get(), funding);
                meterRegistry.counter(METRIC_FREE_RIDING_ATTRIBUTION).increment();
            }
        }

        UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
        Optional<SellFillPositionSplit> sellSplit =
                positions.recordTradeFill(order, insertedId.get(), cmd.lastQuantity(), custody);
        sellSplit.ifPresent(split -> executions.updateSellFillPositionSplit(insertedId.get(), split));

        BigDecimal newCum = order.cumFilledQuantity().add(cmd.lastQuantity());
        if (newCum.compareTo(order.quantity()) > 0) {
            throw new IllegalStateException("fill overflow order=%s newCum=%s orderQty=%s"
                    .formatted(order.id(), newCum, order.quantity()));
        }
        boolean terminal = newCum.compareTo(order.quantity()) >= 0;
        OrderStatus newStatus = terminal ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
        Instant terminalAt = terminal ? Instant.now() : null;

        boolean cas = orders.updateFillOrCancelWithCas(
                order.id(), order.version(), newCum, newStatus, null, terminalAt);
        if (!cas) {
            return TradeApplyOutcome.VERSION_MISMATCH;
        }
        int newSeq = order.version() + 1;
        Order refreshed = orders.findById(order.id()).orElse(order);
        try {
            if (newStatus == OrderStatus.PARTIALLY_FILLED) {
                domainEventOutbox.insert(
                        order.id(),
                        envelopeCodec.orderPartiallyFilled(
                                refreshed,
                                newSeq,
                                newCum,
                                cmd.lastQuantity(),
                                cmd.lastPrice(),
                                cmd.venueId(),
                                cmd.venueExecRef()));
            } else {
                BigDecimal vwap = executions.weightedAverageTradePrice(order.id());
                domainEventOutbox.insert(
                        order.id(),
                        envelopeCodec.orderFilled(
                                refreshed,
                                newSeq,
                                newCum,
                                vwap,
                                cmd.venueId(),
                                cmd.venueExecRef()));
                meterRegistry.counter(METRIC_ORDER_FILLED_EVENTS).increment();
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("domain event serialisation failed", e);
        }
        return TradeApplyOutcome.APPLIED;
    }

    @Transactional
    public CancelApplyOutcome applyCancel(ExecutionCancelCommand cmd) {
        Optional<Order> opt = orders.findById(cmd.orderId());
        if (opt.isEmpty()) {
            return CancelApplyOutcome.UNKNOWN_ORDER;
        }
        Order order = opt.get();
        if (!isOpenForTrade(order.status())) {
            log.warn("Ignoring cancel ER for order {} in status {}", cmd.orderId(), order.status());
            return CancelApplyOutcome.INVALID_STATE;
        }

        String raw = rawCancelJson(cmd);
        Optional<Long> insertedId = executions.tryInsertCancel(
                order.id(),
                order.accountId(),
                cmd.venueId(),
                cmd.venueTs(),
                cmd.venueExecRef(),
                order.cumFilledQuantity(),
                raw);
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return CancelApplyOutcome.DUPLICATE;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();

        boolean cas = orders.updateFillOrCancelWithCas(
                order.id(),
                order.version(),
                order.cumFilledQuantity(),
                OrderStatus.CANCELLED,
                null,
                Instant.now());
        if (!cas) {
            return CancelApplyOutcome.VERSION_MISMATCH;
        }
        int newSeq = order.version() + 1;
        Order refreshed = orders.findById(order.id()).orElse(order);
        try {
            domainEventOutbox.insert(
                    order.id(),
                    envelopeCodec.orderCancelled(refreshed, newSeq, cmd.venueId(), cmd.venueExecRef()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("domain event serialisation failed", e);
        }
        return CancelApplyOutcome.APPLIED;
    }

    /**
     * Applies a venue/broker reject: {@code executions} REJECT row (idempotent) + CAS to {@code REJECTED} +
     * {@code OrderRejected} outbox row.
     */
    @Transactional
    public VenueRejectApplyOutcome applyVenueReject(ExecutionVenueRejectCommand cmd, RejectCode terminalReason) {
        Optional<Order> opt = orders.findById(cmd.orderId());
        if (opt.isEmpty()) {
            return VenueRejectApplyOutcome.UNKNOWN_ORDER;
        }
        Order order = opt.get();
        if (!isOpenForTrade(order.status())) {
            log.warn("Ignoring venue reject for order {} in status {}", cmd.orderId(), order.status());
            return VenueRejectApplyOutcome.INVALID_STATE;
        }

        marketContext.ensureStubSnapshot(order.id(), config.getRouting().getMarketContextStubJson());

        Optional<Long> insertedId = executions.tryInsertVenueReject(
                order.id(),
                order.accountId(),
                cmd.venueId(),
                cmd.venueTs(),
                cmd.venueExecRef(),
                order.cumFilledQuantity(),
                cmd.rawEnvelopeJson());
        if (insertedId.isEmpty()) {
            meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_DUPLICATE).increment();
            return VenueRejectApplyOutcome.DUPLICATE;
        }
        meterRegistry.counter(METRIC_EXECUTIONS_APPLIED, TAG_OUTCOME, OUTCOME_INSERTED).increment();

        boolean cas = orders.updateWithCas(
                order.id(), order.version(), OrderStatus.REJECTED, terminalReason, null, Instant.now());
        if (!cas) {
            return VenueRejectApplyOutcome.VERSION_MISMATCH;
        }
        int newSeq = order.version() + 1;
        Order refreshed = orders.findById(order.id()).orElse(order);
        try {
            domainEventOutbox.insert(
                    order.id(), envelopeCodec.orderRejectedAfterVenue(refreshed, terminalReason, newSeq));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("domain event serialisation failed", e);
        }
        return VenueRejectApplyOutcome.APPLIED;
    }

    /**
     * Marks a still-open order rejected when it sat too long on the FIX outbound queue (slice 4).
     */
    @Transactional
    public VenueRejectApplyOutcome applyOutboundJobExpired(UUID orderId) {
        ingressToFixNosLatencyRecorder.discard(orderId, IngressToFixNosLatencyLimits.REASON_OUTBOUND_EXPIRED);
        String venueId = config.getFix().getVenueIdForExecutions();
        Instant ts = Instant.now();
        String ref = "OMS-OUTBOUND-EXPIRED-" + orderId;
        String raw = "{\"kind\":\"OutboundJobExpired\",\"orderId\":\"" + orderId + "\"}";
        return applyVenueReject(
                new ExecutionVenueRejectCommand(orderId, venueId, ts, ref, raw), RejectCode.FIX_OUTBOUND_JOB_EXPIRED);
    }

    private boolean isOpenForTrade(OrderStatus status) {
        return status == OrderStatus.WORKING || status == OrderStatus.PARTIALLY_FILLED;
    }

    private Optional<MarketContextVenueEvidence.NbboQuoteRef> resolveNbbo(Order order, ExecutionTradeCommand cmd) {
        var r = config.getRouting();
        if (!r.isNbboReferenceInMarketContextEnabled()) {
            return Optional.empty();
        }
        var md = config.getMarketdata();
        if (md.isNbboInMarketContextEnabled() && md.isEnabled()) {
            MarketdataPlatformHttpClient c = marketdataHttp.getIfAvailable();
            if (c != null) {
                Optional<MarketdataNbboQuote> live =
                        meterRegistry
                                .timer(TIMER_NBBO_FETCH, "source", "http")
                                .record(() -> c.fetchNbbo(order.instrumentSymbol()));
                if (live.isPresent()) {
                    MarketdataNbboQuote q = live.get();
                    return Optional.of(
                            new MarketContextVenueEvidence.NbboQuoteRef(
                                    q.bid(), q.ask(), q.asOf(), "NBBO_MARKETDATA_HTTP"));
                }
            }
        }
        BigDecimal bid = r.getNbboStubBidPrice();
        BigDecimal ask = r.getNbboStubAskPrice();
        if (bid.compareTo(BigDecimal.ZERO) <= 0 || ask.compareTo(BigDecimal.ZERO) <= 0) {
            return Optional.empty();
        }
        return Optional.of(new MarketContextVenueEvidence.NbboQuoteRef(bid, ask, cmd.venueTs()));
    }

    private String rawTradeJson(ExecutionTradeCommand cmd) {
        ObjectNode n = objectMapper.createObjectNode();
        n.put("kind", "ExecutionReport");
        n.put("venueId", cmd.venueId());
        n.put("venueExecRef", cmd.venueExecRef());
        n.put("lastQuantity", cmd.lastQuantity().setScale(PRICE_SCALE, RoundingMode.HALF_UP).toPlainString());
        if (cmd.lastPrice() != null) {
            n.put("lastPrice", cmd.lastPrice().setScale(PRICE_SCALE, RoundingMode.HALF_UP).toPlainString());
        }
        try {
            return objectMapper.writeValueAsString(n);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private String rawCancelJson(ExecutionCancelCommand cmd) {
        ObjectNode n = objectMapper.createObjectNode();
        n.put("kind", "ExecutionReport");
        n.put("execType", "CANCEL");
        n.put("venueId", cmd.venueId());
        n.put("venueExecRef", cmd.venueExecRef());
        try {
            return objectMapper.writeValueAsString(n);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
