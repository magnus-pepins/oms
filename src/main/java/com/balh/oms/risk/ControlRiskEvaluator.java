package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.marketdata.MarketdataInstrumentsCache;
import com.balh.oms.persistence.ControlRuntimeFlagsRepository;
import com.balh.oms.persistence.FixRouteStateRepository;
import com.balh.oms.persistence.FixRouteStateRow;
import com.balh.oms.persistence.PositionsRepository;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pre-trade checks that run in {@link com.balh.oms.tailer.ControlTailer}
 * before Ledger buying-power admission (when enabled) and before WORKING CAS.
 *
 * <p>All thresholds are bound from {@link OmsConfig#getRisk()} — no bare numeric
 * literals in business logic (ecosystem config rule). Tradable-instrument CSV is
 * parsed into a set only when the configured string changes (slice 5 cache placeholder).
 */
@Component
public class ControlRiskEvaluator {

    public static final String STAGE_CONTROL = "CONTROL";

    private final OmsConfig omsConfig;
    private final ControlRuntimeFlagsRepository runtimeFlags;
    private final MarketdataInstrumentsCache marketdataInstrumentsCache;
    private final SanctionsExecutionGate sanctionsExecutionGate;
    private final PositionsRepository positionsRepository;
    private final FixRouteStateRepository fixRouteStateRepository;

    private final AtomicReference<Set<String>> cachedTradable = new AtomicReference<>(Set.of());
    private final AtomicReference<String> cachedTradableSource = new AtomicReference<>("");
    private final Map<UUID, Instant> lastOrderAcceptedAt = new ConcurrentHashMap<>();

    public ControlRiskEvaluator(
            OmsConfig omsConfig,
            ControlRuntimeFlagsRepository runtimeFlags,
            MarketdataInstrumentsCache marketdataInstrumentsCache,
            SanctionsExecutionGate sanctionsExecutionGate,
            PositionsRepository positionsRepository,
            FixRouteStateRepository fixRouteStateRepository) {
        this.omsConfig = omsConfig;
        this.runtimeFlags = runtimeFlags;
        this.marketdataInstrumentsCache = marketdataInstrumentsCache;
        this.sanctionsExecutionGate = sanctionsExecutionGate;
        this.positionsRepository = positionsRepository;
        this.fixRouteStateRepository = fixRouteStateRepository;
    }

    /**
     * @return empty if all checks pass; otherwise the first failing {@link RejectCode}.
     */
    public Optional<RejectCode> evaluate(Order order) {
        if (runtimeFlags.isGlobalHalt()) {
            return Optional.of(RejectCode.RISK_KILL_SWITCH);
        }
        var risk = omsConfig.getRisk();
        Optional<RejectCode> sanctions = sanctionsExecutionGate.evaluate(order.accountId());
        if (sanctions.isPresent()) {
            return sanctions;
        }
        if (risk.isStpGateEnabled() && risk.isStpGateRejectAll()) {
            return Optional.of(RejectCode.RISK_STP_GATE);
        }
        if (risk.isFixRouteSendEnabledCheckEnabled()) {
            String backend = omsConfig.getRouting().getBackend();
            if (backend != null && "fix".equalsIgnoreCase(backend.trim())) {
                String routeKey = omsConfig.getFix().getRouteKey();
                boolean sendEnabled =
                        fixRouteStateRepository
                                .findByRouteKey(routeKey)
                                .map(FixRouteStateRow::sendEnabled)
                                .orElse(true);
                if (!sendEnabled) {
                    return Optional.of(RejectCode.RISK_MARKET_SESSION_CLOSED);
                }
            }
        }
        long minGapMs = risk.getOrderMinIntervalMsPerAccount();
        if (minGapMs > 0) {
            Instant now = Instant.now();
            Instant prev = lastOrderAcceptedAt.get(order.accountId());
            if (prev != null && now.toEpochMilli() - prev.toEpochMilli() < minGapMs) {
                return Optional.of(RejectCode.RISK_RATE_LIMIT);
            }
            lastOrderAcceptedAt.put(order.accountId(), now);
        }
        if (risk.isTickSizeCheckEnabled()) {
            BigDecimal inc = risk.getTickSizeIncrement();
            if (order.limitPrice() != null && inc.compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal rem = order.limitPrice().remainder(inc);
                if (rem.compareTo(BigDecimal.ZERO) != 0) {
                    return Optional.of(RejectCode.RISK_TICK_SIZE_VIOLATION);
                }
            }
        }
        if (risk.isInstrumentSymbolHaltCheckEnabled()) {
            Set<String> halted = risk.haltedInstrumentSymbolSet();
            if (!halted.isEmpty()) {
                String hsym = normalizedSymbol(order.instrumentSymbol());
                if (!hsym.isEmpty() && halted.contains(hsym)) {
                    return Optional.of(RejectCode.RISK_SYMBOL_HALT);
                }
            }
        }
        if (risk.isInstrumentAllowlistEnabled()) {
            Set<String> allowed = risk.allowedInstrumentSymbolSet();
            String sym = normalizedSymbol(order.instrumentSymbol());
            if (sym.isEmpty() || !allowed.contains(sym)) {
                return Optional.of(RejectCode.RISK_INVALID_INSTRUMENT);
            }
        }
        if (risk.isInstrumentTradabilityCheckEnabled()) {
            Set<String> tradable = resolveTradableSymbols(risk);
            String sym = normalizedSymbol(order.instrumentSymbol());
            if (sym.isEmpty() || !tradable.contains(sym)) {
                return Optional.of(RejectCode.RISK_INSTRUMENT_NOT_ALLOWED);
            }
        }
        BigDecimal maxLimitPx = risk.getFatFingerMaxLimitPrice();
        if (maxLimitPx.compareTo(BigDecimal.ZERO) > 0
                && order.limitPrice() != null
                && order.limitPrice().compareTo(maxLimitPx) > 0) {
            return Optional.of(RejectCode.RISK_FAT_FINGER_PRICE);
        }
        BigDecimal maxQty = risk.getFatFingerMaxOrderQuantity();
        if (maxQty.compareTo(BigDecimal.ZERO) > 0
                && order.quantity() != null
                && order.quantity().compareTo(maxQty) > 0) {
            return Optional.of(RejectCode.RISK_FAT_FINGER_SIZE);
        }
        BigDecimal maxNotional = risk.getMaxOrderNotional();
        if (maxNotional.compareTo(BigDecimal.ZERO) > 0 && order.quantity() != null) {
            Optional<BigDecimal> notional =
                    order.side() == Side.BUY
                            ? BuyFundsRequirement.buyReferencePrice(order)
                                    .map(px -> order.quantity().multiply(px))
                            : order.limitPrice() != null
                                    ? Optional.of(order.quantity().multiply(order.limitPrice()))
                                    : Optional.empty();
            if (notional.isPresent() && notional.get().compareTo(maxNotional) > 0) {
                return Optional.of(RejectCode.RISK_NOTIONAL_CAP);
            }
        }
        if (risk.isSellPositionCheckEnabled()
                && order.side() == Side.SELL
                && order.quantity() != null
                && order.quantity().signum() > 0) {
            UUID custody = UUID.fromString(omsConfig.getSettlement().getDefaultCustodyAccountId());
            BigDecimal held =
                    positionsRepository.findQuantityTotal(
                            order.accountId(), order.instrumentSymbol(), custody);
            if (held.compareTo(order.quantity()) < 0) {
                return Optional.of(RejectCode.RISK_INSUFFICIENT_POSITION);
            }
        }
        if (risk.isMaxAggregatePositionQuantityCheckEnabled()
                && order.side() == Side.BUY
                && order.quantity() != null) {
            BigDecimal maxAgg = risk.getMaxAggregatePositionQuantity();
            if (maxAgg.compareTo(BigDecimal.ZERO) > 0) {
                UUID custody = UUID.fromString(omsConfig.getSettlement().getDefaultCustodyAccountId());
                BigDecimal current =
                        positionsRepository.findQuantityTotal(order.accountId(), order.instrumentSymbol(), custody);
                BigDecimal next = current.add(order.quantity());
                if (next.compareTo(maxAgg) > 0) {
                    return Optional.of(RejectCode.RISK_CONCENTRATION_LIMIT);
                }
            }
        }
        return Optional.empty();
    }

    private static String normalizedSymbol(String raw) {
        if (raw == null) {
            return "";
        }
        return raw.trim().toUpperCase(Locale.ROOT);
    }

    private Set<String> resolveTradableSymbols(OmsConfig.Risk risk) {
        if (risk.isInstrumentTradabilityFromMarketdataEnabled() && omsConfig.getMarketdata().isEnabled()) {
            Set<String> mp = marketdataInstrumentsCache.getSymbols();
            if (!mp.isEmpty()) {
                return mp;
            }
        }
        return tradableSetCoalesced(risk);
    }

    /**
     * Re-parses the tradable CSV only when the configured string changes — read-through view of config (slice 5
     * instruments cache placeholder until Marketdata Platform wiring).
     */
    private Set<String> tradableSetCoalesced(OmsConfig.Risk risk) {
        String src = risk.getTradableInstrumentSymbols() == null ? "" : risk.getTradableInstrumentSymbols();
        if (!risk.isInstrumentTradabilityCheckEnabled() || src.isBlank()) {
            cachedTradable.set(Set.of());
            cachedTradableSource.set(src);
            return Set.of();
        }
        if (src.equals(cachedTradableSource.get())) {
            return cachedTradable.get();
        }
        Set<String> next = risk.tradableInstrumentSymbolSet();
        cachedTradable.set(next);
        cachedTradableSource.set(src);
        return next;
    }
}
