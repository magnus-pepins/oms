package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.marketdata.MarketdataInstrumentsCache;
import com.balh.oms.persistence.ControlRuntimeFlagsRepository;
import com.balh.oms.persistence.FixRouteStateRepository;
import com.balh.oms.persistence.PositionsRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ControlRiskEvaluatorTradabilityTest {

    @Test
    void tradabilityEnabled_rejectsSymbolNotInList() {
        ControlRuntimeFlagsRepository flags = mock(ControlRuntimeFlagsRepository.class);
        when(flags.isGlobalHalt()).thenReturn(false);
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setInstrumentTradabilityCheckEnabled(true);
        cfg.getRisk().setTradableInstrumentSymbols("AAPL");
        var ev = evaluator(cfg, flags, emptyMarketdataCache());

        Order order = sampleOrder("MSFT");

        assertThat(ev.evaluate(order)).contains(RejectCode.RISK_INSTRUMENT_NOT_ALLOWED);
    }

    @Test
    void tradabilityEnabled_acceptsListedSymbol() {
        ControlRuntimeFlagsRepository flags = mock(ControlRuntimeFlagsRepository.class);
        when(flags.isGlobalHalt()).thenReturn(false);
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setInstrumentTradabilityCheckEnabled(true);
        cfg.getRisk().setTradableInstrumentSymbols("AAPL, msft ");
        var ev = evaluator(cfg, flags, emptyMarketdataCache());

        assertThat(ev.evaluate(sampleOrder("MSFT"))).isEmpty();
    }

    @Test
    void symbolHaltEnabled_rejectsHaltedSymbol() {
        ControlRuntimeFlagsRepository flags = mock(ControlRuntimeFlagsRepository.class);
        when(flags.isGlobalHalt()).thenReturn(false);
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setInstrumentSymbolHaltCheckEnabled(true);
        cfg.getRisk().setHaltedInstrumentSymbols("GME");
        var ev = evaluator(cfg, flags, emptyMarketdataCache());
        assertThat(ev.evaluate(sampleOrder("GME"))).contains(RejectCode.RISK_SYMBOL_HALT);
        assertThat(ev.evaluate(sampleOrder("AAPL"))).isEmpty();
    }

    @Test
    void tradability_prefersMarketdataSetWhenFlaggedAndNonEmpty() {
        ControlRuntimeFlagsRepository flags = mock(ControlRuntimeFlagsRepository.class);
        when(flags.isGlobalHalt()).thenReturn(false);
        OmsConfig cfg = new OmsConfig();
        cfg.getMarketdata().setEnabled(true);
        cfg.getRisk().setInstrumentTradabilityCheckEnabled(true);
        cfg.getRisk().setInstrumentTradabilityFromMarketdataEnabled(true);
        cfg.getRisk().setTradableInstrumentSymbols("AAPL,MSFT");
        MarketdataInstrumentsCache cache = mock(MarketdataInstrumentsCache.class);
        when(cache.getSymbols()).thenReturn(Set.of("AAPL"));
        var ev = evaluator(cfg, flags, cache);
        assertThat(ev.evaluate(sampleOrder("AAPL"))).isEmpty();
        assertThat(ev.evaluate(sampleOrder("MSFT"))).contains(RejectCode.RISK_INSTRUMENT_NOT_ALLOWED);
    }

    private static ControlRiskEvaluator evaluator(
            OmsConfig cfg, ControlRuntimeFlagsRepository flags, MarketdataInstrumentsCache cache) {
        PositionsRepository positions = mock(PositionsRepository.class);
        when(positions.findQuantityTotal(any(), anyString(), any())).thenReturn(BigDecimal.ZERO);
        FixRouteStateRepository fixRoutes = mock(FixRouteStateRepository.class);
        when(fixRoutes.findByRouteKey(anyString())).thenReturn(Optional.empty());
        return new ControlRiskEvaluator(cfg, flags, cache, new SanctionsExecutionGate(cfg), positions, fixRoutes);
    }

    private static MarketdataInstrumentsCache emptyMarketdataCache() {
        MarketdataInstrumentsCache c = mock(MarketdataInstrumentsCache.class);
        when(c.getSymbols()).thenReturn(Set.of());
        return c;
    }

    private static Order sampleOrder(String symbol) {
        Instant now = Instant.now();
        return new Order(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "idem",
                0,
                0,
                OrderStatus.NEW,
                null,
                Side.BUY,
                symbol,
                BigDecimal.ONE,
                new BigDecimal("10.00"),
                "DAY",
                now,
                now,
                null,
                "h",
                null,
                BigDecimal.ZERO);
    }
}
