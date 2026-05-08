package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.persistence.ControlRuntimeFlagsRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
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
        var ev = new ControlRiskEvaluator(cfg, flags);

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
        var ev = new ControlRiskEvaluator(cfg, flags);

        assertThat(ev.evaluate(sampleOrder("MSFT"))).isEmpty();
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
