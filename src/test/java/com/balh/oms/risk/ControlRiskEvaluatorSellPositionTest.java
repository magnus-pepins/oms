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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ControlRiskEvaluatorSellPositionTest {

    @Test
    void sellExceedingPositionRejects() {
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setSellPositionCheckEnabled(true);
        cfg.getSettlement().setDefaultCustodyAccountId(UUID.randomUUID().toString());

        ControlRuntimeFlagsRepository flags = mock(ControlRuntimeFlagsRepository.class);
        when(flags.isGlobalHalt()).thenReturn(false);
        MarketdataInstrumentsCache cache = mock(MarketdataInstrumentsCache.class);
        PositionsRepository positions = mock(PositionsRepository.class);
        when(positions.findQuantityTotal(any(), eq("AAPL"), any())).thenReturn(new BigDecimal("2"));
        FixRouteStateRepository fixRoutes = mock(FixRouteStateRepository.class);

        ControlRiskEvaluator evaluator =
                new ControlRiskEvaluator(
                        cfg,
                        flags,
                        cache,
                        new SanctionsExecutionGate(cfg),
                        positions,
                        fixRoutes,
                        disabledIskGate(cfg));

        Instant now = Instant.now();
        Order sell =
                new Order(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        "k",
                        0,
                        0,
                        OrderStatus.NEW,
                        null,
                        Side.SELL,
                        "AAPL",
                        new BigDecimal("5"),
                        new BigDecimal("10"),
                        "DAY",
                        now,
                        now,
                        null,
                        "hash",
                        null,
                        BigDecimal.ZERO);

        Optional<RejectCode> code = evaluator.evaluate(sell);
        assertThat(code).contains(RejectCode.RISK_INSUFFICIENT_POSITION);
    }

    private static IskInstrumentEligibilityGate disabledIskGate(OmsConfig cfg) {
        return new IskInstrumentEligibilityGate(
                cfg,
                mock(com.balh.oms.settlement.OmsAccountTaxWrapperRepository.class),
                mock(com.balh.oms.settlement.InstrumentSettlementProfileRepository.class));
    }
}
