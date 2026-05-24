package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.settlement.InstrumentSettlementProfile;
import com.balh.oms.settlement.InstrumentSettlementProfileRepository;
import com.balh.oms.settlement.OmsAccountTaxWrapperRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IskInstrumentEligibilityGateTest {

    private static final UUID ACCOUNT_ID = UUID.fromString("cccccccc-cccc-4ccc-8ccc-cccccccccccc");

    @Mock private OmsAccountTaxWrapperRepository accountTaxWrapper;
    @Mock private InstrumentSettlementProfileRepository settlementProfiles;

    @Test
    void disabledCheck_passesThrough() {
        OmsConfig cfg = new OmsConfig();
        IskInstrumentEligibilityGate gate = new IskInstrumentEligibilityGate(cfg, accountTaxWrapper, settlementProfiles);
        assertThat(gate.evaluate(sampleOrder("GME"))).isEmpty();
    }

    @Test
    void iskAccount_nonEligibleInstrument_rejects() {
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setIskInstrumentEligibilityCheckEnabled(true);
        when(accountTaxWrapper.findByAccountId(ACCOUNT_ID))
                .thenReturn(
                        Optional.of(
                                new OmsAccountTaxWrapperRepository.AccountTaxWrapperRow(
                                        ACCOUNT_ID, OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK, null, null)));
        when(settlementProfiles.findActiveBySymbol(eq("GME"), any(LocalDate.class)))
                .thenReturn(Optional.of(profile(false)));

        IskInstrumentEligibilityGate gate = new IskInstrumentEligibilityGate(cfg, accountTaxWrapper, settlementProfiles);
        assertThat(gate.evaluate(sampleOrder("GME"))).contains(RejectCode.RISK_ISK_INSTRUMENT_NOT_ELIGIBLE);
    }

    @Test
    void iskAccount_eligibleInstrument_passes() {
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setIskInstrumentEligibilityCheckEnabled(true);
        when(accountTaxWrapper.findByAccountId(ACCOUNT_ID))
                .thenReturn(
                        Optional.of(
                                new OmsAccountTaxWrapperRepository.AccountTaxWrapperRow(
                                        ACCOUNT_ID, OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK, null, null)));
        when(settlementProfiles.findActiveBySymbol(eq("AAPL"), any(LocalDate.class)))
                .thenReturn(Optional.of(profile(true)));

        IskInstrumentEligibilityGate gate = new IskInstrumentEligibilityGate(cfg, accountTaxWrapper, settlementProfiles);
        assertThat(gate.evaluate(sampleOrder("AAPL"))).isEmpty();
    }

    @Test
    void investmentAccount_skipsGate() {
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setIskInstrumentEligibilityCheckEnabled(true);
        when(accountTaxWrapper.findByAccountId(ACCOUNT_ID))
                .thenReturn(
                        Optional.of(
                                new OmsAccountTaxWrapperRepository.AccountTaxWrapperRow(
                                        ACCOUNT_ID,
                                        OmsAccountTaxWrapperRepository.TAX_WRAPPER_INVESTMENT,
                                        null,
                                        null)));

        IskInstrumentEligibilityGate gate = new IskInstrumentEligibilityGate(cfg, accountTaxWrapper, settlementProfiles);
        assertThat(gate.evaluate(sampleOrder("GME"))).isEmpty();
    }

    private static InstrumentSettlementProfile profile(boolean iskEligible) {
        return new InstrumentSettlementProfile(
                1L,
                "inst",
                "SYM",
                null,
                null,
                null,
                "T+2",
                "USD",
                iskEligible,
                LocalDate.of(2020, 1, 1),
                null);
    }

    private static Order sampleOrder(String symbol) {
        Instant now = Instant.parse("2026-05-20T12:00:00Z");
        return new Order(
                UUID.randomUUID(),
                ACCOUNT_ID,
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
