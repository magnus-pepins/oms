package com.balh.oms.risk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.settlement.OmsAccountTaxWrapperRepository;
import com.balh.oms.settlement.OmsAccountTaxWrapperRepository.AccountTaxWrapperRow;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link IskFundingGate}. The matrix is small because the gate is
 * an early-return cascade — each test pins one branch so a regression in a single
 * conditional cannot silently flip a reject into an accept.
 */
class IskFundingGateTest {

    private static final String ISK_BALANCE_ID = "balance-isk-sek-cash";
    private static final String OTHER_BALANCE_ID = "balance-other-currency";

    @Test
    void disabledFlag_passesEverythingThrough() {
        OmsConfig cfg = new OmsConfig();
        // Default off; do not flip.
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        IskFundingGate gate = new IskFundingGate(cfg, wrapper);

        Order order = buy(UUID.randomUUID(), OTHER_BALANCE_ID);
        assertThat(gate.evaluate(order)).isEmpty();
    }

    @Test
    void nullOrder_passes() {
        IskFundingGate gate = enabledGate(mock(OmsAccountTaxWrapperRepository.class));
        assertThat(gate.evaluate(null)).isEmpty();
    }

    @Test
    void orderWithNullAccountId_passes() {
        IskFundingGate gate = enabledGate(mock(OmsAccountTaxWrapperRepository.class));
        Order order = buyWithAccount(null, OTHER_BALANCE_ID);
        assertThat(gate.evaluate(order)).isEmpty();
    }

    @Test
    void sellOnIskAccount_passes_evenWithNonIskBalance() {
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId))
                .thenReturn(Optional.of(iskWrapper(accountId, ISK_BALANCE_ID)));
        IskFundingGate gate = enabledGate(wrapper);

        Order sell = sellWithAccount(accountId, OTHER_BALANCE_ID);
        // SELL generates cash; the wrapper match is not material.
        assertThat(gate.evaluate(sell)).isEmpty();
    }

    @Test
    void noWrapperRow_passes_becauseNotIsk() {
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId)).thenReturn(Optional.empty());
        IskFundingGate gate = enabledGate(wrapper);

        Order order = buyWithAccount(accountId, OTHER_BALANCE_ID);
        assertThat(gate.evaluate(order)).isEmpty();
    }

    @Test
    void wrapperNotIsk_passes() {
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId))
                .thenReturn(
                        Optional.of(
                                new AccountTaxWrapperRow(
                                        accountId,
                                        OmsAccountTaxWrapperRepository.TAX_WRAPPER_INVESTMENT,
                                        null,
                                        OTHER_BALANCE_ID)));
        IskFundingGate gate = enabledGate(wrapper);

        Order order = buyWithAccount(accountId, OTHER_BALANCE_ID);
        assertThat(gate.evaluate(order)).isEmpty();
    }

    @Test
    void iskBuyWithMatchingBalance_passes() {
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId))
                .thenReturn(Optional.of(iskWrapper(accountId, ISK_BALANCE_ID)));
        IskFundingGate gate = enabledGate(wrapper);

        Order order = buyWithAccount(accountId, ISK_BALANCE_ID);
        assertThat(gate.evaluate(order)).isEmpty();
    }

    @Test
    void iskBuyWithDifferentBalance_rejects() {
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId))
                .thenReturn(Optional.of(iskWrapper(accountId, ISK_BALANCE_ID)));
        IskFundingGate gate = enabledGate(wrapper);

        Order order = buyWithAccount(accountId, OTHER_BALANCE_ID);
        assertThat(gate.evaluate(order)).contains(RejectCode.RISK_ISK_FUNDING_MISMATCH);
    }

    @Test
    void iskBuyWithNullOrderBalance_rejects() {
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId))
                .thenReturn(Optional.of(iskWrapper(accountId, ISK_BALANCE_ID)));
        IskFundingGate gate = enabledGate(wrapper);

        Order order = buyWithAccount(accountId, null);
        assertThat(gate.evaluate(order)).contains(RejectCode.RISK_ISK_FUNDING_MISMATCH);
    }

    @Test
    void iskBuyWithBlankOrderBalance_rejects() {
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId))
                .thenReturn(Optional.of(iskWrapper(accountId, ISK_BALANCE_ID)));
        IskFundingGate gate = enabledGate(wrapper);

        Order order = buyWithAccount(accountId, "   ");
        assertThat(gate.evaluate(order)).contains(RejectCode.RISK_ISK_FUNDING_MISMATCH);
    }

    @Test
    void iskWrapperWithNullBalance_rejectsDefensively() {
        // ISK provisioning incomplete: wrapper row exists but ledger_balance_id is null.
        // We cannot prove the order's balance is the right ISK cash — fail loud.
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId))
                .thenReturn(Optional.of(iskWrapper(accountId, null)));
        IskFundingGate gate = enabledGate(wrapper);

        Order order = buyWithAccount(accountId, ISK_BALANCE_ID);
        assertThat(gate.evaluate(order)).contains(RejectCode.RISK_ISK_FUNDING_MISMATCH);
    }

    @Test
    void iskWrapperWithBlankBalance_rejectsDefensively() {
        UUID accountId = UUID.randomUUID();
        OmsAccountTaxWrapperRepository wrapper = mock(OmsAccountTaxWrapperRepository.class);
        when(wrapper.findByAccountId(accountId))
                .thenReturn(Optional.of(iskWrapper(accountId, "")));
        IskFundingGate gate = enabledGate(wrapper);

        Order order = buyWithAccount(accountId, ISK_BALANCE_ID);
        assertThat(gate.evaluate(order)).contains(RejectCode.RISK_ISK_FUNDING_MISMATCH);
    }

    private static IskFundingGate enabledGate(OmsAccountTaxWrapperRepository wrapper) {
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setIskFundingCheckEnabled(true);
        return new IskFundingGate(cfg, wrapper);
    }

    private static AccountTaxWrapperRow iskWrapper(UUID accountId, String balanceId) {
        return new AccountTaxWrapperRow(
                accountId,
                OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK,
                UUID.randomUUID(),
                balanceId);
    }

    private static Order buy(UUID accountId, String ledgerBalanceId) {
        return buyWithAccount(accountId, ledgerBalanceId);
    }

    private static Order buyWithAccount(UUID accountId, String ledgerBalanceId) {
        return order(accountId, Side.BUY, ledgerBalanceId);
    }

    private static Order sellWithAccount(UUID accountId, String ledgerBalanceId) {
        return order(accountId, Side.SELL, ledgerBalanceId);
    }

    private static Order order(UUID accountId, Side side, String ledgerBalanceId) {
        Instant now = Instant.now();
        return new Order(
                UUID.randomUUID(),
                accountId,
                "idem",
                0,
                0,
                OrderStatus.NEW,
                null,
                side,
                "AAPL",
                BigDecimal.ONE,
                new BigDecimal("10.00"),
                "DAY",
                now,
                now,
                null,
                "h",
                ledgerBalanceId,
                BigDecimal.ZERO);
    }
}
