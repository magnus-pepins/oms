package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.ledger.LedgerBalanceClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BuyingPowerAdmissionTest {

    @Test
    void ledgerDisabledAlwaysProceeds() {
        OmsConfig cfg = new OmsConfig();
        cfg.getLedger().setEnabled(false);
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> ledger = mock(ObjectProvider.class);
        when(ledger.getIfAvailable()).thenReturn(null);

        BuyingPowerAdmission admission = new BuyingPowerAdmission(cfg, ledger, new SimpleMeterRegistry());
        Order order = buyOrder("balance_x", new BigDecimal("10"), new BigDecimal("5"));
        assertThat(admission.evaluate(order)).isEqualTo(BuyingPowerAdmission.Outcome.PROCEED);
    }

    @Test
    void buyWithInsufficientBalanceRejects() throws Exception {
        OmsConfig cfg = new OmsConfig();
        cfg.getLedger().setEnabled(true);
        LedgerBalanceClient client = mock(LedgerBalanceClient.class);
        // 3 × 5.00 notional = 15.00; US default fee min $1 → required 16.00
        when(client.fetchAvailableBalance("balance_x")).thenReturn(new BigDecimal("15.99"));
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> ledger = mock(ObjectProvider.class);
        when(ledger.getIfAvailable()).thenReturn(client);

        BuyingPowerAdmission admission = new BuyingPowerAdmission(cfg, ledger, new SimpleMeterRegistry());
        Order order = buyOrder("balance_x", new BigDecimal("3"), new BigDecimal("5.00"));
        assertThat(admission.evaluate(order)).isEqualTo(BuyingPowerAdmission.Outcome.REJECT_INSUFFICIENT);
    }

    @Test
    void buyWithSufficientBalanceProceeds() throws Exception {
        OmsConfig cfg = new OmsConfig();
        cfg.getLedger().setEnabled(true);
        LedgerBalanceClient client = mock(LedgerBalanceClient.class);
        when(client.fetchAvailableBalance("balance_x")).thenReturn(new BigDecimal("110.00"));
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> ledger = mock(ObjectProvider.class);
        when(ledger.getIfAvailable()).thenReturn(client);

        BuyingPowerAdmission admission = new BuyingPowerAdmission(cfg, ledger, new SimpleMeterRegistry());
        Order order = buyOrder("balance_x", new BigDecimal("3"), new BigDecimal("5.00"));
        assertThat(admission.evaluate(order)).isEqualTo(BuyingPowerAdmission.Outcome.PROCEED);
    }

    @Test
    void sellSkipsLedgerCheckAndIncrementsSkipMetric() {
        OmsConfig cfg = new OmsConfig();
        cfg.getLedger().setEnabled(true);
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> ledger = mock(ObjectProvider.class);
        when(ledger.getIfAvailable()).thenReturn(mock(LedgerBalanceClient.class));
        var registry = new SimpleMeterRegistry();
        BuyingPowerAdmission admission = new BuyingPowerAdmission(cfg, ledger, registry);
        Instant now = Instant.now();
        Order sell = new Order(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "k",
                0,
                0,
                OrderStatus.NEW,
                null,
                Side.SELL,
                "AAPL",
                new BigDecimal("1"),
                new BigDecimal("10"),
                "DAY",
                now,
                now,
                null,
                "hash",
                "balance_x",
                BigDecimal.ZERO);
        assertThat(admission.evaluate(sell)).isEqualTo(BuyingPowerAdmission.Outcome.PROCEED);
        assertThat(registry.counter("oms_buying_power_sell_ledger_skip_total").count()).isEqualTo(1.0);
    }

    @Test
    void buyWithoutFundingPriceRejects() {
        OmsConfig cfg = new OmsConfig();
        cfg.getLedger().setEnabled(true);
        LedgerBalanceClient client = mock(LedgerBalanceClient.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> ledger = mock(ObjectProvider.class);
        when(ledger.getIfAvailable()).thenReturn(client);

        BuyingPowerAdmission admission = new BuyingPowerAdmission(cfg, ledger, new SimpleMeterRegistry());
        Order order = buyOrder("balance_x", new BigDecimal("1"), null);
        assertThat(admission.evaluate(order)).isEqualTo(BuyingPowerAdmission.Outcome.REJECT_INSUFFICIENT);
    }

    private static Order buyOrder(String ledgerBalanceId, BigDecimal qty, BigDecimal limit) {
        Instant now = Instant.now();
        return new Order(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "k",
                0,
                0,
                OrderStatus.NEW,
                null,
                Side.BUY,
                "AAPL",
                qty,
                limit,
                "DAY",
                now,
                now,
                null,
                "hash",
                ledgerBalanceId,
                BigDecimal.ZERO
        );
    }
}
