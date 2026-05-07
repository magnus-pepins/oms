package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.Side;
import com.balh.oms.ledger.LedgerBalanceClient;
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

        BuyingPowerAdmission admission = new BuyingPowerAdmission(cfg, ledger);
        Order order = buyOrder("balance_x", new BigDecimal("10"), new BigDecimal("5"));
        assertThat(admission.evaluate(order)).isEqualTo(BuyingPowerAdmission.Outcome.PROCEED);
    }

    @Test
    void buyWithInsufficientBalanceRejects() throws Exception {
        OmsConfig cfg = new OmsConfig();
        cfg.getLedger().setEnabled(true);
        LedgerBalanceClient client = mock(LedgerBalanceClient.class);
        when(client.fetchAvailableBalance("balance_x")).thenReturn(new BigDecimal("10.00"));
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> ledger = mock(ObjectProvider.class);
        when(ledger.getIfAvailable()).thenReturn(client);

        BuyingPowerAdmission admission = new BuyingPowerAdmission(cfg, ledger);
        Order order = buyOrder("balance_x", new BigDecimal("3"), new BigDecimal("5.00"));
        assertThat(admission.evaluate(order)).isEqualTo(BuyingPowerAdmission.Outcome.REJECT_INSUFFICIENT);
    }

    @Test
    void buyWithSufficientBalanceProceeds() throws Exception {
        OmsConfig cfg = new OmsConfig();
        cfg.getLedger().setEnabled(true);
        LedgerBalanceClient client = mock(LedgerBalanceClient.class);
        when(client.fetchAvailableBalance("balance_x")).thenReturn(new BigDecimal("100.00"));
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerBalanceClient> ledger = mock(ObjectProvider.class);
        when(ledger.getIfAvailable()).thenReturn(client);

        BuyingPowerAdmission admission = new BuyingPowerAdmission(cfg, ledger);
        Order order = buyOrder("balance_x", new BigDecimal("3"), new BigDecimal("5.00"));
        assertThat(admission.evaluate(order)).isEqualTo(BuyingPowerAdmission.Outcome.PROCEED);
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
                ledgerBalanceId
        );
    }
}
