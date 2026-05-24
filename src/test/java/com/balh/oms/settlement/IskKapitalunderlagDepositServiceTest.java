package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerIskReadClient;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IskKapitalunderlagDepositServiceTest {

    @Test
    void sumDepositsSek_sumsOnlyKapitalunderlagRows() throws Exception {
        UUID iskId = UUID.randomUUID();
        LedgerIskReadClient client = mock(LedgerIskReadClient.class);
        when(client.listDeposits(eq(iskId.toString()), any(), any()))
                .thenReturn(
                        List.of(
                                new LedgerIskReadClient.DepositRow(
                                        50_000_00L,
                                        "SEK",
                                        "external_cash",
                                        true,
                                        Instant.parse("2026-03-01T10:00:00Z")),
                                new LedgerIskReadClient.DepositRow(
                                        10_000_00L,
                                        "SEK",
                                        "sale_proceeds",
                                        false,
                                        Instant.parse("2026-04-01T10:00:00Z"))));

        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerIskReadClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(client);

        var service = new IskKapitalunderlagDepositService(provider, new OmsConfig());

        assertThat(service.sumDepositsSek(iskId, 2026)).isEqualByComparingTo("50000.00");
    }

    @Test
    void sumDepositsSek_returnsZeroWhenClientAbsent() {
        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerIskReadClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(null);

        var service = new IskKapitalunderlagDepositService(provider, new OmsConfig());

        assertThat(service.sumDepositsSek(UUID.randomUUID(), 2026))
                .isEqualByComparingTo(BigDecimal.ZERO.setScale(2));
    }
}
