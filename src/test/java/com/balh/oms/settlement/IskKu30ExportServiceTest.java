package com.balh.oms.settlement;

import com.balh.oms.ledger.LedgerIskReadClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IskKu30ExportServiceTest {

    private static final UUID ISK_ID = UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");
    private static final UUID ACCOUNT_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");

    @Test
    void buildDraft_includesLedgerDepositSumInPerAccountJson() throws Exception {
        IskValuationSnapshotRepository snapshots = mock(IskValuationSnapshotRepository.class);
        when(snapshots.listByQuarter(any())).thenReturn(List.of());

        OmsAccountTaxWrapperRepository taxWrappers = mock(OmsAccountTaxWrapperRepository.class);
        when(taxWrappers.listIskAccounts())
                .thenReturn(
                        List.of(
                                new OmsAccountTaxWrapperRepository.AccountTaxWrapperRow(
                                        ACCOUNT_ID,
                                        OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK,
                                        ISK_ID,
                                        "bal-isk-1")));

        IskTaxYearExportRepository exports = mock(IskTaxYearExportRepository.class);
        IskSchablonTaxService schablonTax = mock(IskSchablonTaxService.class);
        when(schablonTax.schablonintakt(any(), eq(2026))).thenReturn(new BigDecimal("100.00"));
        when(schablonTax.schablonRateForYear(2026)).thenReturn(new BigDecimal("0.0125"));

        LedgerIskReadClient ledgerClient = mock(LedgerIskReadClient.class);
        when(ledgerClient.listDeposits(eq(ISK_ID.toString()), any(), any()))
                .thenReturn(
                        List.of(
                                new LedgerIskReadClient.DepositRow(
                                        25_000_00L,
                                        "SEK",
                                        "external_cash",
                                        true,
                                        Instant.parse("2026-02-01T10:00:00Z"))));

        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerIskReadClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(ledgerClient);

        var kapitalunderlagDeposits = new IskKapitalunderlagDepositService(provider, new com.balh.oms.config.OmsConfig());
        var service =
                new IskKu30ExportService(
                        snapshots, taxWrappers, exports, schablonTax, kapitalunderlagDeposits, new ObjectMapper());

        IskKu30ExportService.ExportResult result = service.buildDraft(2026);

        assertThat(result.accountCount()).isEqualTo(1);
        ObjectNode aggregate = (ObjectNode) new ObjectMapper().readTree(result.aggregateJson());
        assertThat(aggregate.get("kapitalunderlagSek").asText()).isEqualTo("6250.00");
    }
}
