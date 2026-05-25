package com.balh.oms.settlement;

import com.balh.oms.ledger.LedgerIskReadClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
        when(ledgerClient.listAccounts())
                .thenReturn(
                        List.of(new LedgerIskReadClient.IskAccountRow(ISK_ID.toString(), "ISK-12345")));

        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerIskReadClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(ledgerClient);

        var kapitalunderlagDeposits = new IskKapitalunderlagDepositService(provider, new com.balh.oms.config.OmsConfig());
        var service =
                new IskKu30ExportService(
                        snapshots,
                        taxWrappers,
                        exports,
                        schablonTax,
                        kapitalunderlagDeposits,
                        provider,
                        new ObjectMapper());

        IskKu30ExportService.ExportResult result = service.buildDraft(2026);

        assertThat(result.accountCount()).isEqualTo(1);
        ObjectNode aggregate = (ObjectNode) new ObjectMapper().readTree(result.aggregateJson());
        assertThat(aggregate.get("kapitalunderlagSek").asText()).isEqualTo("6250.00");
    }

    /**
     * Regression for the KU30 ruta 817 placeholder bug: emitted JSON and the persisted
     * draft row must use the ledger {@code isk_accounts.public_account_number}, never the
     * internal {@code ledger_balance_id}. The latter is preserved as a separate audit field.
     */
    @Test
    void buildDraft_emitsLedgerPublicAccountNumberForRuta817_notLedgerBalanceId() throws Exception {
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
                                        "bal-internal-uuid")));

        IskTaxYearExportRepository exports = mock(IskTaxYearExportRepository.class);
        IskSchablonTaxService schablonTax = mock(IskSchablonTaxService.class);
        when(schablonTax.schablonintakt(any(), eq(2026))).thenReturn(new BigDecimal("0.00"));
        when(schablonTax.schablonRateForYear(2026)).thenReturn(new BigDecimal("0.0125"));

        LedgerIskReadClient ledgerClient = mock(LedgerIskReadClient.class);
        when(ledgerClient.listDeposits(any(), any(), any())).thenReturn(List.of());
        when(ledgerClient.listAccounts())
                .thenReturn(
                        List.of(new LedgerIskReadClient.IskAccountRow(ISK_ID.toString(), "ISK-A-RUTA-817")));

        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerIskReadClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(ledgerClient);

        var kapitalunderlagDeposits = new IskKapitalunderlagDepositService(provider, new com.balh.oms.config.OmsConfig());
        var service =
                new IskKu30ExportService(
                        snapshots,
                        taxWrappers,
                        exports,
                        schablonTax,
                        kapitalunderlagDeposits,
                        provider,
                        new ObjectMapper());

        service.buildDraft(2026);

        ArgumentCaptor<String> publicAccountNumberCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> exportJsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(exports)
                .upsertDraft(
                        eq(2026),
                        eq(ISK_ID),
                        publicAccountNumberCaptor.capture(),
                        any(),
                        any(),
                        exportJsonCaptor.capture());

        assertThat(publicAccountNumberCaptor.getValue())
                .as("DB column public_account_number must come from ledger isk_accounts, not ledger_balance_id")
                .isEqualTo("ISK-A-RUTA-817");

        JsonNode perAccount = new ObjectMapper().readTree(exportJsonCaptor.getValue());
        assertThat(perAccount.get("publicAccountNumber").asText())
                .as("KU30 ruta 817 (publicAccountNumber) must be the ledger public number")
                .isEqualTo("ISK-A-RUTA-817");
        assertThat(perAccount.get("ledgerBalanceId").asText())
                .as("internal ledger_balance_id preserved as a separate audit field, NOT ruta 817")
                .isEqualTo("bal-internal-uuid");
    }

    /**
     * When the ledger read client cannot return a public_account_number (offline, missing row,
     * etc.), the export must NOT fall back to the internal ledger_balance_id — better to emit
     * a draft with no ruta 817 (which four-eyes review will catch) than a wrong-by-default value.
     */
    @Test
    void buildDraft_omitsPublicAccountNumberWhenLedgerHasNoRow() throws Exception {
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
                                        "bal-internal-uuid")));

        IskTaxYearExportRepository exports = mock(IskTaxYearExportRepository.class);
        IskSchablonTaxService schablonTax = mock(IskSchablonTaxService.class);
        when(schablonTax.schablonintakt(any(), eq(2026))).thenReturn(new BigDecimal("0.00"));
        when(schablonTax.schablonRateForYear(2026)).thenReturn(new BigDecimal("0.0125"));

        LedgerIskReadClient ledgerClient = mock(LedgerIskReadClient.class);
        when(ledgerClient.listDeposits(any(), any(), any())).thenReturn(List.of());
        when(ledgerClient.listAccounts()).thenReturn(List.of());

        @SuppressWarnings("unchecked")
        ObjectProvider<LedgerIskReadClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(ledgerClient);

        var kapitalunderlagDeposits = new IskKapitalunderlagDepositService(provider, new com.balh.oms.config.OmsConfig());
        var service =
                new IskKu30ExportService(
                        snapshots,
                        taxWrappers,
                        exports,
                        schablonTax,
                        kapitalunderlagDeposits,
                        provider,
                        new ObjectMapper());

        service.buildDraft(2026);

        ArgumentCaptor<String> publicAccountNumberCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> exportJsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(exports)
                .upsertDraft(
                        eq(2026), eq(ISK_ID), publicAccountNumberCaptor.capture(), any(), any(), exportJsonCaptor.capture());

        assertThat(publicAccountNumberCaptor.getValue())
                .as("missing ledger row → DB column public_account_number stays NULL")
                .isNull();

        JsonNode perAccount = new ObjectMapper().readTree(exportJsonCaptor.getValue());
        assertThat(perAccount.hasNonNull("publicAccountNumber"))
                .as("missing ledger row → JSON omits publicAccountNumber (never falls back to ledgerBalanceId)")
                .isFalse();
        assertThat(perAccount.get("ledgerBalanceId").asText()).isEqualTo("bal-internal-uuid");
    }
}
