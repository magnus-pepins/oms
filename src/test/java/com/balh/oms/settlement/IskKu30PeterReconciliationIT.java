package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.ledger.LedgerIskReadClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KU30 reconciliation gate against the Peter worked example from
 * {@code plans/ledger-swedish-isk-account-type.md} §12.1.
 *
 * <p>The plan's authoritative numbers, against an Excel manual workbook:
 *
 * <ul>
 *   <li>Quarter-start valuations (SEK): Q1=0, Q2=42 000, Q3=43 000, Q4=65 000 → Σ = 150 000</li>
 *   <li>Qualifying deposits during the year (SEK): 60 000</li>
 *   <li>Kapitalunderlag = (150 000 + 60 000) / 4 = <strong>52 500.00</strong></li>
 *   <li>Schablonintäkt at statslåneränta 1.96 % (→ schablon rate 2.96 %) = 52 500 × 0.0296 =
 *       <strong>1 554.00</strong> (exactly to the öre)</li>
 * </ul>
 *
 * <p>This IT is the regression gate: any change that breaks the per-öre reconciliation against
 * Peter's example fails the build before it can ship to a draft KU30 the legal team has to
 * eyeball.
 */
@Import(IskKu30PeterReconciliationIT.PeterLedgerStub.class)
class IskKu30PeterReconciliationIT extends AbstractPostgresIntegrationTest {

    /** Inkomstår used by the Peter example. */
    private static final int TAX_YEAR = 2025;

    /** Stable IDs so the stub knows which account to answer for. */
    private static final UUID ACCOUNT_ID =
            UUID.fromString("11111111-2222-4333-8444-555555555555");
    private static final UUID ISK_ACCOUNT_ID =
            UUID.fromString("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
    private static final String LEDGER_BALANCE_ID = "bal-peter-internal-uuid";
    private static final String PUBLIC_ACCOUNT_NUMBER = "ISK-PETER-2025";

    @Autowired NamedParameterJdbcTemplate jdbc;
    @Autowired IskKu30ExportService ku30;

    @BeforeEach
    void seedPeter() {
        jdbc.getJdbcTemplate().update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);

        jdbc.update(
                """
                        INSERT INTO oms_account_tax_wrapper (
                            account_id, tax_wrapper, isk_account_id, ledger_balance_id
                        ) VALUES (:accountId, 'isk', :iskAccountId, :ledgerBalanceId)
                        ON CONFLICT (account_id) DO UPDATE SET
                            tax_wrapper = EXCLUDED.tax_wrapper,
                            isk_account_id = EXCLUDED.isk_account_id,
                            ledger_balance_id = EXCLUDED.ledger_balance_id
                        """,
                new MapSqlParameterSource()
                        .addValue("accountId", ACCOUNT_ID)
                        .addValue("iskAccountId", ISK_ACCOUNT_ID)
                        .addValue("ledgerBalanceId", LEDGER_BALANCE_ID));

        upsertQuarterSnapshot(LocalDate.of(TAX_YEAR, 1, 1), new BigDecimal("0"));
        upsertQuarterSnapshot(LocalDate.of(TAX_YEAR, 4, 1), new BigDecimal("42000"));
        upsertQuarterSnapshot(LocalDate.of(TAX_YEAR, 7, 1), new BigDecimal("43000"));
        upsertQuarterSnapshot(LocalDate.of(TAX_YEAR, 10, 1), new BigDecimal("65000"));

        // Override the V79 seed (which used 0.0188 / 0.0288) with the Peter-example rate of 2.96%.
        jdbc.update(
                """
                        INSERT INTO isk_tax_parameters (tax_year, statslaneranta, schablon_rate, source)
                        VALUES (:taxYear, 0.0196, 0.0296, 'peter-it')
                        ON CONFLICT (tax_year) DO UPDATE SET
                            statslaneranta = EXCLUDED.statslaneranta,
                            schablon_rate = EXCLUDED.schablon_rate,
                            source = EXCLUDED.source,
                            updated_at = NOW()
                        """,
                new MapSqlParameterSource("taxYear", TAX_YEAR));
    }

    @Test
    void peterExample_kapitalunderlagAndSchablonintaktMatchSpecToTheOre() throws Exception {
        IskKu30ExportService.ExportResult result = ku30.buildDraft(TAX_YEAR);

        assertThat(result.accountCount()).isEqualTo(1);

        JsonNode aggregate = new ObjectMapper().readTree(result.aggregateJson());
        assertThat(aggregate.get("taxYear").asInt()).isEqualTo(TAX_YEAR);
        assertThat(new BigDecimal(aggregate.get("kapitalunderlagSek").asText()))
                .as("Peter §12.1: (0 + 42000 + 43000 + 65000 + 60000) / 4 = 52500.00 SEK")
                .isEqualByComparingTo("52500.00");
        assertThat(new BigDecimal(aggregate.get("schablonintaktSek").asText()))
                .as("Peter §12.1: 52500 × 0.0296 = 1554.00 SEK (öre-exact)")
                .isEqualByComparingTo("1554.00");
        assertThat(new BigDecimal(aggregate.get("schablonRate").asText()))
                .as("schablon rate 2.96% for inkomstår 2025 in the example")
                .isEqualByComparingTo("0.029600");

        String perAccountJson =
                jdbc.queryForObject(
                        """
                                SELECT export_json::text FROM isk_tax_year_export
                                WHERE tax_year = :taxYear AND isk_account_id = :iskAccountId
                                """,
                        new MapSqlParameterSource()
                                .addValue("taxYear", TAX_YEAR)
                                .addValue("iskAccountId", ISK_ACCOUNT_ID),
                        String.class);
        JsonNode perAccount = new ObjectMapper().readTree(perAccountJson);

        assertThat(perAccount.get("publicAccountNumber").asText())
                .as("KU30 ruta 817 must be the ledger isk_accounts.public_account_number")
                .isEqualTo(PUBLIC_ACCOUNT_NUMBER);
        assertThat(perAccount.get("ledgerBalanceId").asText())
                .as("internal ledger_balance_id preserved as a SEPARATE audit field, NOT ruta 817")
                .isEqualTo(LEDGER_BALANCE_ID);
        assertThat(new BigDecimal(perAccount.get("kapitalunderlagSek").asText()))
                .isEqualByComparingTo("52500.00");
        assertThat(new BigDecimal(perAccount.get("schablonintaktSek").asText()))
                .isEqualByComparingTo("1554.00");
        assertThat(new BigDecimal(perAccount.get("depositsTowardKapitalunderlagSek").asText()))
                .as("Peter §12.1: 60000 SEK qualifying deposits during 2025")
                .isEqualByComparingTo("60000.00");

        String publicAccountNumberInRow =
                jdbc.queryForObject(
                        """
                                SELECT public_account_number FROM isk_tax_year_export
                                WHERE tax_year = :taxYear AND isk_account_id = :iskAccountId
                                """,
                        new MapSqlParameterSource()
                                .addValue("taxYear", TAX_YEAR)
                                .addValue("iskAccountId", ISK_ACCOUNT_ID),
                        String.class);
        assertThat(publicAccountNumberInRow)
                .as("DB column public_account_number must also come from the ledger projector")
                .isEqualTo(PUBLIC_ACCOUNT_NUMBER);
    }

    private void upsertQuarterSnapshot(LocalDate quarterStart, BigDecimal totalSek) {
        jdbc.update(
                """
                        INSERT INTO isk_valuation_snapshot (
                            isk_account_id, account_id, quarter_start, cash_sek, securities_sek,
                            total_sek, valuation_source
                        ) VALUES (
                            :iskAccountId, :accountId, :quarterStart, :cashSek, 0,
                            :totalSek, 'peter-it'
                        )
                        ON CONFLICT (isk_account_id, quarter_start) DO UPDATE SET
                            cash_sek = EXCLUDED.cash_sek,
                            securities_sek = EXCLUDED.securities_sek,
                            total_sek = EXCLUDED.total_sek,
                            valuation_source = EXCLUDED.valuation_source,
                            snapshot_at = NOW()
                        """,
                new MapSqlParameterSource()
                        .addValue("iskAccountId", ISK_ACCOUNT_ID)
                        .addValue("accountId", ACCOUNT_ID)
                        .addValue("quarterStart", java.sql.Date.valueOf(quarterStart))
                        .addValue("cashSek", totalSek)
                        .addValue("totalSek", totalSek));
    }

    /**
     * Stub {@link LedgerIskReadClient} that returns Peter's worked-example data:
     * <ul>
     *   <li>One ledger ISK account with public_account_number = {@value #PUBLIC_ACCOUNT_NUMBER}.</li>
     *   <li>One deposit of 60 000 SEK in 2025 that counts toward kapitalunderlag.</li>
     * </ul>
     *
     * <p>The production {@code RestLedgerIskReadClient} bean is conditional on
     * {@code oms.ledger.isk-read-enabled=true} which the test profile leaves unset, so this
     * stub is the only {@link LedgerIskReadClient} in the context — {@code @Primary} is
     * defense in depth.
     */
    @TestConfiguration
    static class PeterLedgerStub {
        @Bean
        @Primary
        LedgerIskReadClient peterLedgerStub() {
            return new LedgerIskReadClient() {
                @Override
                public List<DepositRow> listDeposits(String iskAccountId, Instant from, Instant to) {
                    if (!ISK_ACCOUNT_ID.toString().equals(iskAccountId)) {
                        return List.of();
                    }
                    return List.of(
                            new DepositRow(
                                    60_000_00L,
                                    "SEK",
                                    "external_cash",
                                    true,
                                    Instant.parse(TAX_YEAR + "-06-01T10:00:00Z")));
                }

                @Override
                public List<IskAccountRow> listAccounts() {
                    return List.of(new IskAccountRow(ISK_ACCOUNT_ID.toString(), PUBLIC_ACCOUNT_NUMBER));
                }
            };
        }
    }
}
