package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

/** Draft KU30 export assembly (Phase E Slice 15b — gap plan §5.10 / I5). */
@Service
public class IskKu30ExportService {

    private final IskValuationSnapshotRepository snapshots;
    private final OmsAccountTaxWrapperRepository taxWrappers;
    private final IskTaxYearExportRepository exports;
    private final ObjectMapper objectMapper;

    public IskKu30ExportService(
            IskValuationSnapshotRepository snapshots,
            OmsAccountTaxWrapperRepository taxWrappers,
            IskTaxYearExportRepository exports,
            ObjectMapper objectMapper) {
        this.snapshots = snapshots;
        this.taxWrappers = taxWrappers;
        this.exports = exports;
        this.objectMapper = objectMapper;
    }

    public ExportResult buildDraft(int taxYear) {
        BigDecimal quarterSum = BigDecimal.ZERO;
        ArrayNode quarterValues = objectMapper.createArrayNode();
        for (int q = 1; q <= 4; q++) {
            LocalDate qs = LocalDate.of(taxYear, (q - 1) * 3 + 1, 1);
            List<IskValuationSnapshotRepository.SnapshotRow> rows = snapshots.listByQuarter(qs);
            BigDecimal qTotal =
                    rows.stream()
                            .map(IskValuationSnapshotRepository.SnapshotRow::totalSek)
                            .reduce(BigDecimal.ZERO, BigDecimal::add);
            quarterSum = quarterSum.add(qTotal);
            quarterValues.addObject().put("quarterStart", qs.toString()).put("totalSek", qTotal.toPlainString());
        }
        ObjectNode ku30 = objectMapper.createObjectNode();
        ku30.put("schemaVersion", 1);
        ku30.put("taxYear", taxYear);
        ku30.put("form", "KU30");
        ku30.put("status", "draft");
        ku30.set("quarterTotals", quarterValues);
        ku30.put("kapitalunderlagSek", quarterSum.toPlainString());
        ku30.put("schablonintaktSek", "0");
        ku30.put("note", "Draft only — schablon rate and deposit exclusions require isk-tax-service (I5)");

        int saved = 0;
        for (OmsAccountTaxWrapperRepository.AccountTaxWrapperRow acct : taxWrappers.listIskAccounts()) {
            java.util.UUID iskId = acct.iskAccountId() != null ? acct.iskAccountId() : acct.accountId();
            ObjectNode perAccount = ku30.deepCopy();
            perAccount.put("iskAccountId", iskId.toString());
            perAccount.put("accountId", acct.accountId().toString());
            if (acct.ledgerBalanceId() != null) {
                perAccount.put("publicAccountNumber", acct.ledgerBalanceId());
            }
            exports.upsertDraft(
                    taxYear,
                    iskId,
                    acct.ledgerBalanceId(),
                    quarterSum,
                    BigDecimal.ZERO,
                    perAccount.toString());
            saved++;
        }
        return new ExportResult(taxYear, saved, ku30.toString());
    }

    public record ExportResult(int taxYear, int accountCount, String aggregateJson) {}
}

@Repository
class IskTaxYearExportRepository {

    private static final String UPSERT =
            """
                    INSERT INTO isk_tax_year_export (
                        tax_year, isk_account_id, public_account_number,
                        kapitalunderlag_sek, schablonintakt_sek, ku30_status, export_json
                    ) VALUES (
                        :taxYear, :iskAccountId, :publicAccountNumber,
                        :kapitalunderlag, :schablonintakt, 'draft', CAST(:exportJson AS JSONB)
                    )
                    ON CONFLICT (tax_year, isk_account_id) DO UPDATE SET
                        public_account_number = EXCLUDED.public_account_number,
                        kapitalunderlag_sek = EXCLUDED.kapitalunderlag_sek,
                        schablonintakt_sek = EXCLUDED.schablonintakt_sek,
                        export_json = EXCLUDED.export_json,
                        created_at = NOW()
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    IskTaxYearExportRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    void upsertDraft(
            int taxYear,
            java.util.UUID iskAccountId,
            String publicAccountNumber,
            BigDecimal kapitalunderlag,
            BigDecimal schablonintakt,
            String exportJson) {
        jdbc.update(
                UPSERT,
                new MapSqlParameterSource()
                        .addValue("taxYear", taxYear)
                        .addValue("iskAccountId", iskAccountId)
                        .addValue("publicAccountNumber", publicAccountNumber)
                        .addValue("kapitalunderlag", kapitalunderlag)
                        .addValue("schablonintakt", schablonintakt)
                        .addValue("exportJson", exportJson));
    }
}
