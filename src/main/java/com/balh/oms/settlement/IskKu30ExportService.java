package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.List;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

/** Draft KU30 export assembly (Phase E Slice 15b — gap plan §5.10 / I5). */
@Service
public class IskKu30ExportService {

    private static final int MONEY_SCALE = 2;

    private final IskValuationSnapshotRepository snapshots;
    private final OmsAccountTaxWrapperRepository taxWrappers;
    private final IskTaxYearExportRepository exports;
    private final IskSchablonTaxService schablonTax;
    private final ObjectMapper objectMapper;

    public IskKu30ExportService(
            IskValuationSnapshotRepository snapshots,
            OmsAccountTaxWrapperRepository taxWrappers,
            IskTaxYearExportRepository exports,
            IskSchablonTaxService schablonTax,
            ObjectMapper objectMapper) {
        this.snapshots = snapshots;
        this.taxWrappers = taxWrappers;
        this.exports = exports;
        this.schablonTax = schablonTax;
        this.objectMapper = objectMapper;
    }

    public enum ApproveResult {
        APPROVED,
        NOT_FOUND,
        NOT_DRAFT,
        SAME_ACTOR
    }

    public ExportResult buildDraft(int taxYear) {
        BigDecimal aggregateKapital = BigDecimal.ZERO;
        BigDecimal aggregateSchablon = BigDecimal.ZERO;
        ArrayNode quarterValues = objectMapper.createArrayNode();
        for (int q = 1; q <= 4; q++) {
            LocalDate qs = LocalDate.of(taxYear, (q - 1) * 3 + 1, 1);
            List<IskValuationSnapshotRepository.SnapshotRow> rows = snapshots.listByQuarter(qs);
            BigDecimal qTotal =
                    rows.stream()
                            .map(IskValuationSnapshotRepository.SnapshotRow::totalSek)
                            .reduce(BigDecimal.ZERO, BigDecimal::add);
            quarterValues.addObject().put("quarterStart", qs.toString()).put("totalSek", qTotal.toPlainString());
        }

        int saved = 0;
        ObjectNode aggregateKu30 = objectMapper.createObjectNode();
        aggregateKu30.put("schemaVersion", 2);
        aggregateKu30.put("taxYear", taxYear);
        aggregateKu30.put("form", "KU30");
        aggregateKu30.put("status", "draft");
        aggregateKu30.set("quarterTotals", quarterValues);

        for (OmsAccountTaxWrapperRepository.AccountTaxWrapperRow acct : taxWrappers.listIskAccounts()) {
            java.util.UUID iskId = acct.iskAccountId() != null ? acct.iskAccountId() : acct.accountId();
            BigDecimal kapital = kapitalunderlagForAccount(iskId, taxYear);
            BigDecimal schablon = schablonTax.schablonintakt(kapital, taxYear);
            aggregateKapital = aggregateKapital.add(kapital);
            aggregateSchablon = aggregateSchablon.add(schablon);

            ObjectNode perAccount = objectMapper.createObjectNode();
            perAccount.put("schemaVersion", 2);
            perAccount.put("taxYear", taxYear);
            perAccount.put("form", "KU30");
            perAccount.put("status", "draft");
            perAccount.put("iskAccountId", iskId.toString());
            perAccount.put("accountId", acct.accountId().toString());
            if (acct.ledgerBalanceId() != null) {
                perAccount.put("publicAccountNumber", acct.ledgerBalanceId());
            }
            perAccount.put("kapitalunderlagSek", kapital.toPlainString());
            perAccount.put("schablonintaktSek", schablon.toPlainString());
            perAccount.put("schablonRate", schablonTax.schablonRateForYear(taxYear).toPlainString());
            perAccount.put(
                    "note",
                    "Draft — deposit exclusions from ledger isk_deposit_events require elevated ledger read (I5 tail)");

            exports.upsertDraft(
                    taxYear,
                    iskId,
                    acct.ledgerBalanceId(),
                    kapital,
                    schablon,
                    perAccount.toString());
            saved++;
        }

        aggregateKu30.put("kapitalunderlagSek", aggregateKapital.toPlainString());
        aggregateKu30.put("schablonintaktSek", aggregateSchablon.toPlainString());
        aggregateKu30.put("schablonRate", schablonTax.schablonRateForYear(taxYear).toPlainString());
        aggregateKu30.put("accountCount", saved);
        return new ExportResult(taxYear, saved, aggregateKu30.toString());
    }

    public ApproveResult approveKu30(int taxYear, java.util.UUID iskAccountId, String approver, String draftCreator) {
        if (approver == null || approver.isBlank()) {
            return ApproveResult.SAME_ACTOR;
        }
        if (draftCreator != null && approver.trim().equalsIgnoreCase(draftCreator.trim())) {
            return ApproveResult.SAME_ACTOR;
        }
        int updated = exports.approveDraft(taxYear, iskAccountId, approver.trim());
        if (updated == 0) {
            return exports.findStatus(taxYear, iskAccountId).map(s -> "draft".equals(s) ? ApproveResult.NOT_FOUND : ApproveResult.NOT_DRAFT)
                    .orElse(ApproveResult.NOT_FOUND);
        }
        return ApproveResult.APPROVED;
    }

    private BigDecimal kapitalunderlagForAccount(java.util.UUID iskAccountId, int taxYear) {
        BigDecimal quarterSum = BigDecimal.ZERO;
        for (int q = 1; q <= 4; q++) {
            LocalDate qs = LocalDate.of(taxYear, (q - 1) * 3 + 1, 1);
            quarterSum =
                    quarterSum.add(
                            snapshots.listByQuarter(qs).stream()
                                    .filter(r -> iskAccountId.equals(r.iskAccountId()))
                                    .map(IskValuationSnapshotRepository.SnapshotRow::totalSek)
                                    .reduce(BigDecimal.ZERO, BigDecimal::add));
        }
        return quarterSum
                .divide(new BigDecimal("4"), MONEY_SCALE, RoundingMode.HALF_UP);
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
                        ku30_status = 'draft',
                        export_json = EXCLUDED.export_json,
                        approved_by = NULL,
                        approved_at = NULL,
                        created_at = NOW()
                    """;

    private static final String APPROVE =
            """
                    UPDATE isk_tax_year_export
                    SET ku30_status = 'approved',
                        approved_by = :approver,
                        approved_at = NOW(),
                        export_json = jsonb_set(export_json, '{status}', '"approved"')
                    WHERE tax_year = :taxYear
                      AND isk_account_id = :iskAccountId
                      AND ku30_status = 'draft'
                    """;

    private static final String SELECT_STATUS =
            """
                    SELECT ku30_status FROM isk_tax_year_export
                    WHERE tax_year = :taxYear AND isk_account_id = :iskAccountId
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

    int approveDraft(int taxYear, java.util.UUID iskAccountId, String approver) {
        return jdbc.update(
                APPROVE,
                new MapSqlParameterSource()
                        .addValue("taxYear", taxYear)
                        .addValue("iskAccountId", iskAccountId)
                        .addValue("approver", approver));
    }

    java.util.Optional<String> findStatus(int taxYear, java.util.UUID iskAccountId) {
        return jdbc.query(
                        SELECT_STATUS,
                        new MapSqlParameterSource()
                                .addValue("taxYear", taxYear)
                                .addValue("iskAccountId", iskAccountId),
                        (rs, rowNum) -> rs.getString("ku30_status"))
                .stream()
                .findFirst();
    }
}
