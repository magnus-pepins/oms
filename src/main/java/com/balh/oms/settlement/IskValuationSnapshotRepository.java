package com.balh.oms.settlement;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class IskValuationSnapshotRepository {

    public record SnapshotRow(
            long id,
            UUID iskAccountId,
            UUID accountId,
            LocalDate quarterStart,
            BigDecimal cashSek,
            BigDecimal securitiesSek,
            BigDecimal totalSek,
            String valuationSource) {}

    private static final String UPSERT =
            """
                    INSERT INTO isk_valuation_snapshot (
                        isk_account_id, account_id, quarter_start, cash_sek, securities_sek,
                        total_sek, valuation_source
                    ) VALUES (
                        :iskAccountId, :accountId, :quarterStart, :cashSek, :securitiesSek,
                        :totalSek, :valuationSource
                    )
                    ON CONFLICT (isk_account_id, quarter_start) DO UPDATE SET
                        cash_sek = EXCLUDED.cash_sek,
                        securities_sek = EXCLUDED.securities_sek,
                        total_sek = EXCLUDED.total_sek,
                        valuation_source = EXCLUDED.valuation_source,
                        snapshot_at = NOW()
                    RETURNING id
                    """;

    private static final String LIST_BY_QUARTER =
            """
                    SELECT id, isk_account_id, account_id, quarter_start, cash_sek, securities_sek,
                           total_sek, valuation_source
                    FROM isk_valuation_snapshot
                    WHERE quarter_start = :quarterStart
                    ORDER BY isk_account_id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public IskValuationSnapshotRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long upsert(
            UUID iskAccountId,
            UUID accountId,
            LocalDate quarterStart,
            BigDecimal cashSek,
            BigDecimal securitiesSek,
            String valuationSource) {
        BigDecimal total = cashSek.add(securitiesSek);
        List<Long> ids =
                jdbc.query(
                        UPSERT,
                        new MapSqlParameterSource()
                                .addValue("iskAccountId", iskAccountId)
                                .addValue("accountId", accountId)
                                .addValue("quarterStart", java.sql.Date.valueOf(quarterStart))
                                .addValue("cashSek", cashSek)
                                .addValue("securitiesSek", securitiesSek)
                                .addValue("totalSek", total)
                                .addValue("valuationSource", valuationSource),
                        (rs, rowNum) -> rs.getLong("id"));
        return ids.getFirst();
    }

    public List<SnapshotRow> listByQuarter(LocalDate quarterStart) {
        return jdbc.query(
                LIST_BY_QUARTER,
                new MapSqlParameterSource("quarterStart", java.sql.Date.valueOf(quarterStart)),
                (rs, rowNum) ->
                        new SnapshotRow(
                                rs.getLong("id"),
                                (UUID) rs.getObject("isk_account_id"),
                                (UUID) rs.getObject("account_id"),
                                rs.getDate("quarter_start").toLocalDate(),
                                rs.getBigDecimal("cash_sek"),
                                rs.getBigDecimal("securities_sek"),
                                rs.getBigDecimal("total_sek"),
                                rs.getString("valuation_source")));
    }
}
