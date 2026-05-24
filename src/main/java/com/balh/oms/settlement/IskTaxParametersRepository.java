package com.balh.oms.settlement;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class IskTaxParametersRepository {

    public record TaxParametersRow(int taxYear, BigDecimal statslaneranta, BigDecimal schablonRate, String source) {}

    private static final String SELECT_BY_YEAR =
            """
                    SELECT tax_year, statslaneranta, schablon_rate, source
                    FROM isk_tax_parameters
                    WHERE tax_year = :taxYear
                    """;

    private static final int RATE_SCALE = 6;

    private final NamedParameterJdbcTemplate jdbc;

    public IskTaxParametersRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<TaxParametersRow> findByYear(int taxYear) {
        return jdbc.query(
                        SELECT_BY_YEAR,
                        new MapSqlParameterSource("taxYear", taxYear),
                        (rs, rowNum) ->
                                new TaxParametersRow(
                                        rs.getInt("tax_year"),
                                        rs.getBigDecimal("statslaneranta"),
                                        rs.getBigDecimal("schablon_rate"),
                                        rs.getString("source")))
                .stream()
                .findFirst();
    }

    /** IL 42 kap. 35 §: max(statslåneränta + 1.00%, 1.25%). */
    public static BigDecimal deriveSchablonRate(BigDecimal statslaneranta) {
        BigDecimal plusOnePercent = statslaneranta.add(new BigDecimal("0.01"));
        BigDecimal floor = new BigDecimal("0.0125");
        return plusOnePercent.max(floor).setScale(RATE_SCALE, RoundingMode.HALF_UP);
    }
}
