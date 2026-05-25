package com.balh.oms.corporateaction;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AccountTaxResidencyRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public AccountTaxResidencyRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<String> findTaxCountry(UUID accountId) {
        if (accountId == null) {
            return Optional.empty();
        }
        var rows =
                jdbc.query(
                        """
                                SELECT tax_country FROM oms_account_tax_residency
                                WHERE account_id = :accountId
                                """,
                        new MapSqlParameterSource("accountId", accountId),
                        (rs, rowNum) -> rs.getString("tax_country"));
        return rows.stream().findFirst();
    }
}
