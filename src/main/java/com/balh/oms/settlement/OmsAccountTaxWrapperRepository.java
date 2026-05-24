package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Read-side mapping for {@code oms_account_tax_wrapper} (Flyway V73). */
@Repository
public class OmsAccountTaxWrapperRepository {

    public record AccountTaxWrapperRow(
            UUID accountId, String taxWrapper, UUID iskAccountId, String ledgerBalanceId) {}

    public static final String TAX_WRAPPER_ISK = "isk";
    public static final String TAX_WRAPPER_INVESTMENT = "investment";
    public static final String TAX_WRAPPER_NONE = "none";

    private static final String SELECT_BY_ACCOUNT =
            """
                    SELECT account_id, tax_wrapper, isk_account_id, ledger_balance_id
                    FROM oms_account_tax_wrapper
                    WHERE account_id = :accountId
                    LIMIT 1
                    """;

    private static final String SELECT_ISK_ACCOUNTS =
            """
                    SELECT account_id, tax_wrapper, isk_account_id, ledger_balance_id
                    FROM oms_account_tax_wrapper
                    WHERE tax_wrapper = 'isk'
                    ORDER BY account_id
                    """;

    private static final String UPSERT =
            """
                    INSERT INTO oms_account_tax_wrapper (
                        account_id, tax_wrapper, isk_account_id, ledger_balance_id, updated_at
                    ) VALUES (
                        :accountId, :taxWrapper, :iskAccountId, :ledgerBalanceId, NOW()
                    )
                    ON CONFLICT (account_id) DO UPDATE SET
                        tax_wrapper = EXCLUDED.tax_wrapper,
                        isk_account_id = EXCLUDED.isk_account_id,
                        ledger_balance_id = EXCLUDED.ledger_balance_id,
                        updated_at = NOW()
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public OmsAccountTaxWrapperRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<AccountTaxWrapperRow> findByAccountId(UUID accountId) {
        if (accountId == null) {
            return Optional.empty();
        }
        return jdbc.query(
                        SELECT_BY_ACCOUNT,
                        new MapSqlParameterSource("accountId", accountId),
                        (rs, rowNum) ->
                                new AccountTaxWrapperRow(
                                        (UUID) rs.getObject("account_id"),
                                        rs.getString("tax_wrapper"),
                                        (UUID) rs.getObject("isk_account_id"),
                                        rs.getString("ledger_balance_id")))
                .stream()
                .findFirst();
    }

    public void upsert(UUID accountId, String taxWrapper, UUID iskAccountId, String ledgerBalanceId) {
        jdbc.update(
                UPSERT,
                new MapSqlParameterSource()
                        .addValue("accountId", accountId)
                        .addValue("taxWrapper", taxWrapper)
                        .addValue("iskAccountId", iskAccountId)
                        .addValue("ledgerBalanceId", ledgerBalanceId));
    }

    public List<AccountTaxWrapperRow> listIskAccounts() {
        return jdbc.query(
                SELECT_ISK_ACCOUNTS,
                new MapSqlParameterSource(),
                (rs, rowNum) ->
                        new AccountTaxWrapperRow(
                                (UUID) rs.getObject("account_id"),
                                rs.getString("tax_wrapper"),
                                (UUID) rs.getObject("isk_account_id"),
                                rs.getString("ledger_balance_id")));
    }
}
