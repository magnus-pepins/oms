package com.balh.oms.fx;

import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class FxNostroCorrespondentRepository {

    public record Row(
            long id,
            String currency,
            String correspondentCode,
            String ledgerBalanceId,
            int priority,
            String status) {}

    private final NamedParameterJdbcTemplate jdbc;

    public FxNostroCorrespondentRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Row> listByCurrency(String currency) {
        return jdbc.query(
                """
                        SELECT id, currency, correspondent_code, ledger_balance_id, priority, status
                        FROM fx_nostro_correspondent
                        WHERE currency = :currency
                        ORDER BY priority ASC, id ASC
                        """,
                new MapSqlParameterSource("currency", currency.trim().toUpperCase()),
                (rs, rowNum) ->
                        new Row(
                                rs.getLong("id"),
                                rs.getString("currency"),
                                rs.getString("correspondent_code"),
                                rs.getString("ledger_balance_id"),
                                rs.getInt("priority"),
                                rs.getString("status")));
    }

    public Optional<Row> findActivePrimary(String currency) {
        var rows =
                jdbc.query(
                        """
                                SELECT id, currency, correspondent_code, ledger_balance_id, priority, status
                                FROM fx_nostro_correspondent
                                WHERE currency = :currency AND status = 'active'
                                ORDER BY priority ASC, id ASC
                                LIMIT 1
                                """,
                        new MapSqlParameterSource("currency", currency.trim().toUpperCase()),
                        (rs, rowNum) ->
                                new Row(
                                        rs.getLong("id"),
                                        rs.getString("currency"),
                                        rs.getString("correspondent_code"),
                                        rs.getString("ledger_balance_id"),
                                        rs.getInt("priority"),
                                        rs.getString("status")));
        return rows.stream().findFirst();
    }

    public List<Row> listAll() {
        return jdbc.query(
                """
                        SELECT id, currency, correspondent_code, ledger_balance_id, priority, status
                        FROM fx_nostro_correspondent
                        ORDER BY currency ASC, priority ASC, id ASC
                        """,
                new MapSqlParameterSource(),
                (rs, rowNum) ->
                        new Row(
                                rs.getLong("id"),
                                rs.getString("currency"),
                                rs.getString("correspondent_code"),
                                rs.getString("ledger_balance_id"),
                                rs.getInt("priority"),
                                rs.getString("status")));
    }
}
