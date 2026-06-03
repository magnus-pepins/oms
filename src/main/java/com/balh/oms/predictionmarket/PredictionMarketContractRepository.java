package com.balh.oms.predictionmarket;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class PredictionMarketContractRepository {

    public record ContractRow(
            long id,
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String resolutionSource,
            String status,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt) {}

    private static final String LIST_OPEN =
            """
                    SELECT id, slug, title, yes_symbol, no_symbol, resolution_source, status,
                           tick_size, payout_per_contract, closes_at
                    FROM prediction_market_contract
                    WHERE status = 'OPEN'
                    ORDER BY closes_at NULLS LAST, id
                    """;

    private static final String FIND_BY_YES =
            """
                    SELECT id, slug, title, yes_symbol, no_symbol, resolution_source, status,
                           tick_size, payout_per_contract, closes_at
                    FROM prediction_market_contract
                    WHERE yes_symbol = :yesSymbol
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public PredictionMarketContractRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<ContractRow> listOpen() {
        return jdbc.query(LIST_OPEN, new MapSqlParameterSource(), this::mapRow);
    }

    public Optional<ContractRow> findByYesSymbol(String yesSymbol) {
        List<ContractRow> rows =
                jdbc.query(
                        FIND_BY_YES,
                        new MapSqlParameterSource("yesSymbol", yesSymbol.trim()),
                        this::mapRow);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    private ContractRow mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        java.sql.Timestamp closes = rs.getTimestamp("closes_at");
        return new ContractRow(
                rs.getLong("id"),
                rs.getString("slug"),
                rs.getString("title"),
                rs.getString("yes_symbol"),
                rs.getString("no_symbol"),
                rs.getString("resolution_source"),
                rs.getString("status"),
                rs.getBigDecimal("tick_size"),
                rs.getBigDecimal("payout_per_contract"),
                closes == null ? null : closes.toInstant());
    }
}
