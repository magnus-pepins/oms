package com.balh.oms.predictionmarket;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

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
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt) {}

    private static final String SELECT_COLUMNS =
            """
                    id, slug, title, yes_symbol, no_symbol, resolution_source, status,
                    settlement_currency, tick_size, payout_per_contract, closes_at
                    """;

    private static final String LIST_OPEN =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE status = 'OPEN'
                    ORDER BY closes_at NULLS LAST, id
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String LIST_ALL =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE (:status IS NULL OR status = :status)
                    ORDER BY id DESC
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String FIND_BY_ID =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE id = :id
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String FIND_BY_YES =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE yes_symbol = :yesSymbol
                    """
                    .formatted(SELECT_COLUMNS);

    private final NamedParameterJdbcTemplate jdbc;

    public PredictionMarketContractRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<ContractRow> listOpen() {
        return jdbc.query(LIST_OPEN, new MapSqlParameterSource(), this::mapRow);
    }

    public List<ContractRow> listAll(String statusOrNull) {
        MapSqlParameterSource params = new MapSqlParameterSource("status", statusOrNull);
        return jdbc.query(LIST_ALL, params, this::mapRow);
    }

    public Optional<ContractRow> findById(long id) {
        List<ContractRow> rows =
                jdbc.query(FIND_BY_ID, new MapSqlParameterSource("id", id), this::mapRow);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    public Optional<ContractRow> findByYesSymbol(String yesSymbol) {
        List<ContractRow> rows =
                jdbc.query(
                        FIND_BY_YES,
                        new MapSqlParameterSource("yesSymbol", yesSymbol.trim()),
                        this::mapRow);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    public ContractRow insert(
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String resolutionSource,
            String status,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt) {
        Long id =
                jdbc.queryForObject(
                        """
                                INSERT INTO prediction_market_contract (
                                    slug, title, yes_symbol, no_symbol, resolution_source, status,
                                    settlement_currency, tick_size, payout_per_contract, closes_at
                                ) VALUES (
                                    :slug, :title, :yesSymbol, :noSymbol, :resolutionSource, :status,
                                    :settlementCurrency, :tickSize, :payoutPerContract, :closesAt
                                )
                                RETURNING id
                                """,
                        bindWrite(
                                slug,
                                title,
                                yesSymbol,
                                noSymbol,
                                resolutionSource,
                                status,
                                settlementCurrency,
                                tickSize,
                                payoutPerContract,
                                closesAt),
                        Long.class);
        return findById(id).orElseThrow();
    }

    public ContractRow update(
            long id,
            String title,
            String resolutionSource,
            String status,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt) {
        jdbc.update(
                """
                        UPDATE prediction_market_contract
                        SET title = :title,
                            resolution_source = :resolutionSource,
                            status = :status,
                            settlement_currency = :settlementCurrency,
                            tick_size = :tickSize,
                            payout_per_contract = :payoutPerContract,
                            closes_at = :closesAt
                        WHERE id = :id
                        """,
                bindWrite(null, title, null, null, resolutionSource, status, settlementCurrency, tickSize, payoutPerContract, closesAt)
                        .addValue("id", id));
        return findById(id).orElseThrow();
    }

    private MapSqlParameterSource bindWrite(
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String resolutionSource,
            String status,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (slug != null) {
            params.addValue("slug", slug);
        }
        params.addValue("title", title);
        if (yesSymbol != null) {
            params.addValue("yesSymbol", yesSymbol);
        }
        if (noSymbol != null) {
            params.addValue("noSymbol", noSymbol);
        }
        params.addValue("resolutionSource", resolutionSource);
        params.addValue("status", status);
        params.addValue("settlementCurrency", settlementCurrency);
        params.addValue("tickSize", tickSize);
        params.addValue("payoutPerContract", payoutPerContract);
        params.addValue("closesAt", closesAt == null ? null : Timestamp.from(closesAt));
        return params;
    }

    private ContractRow mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        Timestamp closes = rs.getTimestamp("closes_at");
        return new ContractRow(
                rs.getLong("id"),
                rs.getString("slug"),
                rs.getString("title"),
                rs.getString("yes_symbol"),
                rs.getString("no_symbol"),
                rs.getString("resolution_source"),
                rs.getString("status"),
                rs.getString("settlement_currency"),
                rs.getBigDecimal("tick_size"),
                rs.getBigDecimal("payout_per_contract"),
                closes == null ? null : closes.toInstant());
    }
}
