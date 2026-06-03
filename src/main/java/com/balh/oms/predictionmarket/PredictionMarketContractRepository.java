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
            String description,
            String resolutionCriteria,
            List<PredictionMarketReferenceLinks.Link> referenceLinks,
            String resolutionSource,
            String status,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt,
            Instant resolvesAt,
            List<String> jurisdictionTags) {}

    private static final String SELECT_COLUMNS =
            """
                    id, slug, title, yes_symbol, no_symbol, description, resolution_criteria,
                    reference_links, resolution_source, status, settlement_currency, tick_size,
                    payout_per_contract, closes_at, resolves_at, jurisdiction_tags
                    """;

    private static final String LIST_OPEN =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE status = 'OPEN'
                    ORDER BY closes_at NULLS LAST, id
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String LIST_ALL_UNFILTERED =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    ORDER BY id DESC
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String LIST_ALL_BY_STATUS =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE status = :status
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

    private static final String FIND_BY_SLUG =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE slug = :slug
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String FIND_TICK_BY_SYMBOL =
            """
                    SELECT tick_size
                    FROM prediction_market_contract
                    WHERE yes_symbol = :symbol OR no_symbol = :symbol
                    LIMIT 1
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public PredictionMarketContractRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<ContractRow> listOpen() {
        return jdbc.query(LIST_OPEN, new MapSqlParameterSource(), this::mapRow);
    }

    public List<ContractRow> listAll(String statusOrNull) {
        if (statusOrNull == null || statusOrNull.isBlank()) {
            return jdbc.query(LIST_ALL_UNFILTERED, new MapSqlParameterSource(), this::mapRow);
        }
        return jdbc.query(
                LIST_ALL_BY_STATUS,
                new MapSqlParameterSource("status", statusOrNull.trim().toUpperCase()),
                this::mapRow);
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

    public Optional<ContractRow> findBySlug(String slug) {
        if (slug == null || slug.isBlank()) {
            return Optional.empty();
        }
        List<ContractRow> rows =
                jdbc.query(
                        FIND_BY_SLUG,
                        new MapSqlParameterSource("slug", slug.trim()),
                        this::mapRow);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    /**
     * Tick size for whichever leg (YES or NO) of a contract carries {@code symbol}. Single indexed
     * point-select used by the ingress tick gate ({@code PredictionMarketTickGate}); cached there so the
     * accept path does not re-query per order. Empty when no contract owns the symbol.
     */
    public Optional<BigDecimal> findTickSizeBySymbol(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return Optional.empty();
        }
        List<BigDecimal> ticks =
                jdbc.query(
                        FIND_TICK_BY_SYMBOL,
                        new MapSqlParameterSource("symbol", symbol.trim()),
                        (rs, rowNum) -> rs.getBigDecimal("tick_size"));
        return ticks.isEmpty() ? Optional.empty() : Optional.ofNullable(ticks.getFirst());
    }

    public ContractRow insert(
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String description,
            String resolutionCriteria,
            List<PredictionMarketReferenceLinks.Link> referenceLinks,
            String resolutionSource,
            String status,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt,
            Instant resolvesAt,
            List<String> jurisdictionTags) {
        Long id =
                jdbc.queryForObject(
                        """
                                INSERT INTO prediction_market_contract (
                                    slug, title, yes_symbol, no_symbol, description, resolution_criteria,
                                    reference_links, resolution_source, status, settlement_currency,
                                    tick_size, payout_per_contract, closes_at, resolves_at, jurisdiction_tags
                                ) VALUES (
                                    :slug, :title, :yesSymbol, :noSymbol, :description, :resolutionCriteria,
                                    CAST(:referenceLinks AS JSONB), :resolutionSource, :status,
                                    :settlementCurrency, :tickSize, :payoutPerContract, :closesAt, :resolvesAt,
                                    CAST(:jurisdictionTags AS JSONB)
                                )
                                RETURNING id
                                """,
                        bindWrite(
                                slug,
                                title,
                                yesSymbol,
                                noSymbol,
                                description,
                                resolutionCriteria,
                                referenceLinks,
                                resolutionSource,
                                status,
                                settlementCurrency,
                                tickSize,
                                payoutPerContract,
                                closesAt,
                                resolvesAt,
                                jurisdictionTags),
                        Long.class);
        return findById(id).orElseThrow();
    }

    public ContractRow update(
            long id,
            String title,
            String description,
            String resolutionCriteria,
            List<PredictionMarketReferenceLinks.Link> referenceLinks,
            String resolutionSource,
            String status,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt,
            Instant resolvesAt,
            List<String> jurisdictionTags) {
        jdbc.update(
                """
                        UPDATE prediction_market_contract
                        SET title = :title,
                            description = :description,
                            resolution_criteria = :resolutionCriteria,
                            reference_links = CAST(:referenceLinks AS JSONB),
                            resolution_source = :resolutionSource,
                            status = :status,
                            settlement_currency = :settlementCurrency,
                            tick_size = :tickSize,
                            payout_per_contract = :payoutPerContract,
                            closes_at = :closesAt,
                            resolves_at = :resolvesAt,
                            jurisdiction_tags = CAST(:jurisdictionTags AS JSONB)
                        WHERE id = :id
                        """,
                bindWrite(
                                null,
                                title,
                                null,
                                null,
                                description,
                                resolutionCriteria,
                                referenceLinks,
                                resolutionSource,
                                status,
                                settlementCurrency,
                                tickSize,
                                payoutPerContract,
                                closesAt,
                                resolvesAt,
                                jurisdictionTags)
                        .addValue("id", id));
        return findById(id).orElseThrow();
    }

    private MapSqlParameterSource bindWrite(
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String description,
            String resolutionCriteria,
            List<PredictionMarketReferenceLinks.Link> referenceLinks,
            String resolutionSource,
            String status,
            String settlementCurrency,
            BigDecimal tickSize,
            BigDecimal payoutPerContract,
            Instant closesAt,
            Instant resolvesAt,
            List<String> jurisdictionTags) {
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
        params.addValue("description", description);
        params.addValue("resolutionCriteria", resolutionCriteria);
        params.addValue(
                "referenceLinks",
                PredictionMarketReferenceLinks.toJson(
                        referenceLinks == null
                                ? PredictionMarketReferenceLinks.empty()
                                : referenceLinks));
        params.addValue("resolutionSource", resolutionSource);
        params.addValue("status", status);
        params.addValue("settlementCurrency", settlementCurrency);
        params.addValue("tickSize", tickSize);
        params.addValue("payoutPerContract", payoutPerContract);
        params.addValue("closesAt", closesAt == null ? null : Timestamp.from(closesAt));
        params.addValue("resolvesAt", resolvesAt == null ? null : Timestamp.from(resolvesAt));
        params.addValue(
                "jurisdictionTags",
                PredictionMarketJurisdictionTags.toJson(
                        jurisdictionTags == null
                                ? PredictionMarketJurisdictionTags.empty()
                                : jurisdictionTags));
        return params;
    }

    private ContractRow mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        Timestamp closes = rs.getTimestamp("closes_at");
        Timestamp resolves = rs.getTimestamp("resolves_at");
        return new ContractRow(
                rs.getLong("id"),
                rs.getString("slug"),
                rs.getString("title"),
                rs.getString("yes_symbol"),
                rs.getString("no_symbol"),
                rs.getString("description"),
                rs.getString("resolution_criteria"),
                PredictionMarketReferenceLinks.fromJson(rs.getString("reference_links")),
                rs.getString("resolution_source"),
                rs.getString("status"),
                rs.getString("settlement_currency"),
                rs.getBigDecimal("tick_size"),
                rs.getBigDecimal("payout_per_contract"),
                closes == null ? null : closes.toInstant(),
                resolves == null ? null : resolves.toInstant(),
                PredictionMarketJurisdictionTags.fromJson(rs.getString("jurisdiction_tags")));
    }
}
