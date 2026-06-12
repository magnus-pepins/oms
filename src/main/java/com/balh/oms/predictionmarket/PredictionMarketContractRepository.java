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
            List<String> jurisdictionTags,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String feeModelId,
            int feeScheduleVersion,
            String feeParamsJson,
            String retailFeeModelId,
            Long eventId,
            String eventSlug,
            String outcomeLabel,
            int outcomeDisplayOrder) {}

    private static final String SELECT_COLUMNS =
            """
                    id, slug, title, yes_symbol, no_symbol, description, resolution_criteria,
                    reference_links, resolution_source, status, settlement_currency, tick_size,
                    payout_per_contract, closes_at, resolves_at, jurisdiction_tags,
                    category, tags, card_image_url, display_order,
                    fee_model_id, fee_schedule_version, fee_params_json::text AS fee_params_json,
                    retail_fee_model_id, event_id,
                    (SELECT slug FROM prediction_market_event e WHERE e.id = event_id) AS event_slug,
                    outcome_label, outcome_display_order
                    """;

    private static final String LIST_OPEN =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE status = 'OPEN'
                    ORDER BY display_order ASC, id ASC
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String LIST_OPEN_BY_EVENT =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE status = 'OPEN' AND event_id = :eventId
                    ORDER BY outcome_display_order ASC, id ASC
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String LIST_BY_EVENT =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE event_id = :eventId
                    ORDER BY outcome_display_order ASC, id ASC
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String LIST_OPEN_STANDALONE =
            """
                    SELECT %s
                    FROM prediction_market_contract
                    WHERE status = 'OPEN' AND event_id IS NULL
                    ORDER BY display_order ASC, id ASC
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

    public List<ContractRow> listOpenByEventId(long eventId) {
        return jdbc.query(
                LIST_OPEN_BY_EVENT, new MapSqlParameterSource("eventId", eventId), this::mapRow);
    }

    /** All contract statuses linked to an event (operator admin). */
    public List<ContractRow> listByEventId(long eventId) {
        return jdbc.query(
                LIST_BY_EVENT, new MapSqlParameterSource("eventId", eventId), this::mapRow);
    }

    public List<ContractRow> listOpenStandalone() {
        return jdbc.query(LIST_OPEN_STANDALONE, new MapSqlParameterSource(), this::mapRow);
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

    /** Test/seed helper — defaults fee columns to {@code ZERO} / version {@code 1}. */
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
            List<String> jurisdictionTags,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder) {
        return insert(
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
                jurisdictionTags,
                category,
                tags,
                cardImageUrl,
                displayOrder,
                "ZERO",
                1,
                null,
                null,
                null,
                null,
                0);
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
            List<String> jurisdictionTags,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String feeModelId,
            Integer feeScheduleVersion,
            String feeParamsJson,
            String retailFeeModelId) {
        return insert(
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
                jurisdictionTags,
                category,
                tags,
                cardImageUrl,
                displayOrder,
                feeModelId,
                feeScheduleVersion,
                feeParamsJson,
                retailFeeModelId,
                null,
                null,
                0);
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
            List<String> jurisdictionTags,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String feeModelId,
            Integer feeScheduleVersion,
            String feeParamsJson,
            String retailFeeModelId,
            Long eventId,
            String outcomeLabel,
            int outcomeDisplayOrder) {
        Long id =
                jdbc.queryForObject(
                        """
                                INSERT INTO prediction_market_contract (
                                    slug, title, yes_symbol, no_symbol, description, resolution_criteria,
                                    reference_links, resolution_source, status, settlement_currency,
                                    tick_size, payout_per_contract, closes_at, resolves_at, jurisdiction_tags,
                                    category, tags, card_image_url, display_order,
                                    fee_model_id, fee_schedule_version, fee_params_json, retail_fee_model_id,
                                    event_id, outcome_label, outcome_display_order
                                ) VALUES (
                                    :slug, :title, :yesSymbol, :noSymbol, :description, :resolutionCriteria,
                                    CAST(:referenceLinks AS JSONB), :resolutionSource, :status,
                                    :settlementCurrency, :tickSize, :payoutPerContract, :closesAt, :resolvesAt,
                                    CAST(:jurisdictionTags AS JSONB), :category, CAST(:tags AS JSONB),
                                    :cardImageUrl, :displayOrder,
                                    :feeModelId, :feeScheduleVersion, CAST(:feeParamsJson AS JSONB),
                                    :retailFeeModelId, :eventId, :outcomeLabel, :outcomeDisplayOrder
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
                                jurisdictionTags,
                                category,
                                tags,
                                cardImageUrl,
                                displayOrder,
                                feeModelId,
                                feeScheduleVersion,
                                feeParamsJson,
                                retailFeeModelId,
                                eventId,
                                outcomeLabel,
                                outcomeDisplayOrder),
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
            List<String> jurisdictionTags,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String feeModelId,
            Integer feeScheduleVersion,
            String feeParamsJson,
            String retailFeeModelId,
            Long eventId,
            String outcomeLabel,
            Integer outcomeDisplayOrder) {
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
                            jurisdiction_tags = CAST(:jurisdictionTags AS JSONB),
                            category = :category,
                            tags = CAST(:tags AS JSONB),
                            card_image_url = :cardImageUrl,
                            display_order = :displayOrder,
                            fee_model_id = COALESCE(:feeModelId, fee_model_id),
                            fee_schedule_version = COALESCE(:feeScheduleVersion, fee_schedule_version),
                            fee_params_json = COALESCE(CAST(:feeParamsJson AS JSONB), fee_params_json),
                            retail_fee_model_id = :retailFeeModelId,
                            event_id = :eventId,
                            outcome_label = :outcomeLabel,
                            outcome_display_order = COALESCE(:outcomeDisplayOrder, outcome_display_order)
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
                                jurisdictionTags,
                                category,
                                tags,
                                cardImageUrl,
                                displayOrder,
                                feeModelId,
                                feeScheduleVersion,
                                feeParamsJson,
                                retailFeeModelId,
                                eventId,
                                outcomeLabel,
                                outcomeDisplayOrder)
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
            List<String> jurisdictionTags,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String feeModelId,
            Integer feeScheduleVersion,
            String feeParamsJson,
            String retailFeeModelId,
            Long eventId,
            String outcomeLabel,
            Integer outcomeDisplayOrder) {
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
        params.addValue("category", category);
        params.addValue(
                "tags",
                PredictionMarketCatalogTags.toJson(
                        tags == null ? PredictionMarketCatalogTags.empty() : tags));
        params.addValue("cardImageUrl", cardImageUrl);
        params.addValue("displayOrder", displayOrder);
        params.addValue("feeModelId", feeModelId == null ? "ZERO" : feeModelId);
        params.addValue("feeScheduleVersion", feeScheduleVersion == null ? 1 : feeScheduleVersion);
        params.addValue("feeParamsJson", feeParamsJson == null || feeParamsJson.isBlank() ? "{}" : feeParamsJson);
        params.addValue("retailFeeModelId", retailFeeModelId);
        params.addValue("eventId", eventId);
        params.addValue("outcomeLabel", outcomeLabel);
        params.addValue("outcomeDisplayOrder", outcomeDisplayOrder == null ? 0 : outcomeDisplayOrder);
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
                PredictionMarketJurisdictionTags.fromJson(rs.getString("jurisdiction_tags")),
                rs.getString("category"),
                PredictionMarketCatalogTags.fromJson(rs.getString("tags")),
                rs.getString("card_image_url"),
                rs.getInt("display_order"),
                rs.getString("fee_model_id"),
                rs.getInt("fee_schedule_version"),
                rs.getString("fee_params_json"),
                rs.getString("retail_fee_model_id"),
                rs.getObject("event_id") == null ? null : rs.getLong("event_id"),
                rs.getString("event_slug"),
                rs.getString("outcome_label"),
                rs.getInt("outcome_display_order"));
    }
}
