package com.balh.oms.predictionmarket;

import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PredictionMarketEventRepository {

    public record EventRow(
            long id,
            String slug,
            String title,
            String description,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String status) {}

    private static final String SELECT_COLUMNS =
            """
                    id, slug, title, description, category, tags, card_image_url, display_order, status
                    """;

    private static final String LIST_OPEN =
            """
                    SELECT %s
                    FROM prediction_market_event
                    WHERE status = 'OPEN'
                    ORDER BY display_order ASC, id ASC
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String LIST_ALL_UNFILTERED =
            """
                    SELECT %s
                    FROM prediction_market_event
                    ORDER BY display_order ASC, id DESC
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String LIST_ALL_BY_STATUS =
            """
                    SELECT %s
                    FROM prediction_market_event
                    WHERE status = :status
                    ORDER BY display_order ASC, id DESC
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String FIND_BY_ID =
            """
                    SELECT %s
                    FROM prediction_market_event
                    WHERE id = :id
                    """
                    .formatted(SELECT_COLUMNS);

    private static final String FIND_BY_SLUG =
            """
                    SELECT %s
                    FROM prediction_market_event
                    WHERE slug = :slug
                    """
                    .formatted(SELECT_COLUMNS);

    private final NamedParameterJdbcTemplate jdbc;

    public PredictionMarketEventRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<EventRow> listOpen() {
        return jdbc.query(LIST_OPEN, new MapSqlParameterSource(), this::mapRow);
    }

    public List<EventRow> listAll(String statusOrNull) {
        if (statusOrNull == null || statusOrNull.isBlank()) {
            return jdbc.query(LIST_ALL_UNFILTERED, new MapSqlParameterSource(), this::mapRow);
        }
        return jdbc.query(
                LIST_ALL_BY_STATUS,
                new MapSqlParameterSource("status", statusOrNull.trim().toUpperCase()),
                this::mapRow);
    }

    public Optional<EventRow> findById(long id) {
        List<EventRow> rows =
                jdbc.query(FIND_BY_ID, new MapSqlParameterSource("id", id), this::mapRow);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    public Optional<EventRow> findBySlug(String slug) {
        if (slug == null || slug.isBlank()) {
            return Optional.empty();
        }
        List<EventRow> rows =
                jdbc.query(
                        FIND_BY_SLUG,
                        new MapSqlParameterSource("slug", slug.trim()),
                        this::mapRow);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    public EventRow insert(
            String slug,
            String title,
            String description,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String status) {
        Long id =
                jdbc.queryForObject(
                        """
                                INSERT INTO prediction_market_event (
                                    slug, title, description, category, tags, card_image_url,
                                    display_order, status
                                ) VALUES (
                                    :slug, :title, :description, :category, CAST(:tags AS JSONB),
                                    :cardImageUrl, :displayOrder, :status
                                )
                                RETURNING id
                                """,
                        bindWrite(slug, title, description, category, tags, cardImageUrl, displayOrder, status),
                        Long.class);
        return findById(id).orElseThrow();
    }

    public EventRow update(
            long id,
            String title,
            String description,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String status) {
        jdbc.update(
                """
                        UPDATE prediction_market_event
                        SET title = :title,
                            description = :description,
                            category = :category,
                            tags = CAST(:tags AS JSONB),
                            card_image_url = :cardImageUrl,
                            display_order = :displayOrder,
                            status = :status
                        WHERE id = :id
                        """,
                bindWrite(null, title, description, category, tags, cardImageUrl, displayOrder, status)
                        .addValue("id", id));
        return findById(id).orElseThrow();
    }

    private MapSqlParameterSource bindWrite(
            String slug,
            String title,
            String description,
            String category,
            List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String status) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        if (slug != null) {
            params.addValue("slug", slug);
        }
        params.addValue("title", title);
        params.addValue("description", description);
        params.addValue("category", category);
        params.addValue(
                "tags",
                PredictionMarketCatalogTags.toJson(
                        tags == null ? PredictionMarketCatalogTags.empty() : tags));
        params.addValue("cardImageUrl", cardImageUrl);
        params.addValue("displayOrder", displayOrder);
        params.addValue("status", status);
        return params;
    }

    private EventRow mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new EventRow(
                rs.getLong("id"),
                rs.getString("slug"),
                rs.getString("title"),
                rs.getString("description"),
                rs.getString("category"),
                PredictionMarketCatalogTags.fromJson(rs.getString("tags")),
                rs.getString("card_image_url"),
                rs.getInt("display_order"),
                rs.getString("status"));
    }
}
