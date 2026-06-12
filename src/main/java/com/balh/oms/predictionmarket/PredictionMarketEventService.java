package com.balh.oms.predictionmarket;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Service;

/** Operator catalog CRUD for {@link PredictionMarketEventRepository}. */
@Service
public class PredictionMarketEventService {

    private static final Set<String> ALLOWED_STATUS =
            Set.of("DRAFT", "OPEN", "HALTED", "CLOSED");

    private final PredictionMarketEventRepository repository;

    public PredictionMarketEventService(PredictionMarketEventRepository repository) {
        this.repository = repository;
    }

    public record CreateRequest(
            String slug,
            String title,
            String description,
            String category,
            List<String> tags,
            String cardImageUrl,
            Integer displayOrder,
            String status) {}

    public record UpdateRequest(
            String title,
            String description,
            String category,
            List<String> tags,
            String cardImageUrl,
            Integer displayOrder,
            String status) {}

    public PredictionMarketEventRepository.EventRow create(CreateRequest req) {
        String slug = normalizeSlug(req.slug());
        String title = requireNonBlank(req.title(), "title");
        String status = normalizeStatus(req.status(), "DRAFT");
        String description = PredictionMarketReferenceLinks.normalizeDescription(req.description());
        String category = PredictionMarketCatalogPresentation.normalizeCategory(req.category());
        List<String> tags = PredictionMarketCatalogTags.normalize(req.tags());
        String cardImageUrl =
                PredictionMarketCatalogPresentation.normalizeCardImageUrl(req.cardImageUrl());
        int displayOrder = PredictionMarketCatalogPresentation.normalizeDisplayOrder(req.displayOrder());
        return repository.insert(slug, title, description, category, tags, cardImageUrl, displayOrder, status);
    }

    public Optional<PredictionMarketEventRepository.EventRow> update(long id, UpdateRequest req) {
        return repository
                .findById(id)
                .map(
                        existing -> {
                            String title =
                                    req.title() != null ? requireNonBlank(req.title(), "title") : existing.title();
                            String description =
                                    req.description() != null
                                            ? PredictionMarketReferenceLinks.normalizeDescription(req.description())
                                            : existing.description();
                            String category =
                                    req.category() != null
                                            ? PredictionMarketCatalogPresentation.normalizeCategory(req.category())
                                            : existing.category();
                            List<String> tags =
                                    req.tags() != null
                                            ? PredictionMarketCatalogTags.normalize(req.tags())
                                            : existing.tags();
                            String cardImageUrl =
                                    req.cardImageUrl() != null
                                            ? PredictionMarketCatalogPresentation.normalizeCardImageUrl(
                                                    req.cardImageUrl())
                                            : existing.cardImageUrl();
                            int displayOrder =
                                    req.displayOrder() != null
                                            ? PredictionMarketCatalogPresentation.normalizeDisplayOrder(
                                                    req.displayOrder())
                                            : existing.displayOrder();
                            String status =
                                    req.status() != null
                                            ? normalizeStatus(req.status(), existing.status())
                                            : existing.status();
                            return repository.update(
                                    id,
                                    title,
                                    description,
                                    category,
                                    tags,
                                    cardImageUrl,
                                    displayOrder,
                                    status);
                        });
    }

    public static String normalizeOutcomeLabel(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String label = raw.trim();
        if (label.length() > 120) {
            throw new IllegalArgumentException("outcomeLabel must be at most 120 characters");
        }
        return label;
    }

    public static int normalizeOutcomeDisplayOrder(Integer raw) {
        return raw == null ? 0 : PredictionMarketCatalogPresentation.normalizeDisplayOrder(raw);
    }

    public static Long resolveEventId(
            PredictionMarketEventRepository eventRepository, Long eventId, String eventSlug) {
        if (eventId != null) {
            if (eventRepository.findById(eventId).isEmpty()) {
                throw new IllegalArgumentException("eventId not found");
            }
            return eventId;
        }
        if (eventSlug != null && !eventSlug.isBlank()) {
            return eventRepository
                    .findBySlug(eventSlug.trim())
                    .map(PredictionMarketEventRepository.EventRow::id)
                    .orElseThrow(() -> new IllegalArgumentException("eventSlug not found"));
        }
        return null;
    }

    private static String normalizeSlug(String raw) {
        String slug = requireNonBlank(raw, "slug").toLowerCase(Locale.ROOT);
        if (!slug.matches("[a-z0-9][a-z0-9-]{0,62}")) {
            throw new IllegalArgumentException("slug must be lowercase alphanumeric with hyphens");
        }
        return slug;
    }

    private static String normalizeStatus(String raw, String defaultStatus) {
        String status = raw == null || raw.isBlank() ? defaultStatus : raw.trim().toUpperCase(Locale.ROOT);
        if (!ALLOWED_STATUS.contains(status)) {
            throw new IllegalArgumentException("status must be one of " + ALLOWED_STATUS);
        }
        return status;
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value.trim();
    }
}
