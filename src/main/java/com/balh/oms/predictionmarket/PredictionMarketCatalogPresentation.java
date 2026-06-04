package com.balh.oms.predictionmarket;

import java.net.URI;
import java.util.Locale;

/** Category, card image, and sort order for retail catalog presentation. */
public final class PredictionMarketCatalogPresentation {

    public static final int MAX_CATEGORY_LENGTH = 64;
    public static final int MAX_CARD_IMAGE_URL_LENGTH = 2048;
    public static final int MIN_DISPLAY_ORDER = -999_999;
    public static final int MAX_DISPLAY_ORDER = 999_999;

    private PredictionMarketCatalogPresentation() {}

    public static String normalizeCategory(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String category = raw.trim();
        if (category.length() > MAX_CATEGORY_LENGTH) {
            throw new IllegalArgumentException("category exceeds " + MAX_CATEGORY_LENGTH + " characters");
        }
        return category;
    }

    public static String normalizeCardImageUrl(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String url = raw.trim();
        if (url.length() > MAX_CARD_IMAGE_URL_LENGTH) {
            throw new IllegalArgumentException("cardImageUrl exceeds " + MAX_CARD_IMAGE_URL_LENGTH + " characters");
        }
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("cardImageUrl must be a valid URL", e);
        }
        if (!"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("cardImageUrl must use https");
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("cardImageUrl must have a host");
        }
        return url;
    }

    public static int normalizeDisplayOrder(Integer raw) {
        if (raw == null) {
            return 0;
        }
        int order = raw;
        if (order < MIN_DISPLAY_ORDER || order > MAX_DISPLAY_ORDER) {
            throw new IllegalArgumentException(
                    "displayOrder must be between " + MIN_DISPLAY_ORDER + " and " + MAX_DISPLAY_ORDER);
        }
        return order;
    }
}
