package com.balh.oms.predictionmarket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Validates {@code tags} JSON on prediction-market contracts (retail catalog labels). */
public final class PredictionMarketCatalogTags {

    public static final int MAX_TAGS = 12;
    public static final int MAX_TAG_LENGTH = 32;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() {};

    private PredictionMarketCatalogTags() {}

    public static List<String> empty() {
        return List.of();
    }

    public static List<String> normalize(List<String> raw) {
        if (raw == null || raw.isEmpty()) {
            return empty();
        }
        if (raw.size() > MAX_TAGS) {
            throw new IllegalArgumentException("tags exceeds " + MAX_TAGS + " items");
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String item : raw) {
            if (item == null || item.isBlank()) {
                continue;
            }
            String tag = item.trim().toLowerCase(Locale.ROOT);
            if (tag.length() > MAX_TAG_LENGTH) {
                throw new IllegalArgumentException("tag too long: " + item);
            }
            if (!tag.matches("[a-z0-9][a-z0-9-]*")) {
                throw new IllegalArgumentException(
                        "tags must be lowercase alphanumeric with optional hyphens: " + item);
            }
            seen.add(tag);
        }
        return List.copyOf(seen);
    }

    public static String toJson(List<String> tags) {
        try {
            return MAPPER.writeValueAsString(normalize(tags));
        } catch (Exception e) {
            throw new IllegalArgumentException("tags serialization failed", e);
        }
    }

    public static List<String> fromJson(String json) {
        if (json == null || json.isBlank()) {
            return empty();
        }
        try {
            List<String> parsed = MAPPER.readValue(json, STRING_LIST);
            return parsed == null ? empty() : List.copyOf(normalize(parsed));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("tags JSON invalid", e);
        }
    }
}
