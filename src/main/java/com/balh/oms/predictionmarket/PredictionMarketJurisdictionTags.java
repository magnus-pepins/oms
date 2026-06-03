package com.balh.oms.predictionmarket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Validates {@code jurisdiction_tags} JSON on prediction-market contracts. */
public final class PredictionMarketJurisdictionTags {

    public static final int MAX_TAGS = 50;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() {};

    private PredictionMarketJurisdictionTags() {}

    public static List<String> empty() {
        return List.of();
    }

    public static List<String> normalize(List<String> raw) {
        if (raw == null || raw.isEmpty()) {
            return empty();
        }
        if (raw.size() > MAX_TAGS) {
            throw new IllegalArgumentException("jurisdictionTags exceeds " + MAX_TAGS + " items");
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String item : raw) {
            if (item == null || item.isBlank()) {
                continue;
            }
            String code = item.trim().toUpperCase(Locale.ROOT);
            if (!code.matches("[A-Z]{2}")) {
                throw new IllegalArgumentException("jurisdictionTags must be ISO-3166 alpha-2: " + item);
            }
            seen.add(code);
        }
        return List.copyOf(seen);
    }

    public static String toJson(List<String> tags) {
        try {
            return MAPPER.writeValueAsString(normalize(tags));
        } catch (Exception e) {
            throw new IllegalArgumentException("jurisdictionTags serialization failed", e);
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
            throw new IllegalArgumentException("jurisdictionTags JSON invalid", e);
        }
    }
}
