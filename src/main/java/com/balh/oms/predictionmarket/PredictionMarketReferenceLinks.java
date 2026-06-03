package com.balh.oms.predictionmarket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Validates and serializes {@code reference_links} JSON on prediction-market contracts. */
public final class PredictionMarketReferenceLinks {

    public record Link(String label, String url) {}

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<Link>> LINK_LIST = new TypeReference<>() {};

    private PredictionMarketReferenceLinks() {}

    public static List<Link> empty() {
        return List.of();
    }

    public static String normalizeDescription(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.length() > PredictionMarketContractContentLimits.MAX_DESCRIPTION_CHARS) {
            throw new IllegalArgumentException(
                    "description exceeds "
                            + PredictionMarketContractContentLimits.MAX_DESCRIPTION_CHARS
                            + " characters");
        }
        return trimmed;
    }

    public static String normalizeResolutionCriteria(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.length() > PredictionMarketContractContentLimits.MAX_RESOLUTION_CRITERIA_CHARS) {
            throw new IllegalArgumentException(
                    "resolutionCriteria exceeds "
                            + PredictionMarketContractContentLimits.MAX_RESOLUTION_CRITERIA_CHARS
                            + " characters");
        }
        return trimmed;
    }

    public static List<Link> normalize(List<Link> raw) {
        if (raw == null || raw.isEmpty()) {
            return empty();
        }
        if (raw.size() > PredictionMarketContractContentLimits.MAX_REFERENCE_LINKS) {
            throw new IllegalArgumentException(
                    "referenceLinks exceeds "
                            + PredictionMarketContractContentLimits.MAX_REFERENCE_LINKS
                            + " items");
        }
        List<Link> out = new ArrayList<>(raw.size());
        for (Link link : raw) {
            if (link == null) {
                continue;
            }
            String label = requireLabel(link.label());
            String url = requireHttpsUrl(link.url());
            out.add(new Link(label, url));
        }
        return List.copyOf(out);
    }

    public static String toJson(List<Link> links) {
        try {
            return MAPPER.writeValueAsString(links == null ? empty() : links);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize referenceLinks", e);
        }
    }

    public static List<Link> fromJson(String json) {
        if (json == null || json.isBlank()) {
            return empty();
        }
        try {
            List<Link> parsed = MAPPER.readValue(json, LINK_LIST);
            return parsed == null ? empty() : List.copyOf(parsed);
        } catch (Exception e) {
            throw new IllegalArgumentException("referenceLinks must be a JSON array of {label, url}");
        }
    }

    private static String requireLabel(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("referenceLinks[].label is required");
        }
        String label = raw.trim();
        if (label.length() > PredictionMarketContractContentLimits.MAX_REFERENCE_LINK_LABEL_CHARS) {
            throw new IllegalArgumentException("referenceLinks[].label is too long");
        }
        return label;
    }

    private static String requireHttpsUrl(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("referenceLinks[].url is required");
        }
        String url = raw.trim();
        if (url.length() > PredictionMarketContractContentLimits.MAX_REFERENCE_LINK_URL_CHARS) {
            throw new IllegalArgumentException("referenceLinks[].url is too long");
        }
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("referenceLinks[].url is not a valid URL");
        }
        if (!"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("referenceLinks[].url must use https");
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("referenceLinks[].url must include a host");
        }
        return url;
    }
}
