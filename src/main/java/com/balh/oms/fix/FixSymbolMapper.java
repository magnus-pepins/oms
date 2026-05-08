package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * Broker-specific {@code Symbol} mapping for outbound FIX {@code NewOrderSingle} (slice 5 prep).
 * Config: {@code oms.fix.symbol-map-json} — JSON object whose keys are OMS {@code instrument_symbol} (matched
 * case-insensitively) and values are the broker {@code Symbol} tag.
 */
@Component
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixSymbolMapper {

    private final Map<String, String> venueSymbolByClientKey;

    public FixSymbolMapper(OmsConfig omsConfig, ObjectMapper objectMapper) {
        this.venueSymbolByClientKey = parseSymbolMap(omsConfig.getFix().getSymbolMapJson(), objectMapper);
    }

    /**
     * @return broker {@code Symbol} string, or trimmed {@code instrumentSymbol} when unmapped
     */
    public String toVenueSymbol(String instrumentSymbol) {
        if (instrumentSymbol == null || instrumentSymbol.isBlank()) {
            return "";
        }
        String trimmed = instrumentSymbol.trim();
        String mapped = venueSymbolByClientKey.get(normalizeKey(trimmed));
        return mapped != null ? mapped : trimmed;
    }

    static Map<String, String> parseSymbolMap(String rawJson, ObjectMapper objectMapper) {
        String trimmed = rawJson == null ? "" : rawJson.trim();
        if (trimmed.isEmpty() || "{}".equals(trimmed)) {
            return Map.of();
        }
        try {
            JsonNode root = objectMapper.readTree(trimmed);
            if (!root.isObject()) {
                throw new IllegalStateException("oms.fix.symbol-map-json must be a JSON object, got: " + root.getNodeType());
            }
            Map<String, String> out = new HashMap<>();
            for (Iterator<String> it = root.fieldNames(); it.hasNext(); ) {
                String key = it.next();
                JsonNode v = root.get(key);
                if (!v.isTextual()) {
                    throw new IllegalStateException(
                            "oms.fix.symbol-map-json values must be JSON strings (key=%s)".formatted(key));
                }
                out.put(normalizeKey(key), v.asText());
            }
            return Map.copyOf(out);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("oms.fix.symbol-map-json is not valid JSON", e);
        }
    }

    private static String normalizeKey(String key) {
        return key.trim().toUpperCase(Locale.ROOT);
    }
}
