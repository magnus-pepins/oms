package com.balh.oms.fixin;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.balh.oms.config.OmsProfiles;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;

/**
 * Maps inbound FIX {@code Symbol(55)} tags to OMS {@code instrument_symbol}. Separate from broker-facing
 * {@link com.balh.oms.fix.FixSymbolMapper} because client symbols and venue symbols may differ.
 */
@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInSymbolMapper {

    private final Map<String, String> clientToOms;

    public FixInSymbolMapper(OmsConfig omsConfig, ObjectMapper objectMapper) {
        String json = omsConfig.getFixIn().getSymbolMapJson();
        if (json == null || json.isBlank()) {
            this.clientToOms = Collections.emptyMap();
            return;
        }
        try {
            Map<String, String> raw =
                    objectMapper.readValue(json, new TypeReference<>() {});
            this.clientToOms = raw == null ? Collections.emptyMap() : raw;
        } catch (Exception e) {
            throw new IllegalStateException("invalid oms.fix-in.symbol-map-json", e);
        }
    }

    public String toOmsSymbol(String clientSymbol) {
        if (clientSymbol == null || clientSymbol.isBlank()) {
            throw new FixInParseException("symbol_required");
        }
        String trimmed = clientSymbol.trim();
        for (Map.Entry<String, String> e : clientToOms.entrySet()) {
            if (e.getKey().equalsIgnoreCase(trimmed)) {
                return e.getValue();
            }
        }
        return trimmed.toUpperCase(Locale.ROOT);
    }
}
