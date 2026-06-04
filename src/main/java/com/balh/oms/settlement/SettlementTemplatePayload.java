package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Reads {@code template} / {@code templateVersion} from outbox {@code payload_json}. */
public final class SettlementTemplatePayload {

    private SettlementTemplatePayload() {}

    public static void enrich(ObjectNode payload, String templateId, int version) {
        payload.put("template", templateId);
        payload.put("templateVersion", version);
    }

    public static String templateId(JsonNode payload, String defaultTemplateId) {
        if (payload != null && payload.hasNonNull("template")) {
            String t = payload.get("template").asText("").trim();
            if (!t.isEmpty()) {
                return t;
            }
        }
        return defaultTemplateId;
    }

    public static int templateVersion(JsonNode payload, int defaultVersion) {
        if (payload != null && payload.has("templateVersion")) {
            return payload.get("templateVersion").asInt(defaultVersion);
        }
        return defaultVersion;
    }
}
