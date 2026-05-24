package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Detects v0 fixture vs v2 economic broker confirm envelopes for drop-folder and multipart routing.
 */
public final class SettlementBrokerFileFormat {

    public enum Kind {
        V0_FIXTURE,
        V2_ECONOMIC,
        INVALID
    }

    private SettlementBrokerFileFormat() {}

    public static Kind detect(ObjectMapper objectMapper, byte[] fileBytes) {
        if (fileBytes == null || fileBytes.length == 0) {
            return Kind.INVALID;
        }
        try {
            JsonNode tree = objectMapper.readTree(fileBytes);
            JsonNode rows = tree.get("rows");
            if (rows == null || !rows.isArray() || rows.isEmpty()) {
                return Kind.INVALID;
            }
            JsonNode first = rows.get(0);
            if (first == null || first.isNull()) {
                return Kind.INVALID;
            }
            if (tree.has("schemaVersion")
                    && tree.has("brokerId")
                    && tree.has("fileId")
                    && first.has("brokerTradeId")
                    && first.has("instrument")) {
                return Kind.V2_ECONOMIC;
            }
            if (first.has("executionId")) {
                return Kind.V0_FIXTURE;
            }
            if (first.has("accountId") && first.has("venueExecRef")) {
                return Kind.V0_FIXTURE;
            }
            return Kind.INVALID;
        } catch (IOException e) {
            return Kind.INVALID;
        }
    }
}
