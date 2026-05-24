package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Detects broker EOD file envelope kind for drop-folder and multipart routing.
 *
 * <p>Order matters: non-confirm envelopes are checked before trade-confirm {@code rows} shapes.
 */
public final class SettlementBrokerFileFormat {

    public enum Kind {
        V0_FIXTURE,
        V2_ECONOMIC,
        POSITION_SNAPSHOT,
        CASH_STATEMENT,
        CORPORATE_ACTION,
        SETTLEMENT_FAIL,
        INVALID
    }

    private SettlementBrokerFileFormat() {}

    public static Kind detect(ObjectMapper objectMapper, byte[] fileBytes) {
        if (fileBytes == null || fileBytes.length == 0) {
            return Kind.INVALID;
        }
        try {
            JsonNode tree = objectMapper.readTree(fileBytes);
            if (tree.has("events") && tree.get("events").isArray() && !tree.get("events").isEmpty()) {
                return Kind.CORPORATE_ACTION;
            }
            if (tree.has("fails") && tree.get("fails").isArray() && !tree.get("fails").isEmpty()) {
                return Kind.SETTLEMENT_FAIL;
            }
            if (tree.has("movements") && tree.get("movements").isArray()) {
                return Kind.CASH_STATEMENT;
            }
            JsonNode rows = tree.get("rows");
            if (rows != null && rows.isArray() && !rows.isEmpty()) {
                JsonNode first = rows.get(0);
                if (first != null && !first.isNull() && first.has("quantityTotal")) {
                    return Kind.POSITION_SNAPSHOT;
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
            }
            return Kind.INVALID;
        } catch (IOException e) {
            return Kind.INVALID;
        }
    }
}
