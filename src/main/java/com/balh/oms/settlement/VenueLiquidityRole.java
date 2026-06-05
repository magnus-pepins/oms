package com.balh.oms.settlement;

import com.balh.venue.grpc.v1.LiquidityRole;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Maker/taker role for a venue fill leg (Phase E). */
public enum VenueLiquidityRole {
    UNSPECIFIED,
    TAKER,
    MAKER;

    public static VenueLiquidityRole fromExecutionEnvelope(String rawJson, ObjectMapper mapper) {
        if (rawJson == null || rawJson.isBlank()) {
            return UNSPECIFIED;
        }
        try {
            JsonNode n = mapper.readTree(rawJson);
            String role = n.path("liquidityRole").asText("");
            if (role.isBlank()) {
                role = n.path("liquidity_role").asText("");
            }
            if ("MAKER".equalsIgnoreCase(role)) {
                return MAKER;
            }
            if ("TAKER".equalsIgnoreCase(role)) {
                return TAKER;
            }
            if ("balh-venue-maker-fill".equals(n.path("source").asText())) {
                return MAKER;
            }
            if ("venue-grpc".equals(n.path("source").asText()) && n.path("execType").asInt() == 1) {
                return TAKER;
            }
        } catch (Exception ignored) {
            // fall through
        }
        return UNSPECIFIED;
    }

    public static VenueLiquidityRole fromGrpc(LiquidityRole role) {
        return switch (role) {
            case LIQUIDITY_ROLE_TAKER -> TAKER;
            case LIQUIDITY_ROLE_MAKER -> MAKER;
            default -> UNSPECIFIED;
        };
    }

    public char fixLastLiquidityInd() {
        return switch (this) {
            case MAKER -> '1';
            case TAKER -> '2';
            default -> '0';
        };
    }
}
