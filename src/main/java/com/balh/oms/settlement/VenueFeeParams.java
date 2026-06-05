package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;

/** Parsed {@code prediction_market_contract.fee_params_json} or override row. */
public record VenueFeeParams(
        BigDecimal takerBps,
        BigDecimal makerBps,
        BigDecimal symmetricBps,
        BigDecimal takerK,
        BigDecimal makerK,
        boolean makerFeesEnabled) {

    private static final BigDecimal DEFAULT_K_TAKER = new BigDecimal("0.07");
    private static final BigDecimal DEFAULT_K_MAKER = new BigDecimal("0.0175");

    public static VenueFeeParams empty() {
        return new VenueFeeParams(
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                DEFAULT_K_TAKER,
                DEFAULT_K_MAKER,
                false);
    }

    public static VenueFeeParams fromJson(String json, ObjectMapper mapper) {
        if (json == null || json.isBlank()) {
            return empty();
        }
        try {
            JsonNode n = mapper.readTree(json);
            return new VenueFeeParams(
                    decimal(n, "taker_bps", BigDecimal.ZERO),
                    decimal(n, "maker_bps", BigDecimal.ZERO),
                    decimal(n, "symmetric_bps", BigDecimal.ZERO),
                    decimal(n, "taker_k", DEFAULT_K_TAKER),
                    decimal(n, "maker_k", DEFAULT_K_MAKER),
                    n.path("maker_fees_enabled").asBoolean(false));
        } catch (Exception e) {
            return empty();
        }
    }

    private static BigDecimal decimal(JsonNode n, String field, BigDecimal fallback) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) {
            return fallback;
        }
        if (v.isNumber()) {
            return v.decimalValue();
        }
        try {
            return new BigDecimal(v.asText().trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
