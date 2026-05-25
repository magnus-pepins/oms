package com.balh.oms.corporateaction;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;

/** Cost-basis allocation for merger and spin-off position impacts (gap plan §5.9 Phase 2). */
public final class CorporateActionCostBasisAllocator {

    private static final int COST_SCALE = 10;

    public record CostBasisSplit(BigDecimal parentCostAfter, BigDecimal childCostAfter, String method) {}

    private CorporateActionCostBasisAllocator() {}

    public static CostBasisSplit merger(
            BigDecimal parentCostBefore,
            BigDecimal parentQtyBefore,
            BigDecimal survivorQtyAfter,
            JsonNode payload) {
        if (parentCostBefore == null || parentCostBefore.signum() <= 0) {
            return new CostBasisSplit(null, null, null);
        }
        String method = textField(payload, "costBasisAllocationMethod");
        if (method == null || method.isBlank()) {
            method = "PROPORTIONAL_SHARES";
        }
        method = method.trim().toUpperCase(Locale.ROOT);
        if ("ALL_TO_SURVIVOR".equals(method)) {
            return new CostBasisSplit(BigDecimal.ZERO.setScale(COST_SCALE), parentCostBefore, method);
        }
        if (survivorQtyAfter == null || survivorQtyAfter.signum() <= 0 || parentQtyBefore.signum() <= 0) {
            return new CostBasisSplit(parentCostBefore, null, method);
        }
        BigDecimal survivorFraction =
                survivorQtyAfter.divide(parentQtyBefore, COST_SCALE, RoundingMode.HALF_UP);
        if (survivorFraction.compareTo(BigDecimal.ONE) > 0) {
            survivorFraction = BigDecimal.ONE;
        }
        BigDecimal survivorCost =
                parentCostBefore.multiply(survivorFraction).setScale(COST_SCALE, RoundingMode.HALF_UP);
        BigDecimal parentRemainder = parentCostBefore.subtract(survivorCost).max(BigDecimal.ZERO);
        return new CostBasisSplit(parentRemainder, survivorCost, method);
    }

    public static CostBasisSplit spinOff(
            BigDecimal parentCostBefore,
            BigDecimal parentRetentionRatio,
            BigDecimal spinOffRatio,
            JsonNode payload) {
        if (parentCostBefore == null || parentCostBefore.signum() <= 0) {
            return new CostBasisSplit(null, null, null);
        }
        String method = textField(payload, "costBasisAllocationMethod");
        if (method == null || method.isBlank()) {
            method = "SPIN_OFF_FRACTION";
        }
        method = method.trim().toUpperCase(Locale.ROOT);
        BigDecimal childFraction = decimalField(payload, "spinOffCostBasisFraction");
        if (childFraction == null) {
            if (parentRetentionRatio != null && parentRetentionRatio.signum() >= 0) {
                childFraction = BigDecimal.ONE.subtract(parentRetentionRatio);
            } else if (spinOffRatio != null && spinOffRatio.signum() > 0) {
                childFraction =
                        spinOffRatio.divide(
                                spinOffRatio.add(BigDecimal.ONE), COST_SCALE, RoundingMode.HALF_UP);
            } else {
                childFraction = BigDecimal.ZERO;
            }
        }
        if (childFraction.compareTo(BigDecimal.ONE) > 0) {
            childFraction = BigDecimal.ONE;
        }
        if (childFraction.signum() < 0) {
            childFraction = BigDecimal.ZERO;
        }
        BigDecimal childCost =
                parentCostBefore.multiply(childFraction).setScale(COST_SCALE, RoundingMode.HALF_UP);
        BigDecimal parentCostAfter = parentCostBefore.subtract(childCost).max(BigDecimal.ZERO);
        return new CostBasisSplit(parentCostAfter, childCost, method);
    }

    private static String textField(JsonNode payload, String name) {
        if (payload == null || !payload.has(name) || payload.get(name).isNull()) {
            return null;
        }
        return payload.get(name).asText();
    }

    private static BigDecimal decimalField(JsonNode payload, String name) {
        if (payload == null || !payload.has(name) || payload.get(name).isNull()) {
            return null;
        }
        JsonNode node = payload.get(name);
        if (node.isNumber()) {
            return node.decimalValue();
        }
        if (node.isTextual()) {
            try {
                return new BigDecimal(node.asText().trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
