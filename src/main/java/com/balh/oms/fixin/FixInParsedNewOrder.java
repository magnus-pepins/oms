package com.balh.oms.fixin;

import java.math.BigDecimal;

/** Normalized new-order fields parsed from FIX 35=D before cluster admission. */
public record FixInParsedNewOrder(
        String clientClOrdId,
        String fixAccountTagOrEmpty,
        byte sideCode,
        byte timeInForceCode,
        byte ordTypeCode,
        BigDecimal quantity,
        BigDecimal limitPriceOrNull,
        String instrumentSymbol,
        /** Generic portfolio attribution from the configured FIX tag (default 5001); {@code null} when absent. */
        String portfolioIdOrNull) {

    /** Back-compat constructor for callers that pre-date the FIX-in portfolio tag. */
    public FixInParsedNewOrder(
            String clientClOrdId,
            String fixAccountTagOrEmpty,
            byte sideCode,
            byte timeInForceCode,
            byte ordTypeCode,
            BigDecimal quantity,
            BigDecimal limitPriceOrNull,
            String instrumentSymbol) {
        this(clientClOrdId, fixAccountTagOrEmpty, sideCode, timeInForceCode, ordTypeCode,
                quantity, limitPriceOrNull, instrumentSymbol, null);
    }
}
