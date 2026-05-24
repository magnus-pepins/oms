package com.balh.oms.fixin;

import java.math.BigDecimal;

/** Normalized replace fields from FIX 35=G. */
public record FixInParsedReplace(
        String clientClOrdId,
        String origClientClOrdId,
        byte sideCode,
        byte ordTypeCode,
        BigDecimal quantity,
        BigDecimal limitPriceOrNull,
        String instrumentSymbol) {}
