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
        String instrumentSymbol) {}
