package com.balh.oms.fixin;

/** Normalized cancel fields from FIX 35=F. */
public record FixInParsedCancel(
        String clientClOrdId, String origClientClOrdId, byte sideCode, String instrumentSymbol) {}
