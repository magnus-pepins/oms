package com.balh.oms.marketdata;

import java.math.BigDecimal;
import java.time.Instant;

/** NBBO-class bid/ask from Marketdata HTTP (mapped into {@link com.balh.oms.returnpath.MarketContextVenueEvidence}). */
public record MarketdataNbboQuote(BigDecimal bid, BigDecimal ask, Instant asOf) {}
