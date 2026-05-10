package com.balh.oms.marketdata;

import java.util.Optional;
import java.util.Set;

public interface MarketdataPlatformHttpClient {

    Set<String> fetchInstrumentSymbols();

    Optional<MarketdataNbboQuote> fetchNbbo(String instrumentSymbol);
}
