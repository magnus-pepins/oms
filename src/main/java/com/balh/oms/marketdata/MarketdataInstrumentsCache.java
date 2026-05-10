package com.balh.oms.marketdata;

import com.balh.oms.config.OmsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Refreshes tradable symbols from {@link MarketdataPlatformHttpClient} on a fixed delay when
 * {@code oms.marketdata.enabled=true}.
 */
@Component
public class MarketdataInstrumentsCache {

    private static final Logger log = LoggerFactory.getLogger(MarketdataInstrumentsCache.class);

    private final AtomicReference<Set<String>> symbols = new AtomicReference<>(Set.of());
    private final OmsConfig config;
    private final ObjectProvider<MarketdataPlatformHttpClient> client;

    public MarketdataInstrumentsCache(OmsConfig config, ObjectProvider<MarketdataPlatformHttpClient> client) {
        this.config = config;
        this.client = client;
    }

    /** Latest snapshot (may be empty before first successful refresh or when integration is off). */
    public Set<String> getSymbols() {
        return symbols.get();
    }

    @Scheduled(fixedDelayString = "${oms.marketdata.instruments-refresh-interval-ms:60000}")
    public void refresh() {
        if (!config.getMarketdata().isEnabled()) {
            return;
        }
        MarketdataPlatformHttpClient c = client.getIfAvailable();
        if (c == null) {
            return;
        }
        try {
            Set<String> next = c.fetchInstrumentSymbols();
            symbols.set(next == null || next.isEmpty() ? Set.of() : next);
        } catch (RuntimeException e) {
            log.warn("marketdata instruments refresh failed: {}", e.getMessage());
        }
    }
}
