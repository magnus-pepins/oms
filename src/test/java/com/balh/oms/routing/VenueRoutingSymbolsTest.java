package com.balh.oms.routing;

import com.balh.oms.config.OmsConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VenueRoutingSymbolsTest {

    @Test
    void prefixRoutingDisabled_routesAllSymbolsToBothBackends() {
        OmsConfig config = new OmsConfig();
        assertThat(VenueRoutingSymbols.routesToInternalVenue(config, "AAPL")).isTrue();
        assertThat(VenueRoutingSymbols.routesToFixBroker(config, "PREDMKT-TEST-1")).isTrue();
    }

    @Test
    void prefixRoutingEnabled_splitsPredmktAndEquities() {
        OmsConfig config = new OmsConfig();
        config.getRouting().setVenueSymbolPrefixRoutingEnabled(true);
        config.getRouting().setVenueSymbolPrefix("PREDMKT");

        assertThat(VenueRoutingSymbols.routesToInternalVenue(config, "PREDMKT-TEST-1")).isTrue();
        assertThat(VenueRoutingSymbols.routesToInternalVenue(config, "AAPL")).isFalse();

        assertThat(VenueRoutingSymbols.routesToFixBroker(config, "PREDMKT-TEST-1")).isFalse();
        assertThat(VenueRoutingSymbols.routesToFixBroker(config, "AAPL")).isTrue();
    }
}
