package com.balh.oms.routing;

import com.balh.oms.config.OmsConfig;
import java.util.Locale;
import java.util.Objects;

/** Symbol-prefix split between {@code oms-fix-egress} and {@code oms-venue-egress} when both run on pop. */
public final class VenueRoutingSymbols {

    public static final String DEFAULT_VENUE_SYMBOL_PREFIX = "PREDMKT";

    private VenueRoutingSymbols() {}

    public static boolean isVenueSymbolPrefixRoutingEnabled(OmsConfig config) {
        return config != null && config.getRouting().isVenueSymbolPrefixRoutingEnabled();
    }

    public static String venueSymbolPrefix(OmsConfig config) {
        if (config == null) {
            return DEFAULT_VENUE_SYMBOL_PREFIX;
        }
        return config.getRouting().getVenueSymbolPrefix();
    }

    /** When prefix routing is off, every symbol is venue-routable (legacy single-backend tests). */
    public static boolean routesToInternalVenue(OmsConfig config, String instrumentSymbol) {
        if (!isVenueSymbolPrefixRoutingEnabled(config)) {
            return true;
        }
        return matchesVenuePrefix(venueSymbolPrefix(config), instrumentSymbol);
    }

    /** When prefix routing is off, every symbol is fix-routable. */
    public static boolean routesToFixBroker(OmsConfig config, String instrumentSymbol) {
        if (!isVenueSymbolPrefixRoutingEnabled(config)) {
            return true;
        }
        return !matchesVenuePrefix(venueSymbolPrefix(config), instrumentSymbol);
    }

    static boolean matchesVenuePrefix(String prefix, String instrumentSymbol) {
        String p = Objects.requireNonNullElse(prefix, DEFAULT_VENUE_SYMBOL_PREFIX).trim().toUpperCase(Locale.ROOT);
        if (p.isEmpty()) {
            return false;
        }
        String sym = Objects.requireNonNullElse(instrumentSymbol, "").trim().toUpperCase(Locale.ROOT);
        return sym.startsWith(p);
    }
}
