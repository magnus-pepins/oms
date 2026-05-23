package com.balh.oms.fx;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * FX pair naming helpers. Vendor feeds and markups are often stored on a
 * canonical orientation (e.g. {@code AUDUSD}) while wallet flows request
 * the customer route (e.g. {@code USDAUD} = sell USD, receive AUD).
 */
public final class FxPairCatalog {

    private static final int RATE_SCALE = 8;

    private FxPairCatalog() {}

    /** Six-letter pair only: {@code EURUSD} → {@code USDEUR}. */
    public static String inversePair(String pair) {
        if (pair == null) {
            return null;
        }
        String up = pair.trim().toUpperCase();
        if (up.length() != 6) {
            return null;
        }
        return up.substring(3) + up.substring(0, 3);
    }

    /**
     * When quoting {@code requested} using vendor BBO from {@code inversePair(requested)},
     * markup rows are keyed on the inverse pair with BID/ASK swapped.
     */
    public static String markupSideForInverseQuote(String side) {
        return "ASK".equalsIgnoreCase(side) ? "BID" : "ASK";
    }

    /** Re-orient vendor BBO from {@code BASE/QUOTE} to {@code QUOTE/BASE}. */
    public static FxBboMarkup.VendorBbo invertVendorBbo(BigDecimal bid, BigDecimal offer) {
        if (bid == null || offer == null || bid.signum() <= 0 || offer.signum() <= 0) {
            throw new IllegalArgumentException("vendor bid and offer must be positive");
        }
        BigDecimal invBid = BigDecimal.ONE.divide(offer, RATE_SCALE, RoundingMode.HALF_UP);
        BigDecimal invOffer = BigDecimal.ONE.divide(bid, RATE_SCALE, RoundingMode.HALF_UP);
        return new FxBboMarkup.VendorBbo(invBid, invOffer);
    }
}
