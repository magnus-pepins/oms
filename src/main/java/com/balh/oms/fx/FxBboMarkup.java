package com.balh.oms.fx;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Customer FX pricing off vendor best bid / best offer (§11.5.6).
 *
 * <p>Markups are applied <strong>per side</strong>, never off a synthetic mid:
 * <ul>
 *   <li>{@code customerBid = PB_bid × (1 − bidMarkupBps / 10_000)} — customer
 *       sells base at a rate no better than the bank's cost bid.</li>
 *   <li>{@code customerAsk = PB_offer × (1 + askMarkupBps / 10_000)} — customer
 *       buys base at a rate no better than the bank's cost offer.</li>
 * </ul>
 *
 * <p>Shared by {@link FxQuoteService} and {@link OmsFxCustomerQuotePublisher}
 * so MQTT display and HTTP quote-lock mint stay byte-identical.
 */
public final class FxBboMarkup {

    public static final BigDecimal BPS_DIVISOR = new BigDecimal("10000");
    public static final int MARKUP_FACTOR_SCALE = 10;
    public static final int RATE_SCALE = 8;

    private FxBboMarkup() {}

    public static BigDecimal applyBidMarkup(BigDecimal pbBid, BigDecimal bidMarkupBps) {
        return applySideMarkup(pbBid, bidMarkupBps, -1);
    }

    public static BigDecimal applyAskMarkup(BigDecimal pbAsk, BigDecimal askMarkupBps) {
        return applySideMarkup(pbAsk, askMarkupBps, +1);
    }

    /**
     * @param base vendor bid (direction −1) or vendor offer (direction +1)
     * @param bps non-negative markup in basis points
     * @param direction −1 widens against the customer on the bid side, +1 on the ask side
     */
    static BigDecimal applySideMarkup(BigDecimal base, BigDecimal bps, int direction) {
        BigDecimal factor = BigDecimal.ONE.add(
                bps.divide(BPS_DIVISOR, MARKUP_FACTOR_SCALE, RoundingMode.HALF_UP)
                        .multiply(BigDecimal.valueOf(direction)));
        return base.multiply(factor);
    }

    /**
     * Builds tier customer bid/ask from vendor BBO + per-side markup bps.
     */
    public static CustomerBbo customerFromVendor(
            BigDecimal pbBid, BigDecimal pbAsk, BigDecimal bidMarkupBps, BigDecimal askMarkupBps) {
        if (pbBid == null || pbAsk == null || pbBid.signum() <= 0 || pbAsk.signum() <= 0) {
            throw new IllegalArgumentException("vendor bid and offer must be positive");
        }
        BigDecimal bid = applyBidMarkup(pbBid, bidMarkupBps).setScale(RATE_SCALE, RoundingMode.HALF_UP);
        BigDecimal ask = applyAskMarkup(pbAsk, askMarkupBps).setScale(RATE_SCALE, RoundingMode.HALF_UP);
        return new CustomerBbo(bid, ask, pbBid, pbAsk);
    }

    /**
     * Cold-start only: derive a synthetic vendor BBO around a stub mid so demo
     * stacks can quote before the first live tick. Not used when live BBO exists.
     */
    public static VendorBbo vendorBboFromStubMid(BigDecimal mid, BigDecimal halfSpreadBps) {
        BigDecimal half = mid.multiply(halfSpreadBps).divide(BPS_DIVISOR, MARKUP_FACTOR_SCALE, RoundingMode.HALF_UP);
        BigDecimal bid = mid.subtract(half);
        BigDecimal ask = mid.add(half);
        if (bid.signum() <= 0) {
            bid = mid.multiply(new BigDecimal("0.9999"));
        }
        return new VendorBbo(bid, ask);
    }

    /** Customer-facing bid/ask after markup, plus the vendor legs used. */
    public record CustomerBbo(BigDecimal bid, BigDecimal ask, BigDecimal vendorBid, BigDecimal vendorOffer) {
        public BigDecimal mid() {
            return bid.add(ask).divide(BigDecimal.valueOf(2), RATE_SCALE, RoundingMode.HALF_UP);
        }
    }

    /** Raw vendor best bid / best offer for a pair. */
    public record VendorBbo(BigDecimal bid, BigDecimal offer) {
        public BigDecimal mid() {
            return bid.add(offer).divide(BigDecimal.valueOf(2), RATE_SCALE, RoundingMode.HALF_UP);
        }
    }
}
