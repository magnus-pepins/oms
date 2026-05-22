package com.balh.oms.fx;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FxBboMarkupTest {

    @Test
    void wideVendorSpread_customerSpreadNeverInsideVendor() {
        BigDecimal pbBid = new BigDecimal("1.0000");
        BigDecimal pbAsk = new BigDecimal("1.1000");
        BigDecimal bps = new BigDecimal("20");

        FxBboMarkup.CustomerBbo out = FxBboMarkup.customerFromVendor(pbBid, pbAsk, bps, bps);

        assertTrue(out.bid().compareTo(pbBid) <= 0, "customer bid must not beat vendor bid");
        assertTrue(out.ask().compareTo(pbAsk) >= 0, "customer ask must not beat vendor offer");
        assertTrue(out.bid().compareTo(out.ask()) < 0, "customer bid must stay below customer ask");
        assertTrue(out.bid().compareTo(pbAsk) < 0, "customer bid must stay below vendor offer");
        assertTrue(out.ask().compareTo(pbBid) > 0, "customer ask must stay above vendor bid");
    }

    @Test
    void zeroMarkup_preservesVendorBbo() {
        BigDecimal pbBid = new BigDecimal("10.4000");
        BigDecimal pbAsk = new BigDecimal("10.4020");
        FxBboMarkup.CustomerBbo out = FxBboMarkup.customerFromVendor(
                pbBid, pbAsk, BigDecimal.ZERO, BigDecimal.ZERO);
        assertEquals(0, out.bid().compareTo(new BigDecimal("10.40000000")));
        assertEquals(0, out.ask().compareTo(new BigDecimal("10.40200000")));
    }

    @Test
    void bidMarkup_onlyWidensBid_notAsk() {
        BigDecimal pbBid = new BigDecimal("1.0850");
        BigDecimal pbAsk = new BigDecimal("1.0870");
        BigDecimal bps = new BigDecimal("50");
        FxBboMarkup.CustomerBbo out = FxBboMarkup.customerFromVendor(pbBid, pbAsk, bps, BigDecimal.ZERO);
        assertTrue(out.bid().compareTo(pbBid) < 0);
        assertEquals(0, out.ask().compareTo(new BigDecimal("1.08700000")));
    }
}
