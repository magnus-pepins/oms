package com.balh.oms.settlement;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class VenueFeeCalculatorTest {

    /** 100 contracts at 0.50 → notional 50.00 (1 contract = 1e9 scaled qty). */
    private static final long QTY_100_CONTRACTS = 100L * 1_000_000_000L;
    private static final long PX_HALF = 500_000L;

    private static final VenueFeeParams BPS =
            new VenueFeeParams(
                    new BigDecimal("50"),
                    new BigDecimal("10"),
                    new BigDecimal("25"),
                    new BigDecimal("0.07"),
                    new BigDecimal("0.0175"),
                    true);

    @Test
    void takerOnly_chargesTakerNotMaker() {
        BigDecimal taker =
                VenueFeeCalculator.computeFee(
                        VenueFeeModelId.TAKER_ONLY,
                        BPS,
                        VenueLiquidityRole.TAKER,
                        QTY_100_CONTRACTS,
                        PX_HALF);
        BigDecimal maker =
                VenueFeeCalculator.computeFee(
                        VenueFeeModelId.TAKER_ONLY,
                        BPS,
                        VenueLiquidityRole.MAKER,
                        QTY_100_CONTRACTS,
                        PX_HALF);
        assertThat(taker).isGreaterThan(BigDecimal.ZERO);
        assertThat(maker).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void symmetric_chargesBothSides() {
        BigDecimal taker =
                VenueFeeCalculator.computeFee(
                        VenueFeeModelId.SYMMETRIC,
                        BPS,
                        VenueLiquidityRole.TAKER,
                        QTY_100_CONTRACTS,
                        PX_HALF);
        BigDecimal maker =
                VenueFeeCalculator.computeFee(
                        VenueFeeModelId.SYMMETRIC,
                        BPS,
                        VenueLiquidityRole.MAKER,
                        QTY_100_CONTRACTS,
                        PX_HALF);
        assertThat(taker).isEqualByComparingTo(maker);
    }

    @Test
    void kalshi_formula_roundsUpToCent() {
        VenueFeeParams kalshi =
                VenueFeeParams.fromJson(
                        "{\"taker_k\":0.07,\"maker_k\":0.0175,\"maker_fees_enabled\":true}",
                        new ObjectMapper());
        BigDecimal fee =
                VenueFeeCalculator.computeFee(
                        VenueFeeModelId.KALSHI,
                        kalshi,
                        VenueLiquidityRole.TAKER,
                        10_000_000_000L,
                        500_000L);
        assertThat(fee.scale()).isLessThanOrEqualTo(2);
        assertThat(fee).isGreaterThan(BigDecimal.ZERO);
    }
}
