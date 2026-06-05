package com.balh.oms.ingress;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.settlement.FixInCounterpartyLookupRepository;
import com.balh.oms.settlement.VenueFeeCalculator;
import com.balh.oms.settlement.VenueLiquidityRole;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Pre-trade fee quote for prediction-market orders (Phase E). */
@RestController
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
@RequestMapping("/internal/v1/prediction-market/fee-quote")
public class PredictionMarketFeeQuoteController {

    public record QuoteRequest(
            String instrumentSymbol,
            UUID accountId,
            String quantity,
            String limitPrice,
            String liquidityRole) {}

    public record QuoteResponse(
            String feeModelId,
            int feeScheduleVersion,
            String feeAmount,
            String feeCurrency,
            String liquidityRole,
            String feeSource) {}

    private final VenueFeeCalculator feeCalculator;
    private final FixInCounterpartyLookupRepository counterpartyLookup;

    public PredictionMarketFeeQuoteController(
            VenueFeeCalculator feeCalculator, FixInCounterpartyLookupRepository counterpartyLookup) {
        this.feeCalculator = feeCalculator;
        this.counterpartyLookup = counterpartyLookup;
    }

    @PostMapping
    public ResponseEntity<QuoteResponse> quote(@RequestBody QuoteRequest body) {
        if (body.instrumentSymbol() == null
                || body.accountId() == null
                || body.quantity() == null
                || body.limitPrice() == null) {
            return ResponseEntity.badRequest().build();
        }
        VenueLiquidityRole role =
                "MAKER".equalsIgnoreCase(body.liquidityRole())
                        ? VenueLiquidityRole.MAKER
                        : VenueLiquidityRole.TAKER;
        boolean retail = !counterpartyLookup.isFixAccount(body.accountId());
        long qtyScaled = scaleQty(new BigDecimal(body.quantity().trim()));
        long pxScaled = scalePrice(new BigDecimal(body.limitPrice().trim()));
        return feeCalculator
                .quoteForFill(
                        body.instrumentSymbol(),
                        body.accountId(),
                        retail,
                        role,
                        qtyScaled,
                        pxScaled,
                        null)
                .map(
                        q ->
                                ResponseEntity.ok(
                                        new QuoteResponse(
                                                q.modelId().name(),
                                                q.scheduleVersion(),
                                                q.feeAmount().toPlainString(),
                                                q.feeCurrency(),
                                                q.liquidityRole().name(),
                                                q.feeSource())))
                .orElse(ResponseEntity.notFound().build());
    }

    private static long scaleQty(BigDecimal qty) {
        return qty.multiply(new BigDecimal("1000000000")).longValue();
    }

    private static long scalePrice(BigDecimal px) {
        return px.multiply(new BigDecimal("1000000")).longValue();
    }
}
