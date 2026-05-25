package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Time-windowed customer FX flow aggregation before nostro hedge (§11.5.5). */
@Service
public class FxCustomerFlowNettingService {

    private static final Logger log = LoggerFactory.getLogger(FxCustomerFlowNettingService.class);
    private static final int AMOUNT_SCALE = 8;

    private final FxCustomerFlowNettingRepository buckets;
    private final OmsConfig omsConfig;
    private final Clock clock;

    public FxCustomerFlowNettingService(
            FxCustomerFlowNettingRepository buckets, OmsConfig omsConfig, Clock clock) {
        this.buckets = buckets;
        this.omsConfig = omsConfig;
        this.clock = clock;
    }

    /**
     * Record a cross-currency BUY accept: customer funds in pair base currency
     * ({@code baseFundingAmount}) to acquire quote-currency trade notional
     * ({@code quoteTradeNotional}).
     */
    public void recordOrderAcceptFlow(String pair, BigDecimal baseFundingAmount, BigDecimal quoteTradeNotional) {
        if (!omsConfig.getFx().isCustomerFlowNettingEnabled()) {
            return;
        }
        if (pair == null || pair.length() != 6) {
            throw new IllegalArgumentException("pair_must_be_six_chars");
        }
        if (baseFundingAmount == null
                || quoteTradeNotional == null
                || baseFundingAmount.signum() <= 0
                || quoteTradeNotional.signum() <= 0) {
            throw new IllegalArgumentException("flow_amounts_must_be_positive");
        }
        String normalizedPair = pair.trim().toUpperCase(Locale.ROOT);
        String baseCcy = normalizedPair.substring(0, 3);
        String quoteCcy = normalizedPair.substring(3);
        Instant windowStart = windowStart(clock.instant());
        Instant windowEnd = windowStart.plusMillis(omsConfig.getFx().getNettingWindowMs());
        BigDecimal netBase =
                baseFundingAmount.negate().setScale(AMOUNT_SCALE, RoundingMode.HALF_UP);
        BigDecimal netQuote = quoteTradeNotional.setScale(AMOUNT_SCALE, RoundingMode.HALF_UP);
        buckets.addFlow(normalizedPair, baseCcy, quoteCcy, windowStart, windowEnd, netBase, netQuote);
        log.debug(
                "fx_netting recorded pair={} netBase={} netQuote={} windowStart={}",
                normalizedPair,
                netBase,
                netQuote,
                windowStart);
    }

    public List<Map<String, Object>> openBucketSummaries() {
        return buckets.listOpen().stream().map(this::toSummary).toList();
    }

    public int closeExpiredWindows() {
        return buckets.closeExpiredWindows(clock.instant());
    }

    Instant windowStart(Instant now) {
        long windowMs = omsConfig.getFx().getNettingWindowMs();
        long epochMs = now.toEpochMilli();
        long alignedMs = (epochMs / windowMs) * windowMs;
        return Instant.ofEpochMilli(alignedMs);
    }

    private Map<String, Object> toSummary(FxCustomerFlowNettingRepository.BucketRow row) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", row.id());
        m.put("pair", row.pair());
        m.put("baseCurrency", row.baseCurrency());
        m.put("quoteCurrency", row.quoteCurrency());
        m.put("windowStart", row.windowStart().toString());
        m.put("windowEnd", row.windowEnd().toString());
        m.put("netBaseAmount", row.netBaseAmount().toPlainString());
        m.put("netQuoteAmount", row.netQuoteAmount().toPlainString());
        m.put("flowCount", row.flowCount());
        m.put("status", row.status());
        return m;
    }
}
