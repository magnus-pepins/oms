package com.balh.oms.ingress;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.balh.oms.routing.VenueRoutingSymbols;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Pre-admission per-contract tick + price-bounds gate for venue-routed (prediction-market) orders.
 *
 * <p><b>Why.</b> Phase G slice 1 made {@code balh-venue}'s {@code MatchingEngine} authoritatively reject
 * off-tick / out-of-band limit prices. Without an OMS-side mirror, an off-tick order would still be
 * <em>admitted</em> into the OMS cluster, routed to the venue, and only then rejected — leaving a
 * WORKING-in-OMS order with no venue presence (same orphan class as a risk reject). This gate rejects
 * such orders at accept with HTTP 422 so the customer gets an immediate, correct error and nothing is
 * admitted.
 *
 * <p><b>Bounds vs. payout.</b> A binary-contract limit price is a <em>probability</em> scaled by
 * {@link AcceptOrderCommand#PRICE_SCALE} (1e6 = 1.0). The cash <em>payout</em> is a separate multiplier
 * and does not bound the price, so a valid price is a multiple of the contract tick strictly inside
 * {@code (0, 1)} → {@code [tick, PRICE_SCALE − tick]}. Mirrors {@code MatchingEngine.rejectIfOffTickOrOutOfBounds}.
 *
 * <p><b>Hot path.</b> The accept path is zero-Postgres for the common (equity / FIX) flow; this gate
 * only runs for venue-routed symbols (prefix match) and reads the tick from a short-TTL JVM-local cache,
 * so a given contract is queried from Postgres at most once per {@link #CACHE_TTL}. Unknown symbols are
 * negatively cached (tick {@code 0}) so a non-catalog PREDMKT symbol does not re-query each accept; those
 * defer to the venue's authoritative reject.
 */
@Component
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class PredictionMarketTickGate {

    private static final Logger log = LoggerFactory.getLogger(PredictionMarketTickGate.class);

    /** Reject code for a price that is not a multiple of the contract tick; HTTP 422. */
    static final String REJECT_OFF_TICK = "price_off_tick";

    /** Reject code for a price outside {@code [tick, PRICE_SCALE − tick]}; HTTP 422. */
    static final String REJECT_OUT_OF_BOUNDS = "price_out_of_bounds";

    /** Tick / price scale: probability 1.0 == 1e6 (same as {@link AcceptOrderCommand#PRICE_SCALE}). */
    private static final BigDecimal PRICE_SCALE_BD = BigDecimal.valueOf(AcceptOrderCommand.PRICE_SCALE);

    /** Reference data changes rarely; a 60s TTL keeps the accept path off Postgres without staleness risk. */
    private static final Duration CACHE_TTL = Duration.ofSeconds(60);

    /** Sentinel cached for symbols with no catalog row → "not tick-enforced here, defer to venue". */
    private static final long NO_TICK = 0L;

    private static final String METRIC_BLOCKED = "oms_prediction_market_tick_gate_blocked_total";

    private final OmsConfig config;
    private final PredictionMarketContractRepository repository;
    private final MeterRegistry meterRegistry;
    private final Cache<String, Long> tickScaledBySymbol =
            Caffeine.newBuilder().maximumSize(1_024).expireAfterWrite(CACHE_TTL).build();

    public PredictionMarketTickGate(
            OmsConfig config,
            PredictionMarketContractRepository repository,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.repository = repository;
        this.meterRegistry = meterRegistry;
    }

    /**
     * @throws ClusterAdmissionException HTTP 422 {@code price_off_tick} / {@code price_out_of_bounds}
     *     when {@code instrumentSymbol} is a venue-routed contract with a known tick and {@code limitPrice}
     *     is off-tick or out of band. No-op for non-venue symbols, market orders (null price), or symbols
     *     with no catalog tick (deferred to the venue's authoritative reject).
     */
    public void assertOnTick(String instrumentSymbol, BigDecimal limitPrice) {
        if (limitPrice == null || !isVenueRouted(instrumentSymbol)) {
            return;
        }
        long tickScaled = tickScaledFor(instrumentSymbol);
        if (tickScaled <= NO_TICK) {
            return;
        }
        long priceScaled;
        try {
            priceScaled = limitPrice.movePointRight(6).longValueExact();
        } catch (ArithmeticException e) {
            // Sub-1e6 precision is unrepresentable as a scaled price; buildAcceptOrderCommand rejects it
            // with limit_price_unrepresentable. Not a tick concern — let that path own it.
            return;
        }
        if (priceScaled % tickScaled != 0L) {
            trip(instrumentSymbol, REJECT_OFF_TICK,
                    "price " + limitPrice + " is not a multiple of tick " + tickScaled + " (scaled 1e6)");
        }
        long maxOnTickPrice = AcceptOrderCommand.PRICE_SCALE - tickScaled;
        if (priceScaled < tickScaled || priceScaled > maxOnTickPrice) {
            trip(instrumentSymbol, REJECT_OUT_OF_BOUNDS,
                    "price " + limitPrice + " outside [" + tickScaled + ", " + maxOnTickPrice + "] (scaled 1e6)");
        }
    }

    private long tickScaledFor(String instrumentSymbol) {
        return tickScaledBySymbol.get(instrumentSymbol, this::loadTickScaled);
    }

    private long loadTickScaled(String instrumentSymbol) {
        Optional<BigDecimal> tickSize = repository.findTickSizeBySymbol(instrumentSymbol);
        return tickSize
                .map(t -> t.multiply(PRICE_SCALE_BD).setScale(0, java.math.RoundingMode.HALF_UP).longValueExact())
                .orElse(NO_TICK);
    }

    private boolean isVenueRouted(String instrumentSymbol) {
        return VenueRoutingSymbols.matchesVenuePrefix(
                config.getRouting().getVenueSymbolPrefix(), instrumentSymbol);
    }

    private void trip(String instrumentSymbol, String code, String detail) {
        Counter.builder(METRIC_BLOCKED)
                .description("Venue-routed accepts refused for an off-tick / out-of-bounds limit price")
                .tag("symbol", instrumentSymbol == null ? "" : instrumentSymbol)
                .tag("code", code)
                .register(meterRegistry)
                .increment();
        log.info("prediction-market tick gate rejected symbol={} code={} detail={}", instrumentSymbol, code, detail);
        throw new ClusterAdmissionException(
                HttpStatus.UNPROCESSABLE_ENTITY, code, "order rejected for " + instrumentSymbol + ": " + detail);
    }
}
