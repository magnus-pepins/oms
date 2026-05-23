package com.balh.oms.settlement;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * Computes {@code executions.trade_date} and {@code executions.expected_settlement_date} for
 * the {@link com.balh.oms.persistence.ExecutionsRepository#tryInsertTrade} call (gap plan
 * §5.3 Slice 1).
 *
 * <p><b>Two outputs:</b></p>
 *
 * <ul>
 *   <li>{@link #computeTradeDate(Instant)} — calendar date the execution happened. Currently
 *       projected in UTC because we do not yet carry the venue's listing-MIC timezone on
 *       executions; an XSTO fill at 02:30 Stockholm time on Tuesday would today land on
 *       Monday by this calculator. This is documented and considered acceptable for Slice 1
 *       because the broker's confirm carries its own {@code tradeDate} that the matcher will
 *       eventually treat as authoritative. The eventual rule (Slice 2 / §5.3 follow-up) maps
 *       venue MIC to {@link ZoneId} via {@code instrument_settlement_profile}.</li>
 *   <li>{@link #computeExpectedSettlementDate(LocalDate)} — applies the configured default
 *       settlement cycle (see {@link #DEFAULT_CYCLE_PROPERTY}) and rolls forward over
 *       Saturdays / Sundays only. There is intentionally no holiday calendar yet — that is
 *       captured as gap plan §5.3 follow-up. A configured cycle of {@code T+0} returns
 *       {@code tradeDate} unchanged (assuming it is itself a business day; weekends are
 *       still rolled forward).</li>
 * </ul>
 *
 * <p><b>Slice 2b-1 (V61):</b> the configured default cycle is now a <em>fallback</em>, not
 * the primary rule. {@link #resolveExpectedSettlementDate(LocalDate, String)} consults
 * {@link SettlementProfileLookup} (backed by {@link InstrumentSettlementProfileRepository}
 * at runtime, by a hand-rolled fake in unit tests) and uses the per-instrument
 * {@code settlement_cycle} when an effective row exists for the symbol at the trade date.
 * Empty / missing profile falls back to the configured default — same behaviour as Slice 1,
 * just routed through the lookup so the fallback is explicit and instrumented (DEBUG log
 * line at lookup-miss). The legacy {@link #computeExpectedSettlementDate(LocalDate)} stays
 * for unit-test callers that do not want to construct a lookup; production code paths
 * (projector) should call the symbol-aware overload.
 *
 * <p>The matcher (Slice 2a) already treats any execution row whose
 * {@code expected_settlement_date} disagrees with the broker's {@code settlementDate} as a
 * {@code settlement_date_mismatch} break — so populating {@code instrument_settlement_profile}
 * with the wrong cycle will surface as a break, not as a silent calendar drift. That is the
 * intentional safety net for skeleton-stage data.</p>
 */
@Component
public class SettlementDateCalculator {

    private static final Logger log = LoggerFactory.getLogger(SettlementDateCalculator.class);

    /** Spring property key for the placeholder default cycle. */
    public static final String DEFAULT_CYCLE_PROPERTY = "oms.settlement.default-cycle";

    /**
     * Default value when {@link #DEFAULT_CYCLE_PROPERTY} is not set. {@code T+2} matches the
     * current EU/UK convention and is the most conservative pick (it is the slowest of the
     * standard cycles), so an over-estimate on US T+1 trades shows up as a real break in the
     * matcher rather than a silent false-positive in the customer-facing settled-date UI.
     */
    public static final String DEFAULT_CYCLE_FALLBACK = "T+2";

    /** Calendar date conversion zone for {@link #computeTradeDate(Instant)} — see class doc. */
    static final ZoneId TRADE_DATE_ZONE = ZoneId.of("UTC");

    private final int defaultCycleDays;

    /**
     * Optional dependency wired by Spring via {@link #setProfileLookup(SettlementProfileLookup)}.
     * {@code null} when no {@link SettlementProfileLookup} bean is present (true in unit tests
     * that construct the calculator directly). When {@code null} the symbol-aware
     * {@link #resolveExpectedSettlementDate(LocalDate, String)} call short-circuits to the
     * configured default cycle, preserving Slice 1 behaviour for unit-test callers.
     */
    @Nullable
    private SettlementProfileLookup profileLookup;

    public SettlementDateCalculator(
            @Value("${" + DEFAULT_CYCLE_PROPERTY + ":" + DEFAULT_CYCLE_FALLBACK + "}") String defaultCycle) {
        this.defaultCycleDays = parseCycleDays(defaultCycle);
    }

    /**
     * Spring wiring entry point. Marked {@code required=false} so the Slice-1 unit tests
     * that construct the calculator with just a cycle string still compile and run. The
     * production {@link InstrumentSettlementProfileRepository} bean satisfies this in real
     * deployments and is verified by the integration tests.
     */
    @Autowired(required = false)
    public void setProfileLookup(SettlementProfileLookup profileLookup) {
        this.profileLookup = profileLookup;
    }

    /**
     * Returns the calendar date (currently UTC, see class Javadoc) for the venue-supplied
     * timestamp.
     */
    public LocalDate computeTradeDate(Instant venueTs) {
        if (venueTs == null) {
            throw new IllegalArgumentException("venueTs must not be null");
        }
        return venueTs.atZone(TRADE_DATE_ZONE).toLocalDate();
    }

    /**
     * Returns {@code tradeDate} advanced by the configured default cycle's worth of business
     * days, rolling Saturdays and Sundays forward to the next Monday. No holiday calendar.
     *
     * <p>Kept for the Slice-1 unit tests that drive the pure compute path without a profile
     * lookup. Production callers (projector) should use
     * {@link #resolveExpectedSettlementDate(LocalDate, String)} so per-instrument cycles are
     * honoured.
     */
    public LocalDate computeExpectedSettlementDate(LocalDate tradeDate) {
        if (tradeDate == null) {
            throw new IllegalArgumentException("tradeDate must not be null");
        }
        return advanceByBusinessDays(tradeDate, defaultCycleDays);
    }

    /**
     * Symbol-aware compute: looks up the active {@code instrument_settlement_profile} for
     * {@code instrumentSymbol} at {@code tradeDate} and uses its
     * {@code settlement_cycle}. Falls back to the configured default cycle when the lookup
     * is empty, the symbol is null/blank, or no {@link SettlementProfileLookup} is wired.
     *
     * <p>The fallback path is logged at DEBUG so operators can see — in a production tail —
     * which symbols still need profile rows. We deliberately do not log at INFO because the
     * skeleton table starts empty and a noisy log here would drown out real signal.
     */
    public LocalDate resolveExpectedSettlementDate(LocalDate tradeDate, String instrumentSymbol) {
        if (tradeDate == null) {
            throw new IllegalArgumentException("tradeDate must not be null");
        }
        int cycleDays = resolveCycleDaysForSymbol(tradeDate, instrumentSymbol);
        return advanceByBusinessDays(tradeDate, cycleDays);
    }

    private int resolveCycleDaysForSymbol(LocalDate tradeDate, String instrumentSymbol) {
        if (profileLookup == null || instrumentSymbol == null || instrumentSymbol.isBlank()) {
            return defaultCycleDays;
        }
        Optional<InstrumentSettlementProfile> profile;
        try {
            profile = profileLookup.findActiveBySymbol(instrumentSymbol, tradeDate);
        } catch (RuntimeException e) {
            // Defensive: a JDBC failure inside the projector's transaction would already
            // cause Spring to roll back. We log and fall back rather than re-throw so the
            // trade-projection writer reports the actual SQLException at the executions
            // insert if there's a real problem, not at an unrelated profile read.
            log.warn(
                    "instrument_settlement_profile lookup failed for symbol={} tradeDate={}; falling back to default cycle ({} days)",
                    instrumentSymbol,
                    tradeDate,
                    defaultCycleDays,
                    e);
            return defaultCycleDays;
        }
        if (profile.isEmpty()) {
            log.debug(
                    "instrument_settlement_profile miss for symbol={} tradeDate={}; using default cycle ({} days)",
                    instrumentSymbol,
                    tradeDate,
                    defaultCycleDays);
            return defaultCycleDays;
        }
        int parsed;
        try {
            parsed = parseCycleDays(profile.get().settlementCycle());
        } catch (IllegalStateException e) {
            // The V61 CHECK constraint enforces a 4-value allowlist, so this branch should
            // never fire in practice. We keep it as belt-and-braces: if a future migration
            // adds a new cycle value but forgets to widen the parser, fall back rather than
            // hard-crash the trade-projection writer.
            log.warn(
                    "instrument_settlement_profile carried unparseable cycle={} for symbol={} (profileId={}); falling back to default ({} days)",
                    profile.get().settlementCycle(),
                    instrumentSymbol,
                    profile.get().id(),
                    defaultCycleDays);
            return defaultCycleDays;
        }
        return parsed;
    }

    /**
     * Pure business-day advance. Extracted so both
     * {@link #computeExpectedSettlementDate(LocalDate)} and
     * {@link #resolveExpectedSettlementDate(LocalDate, String)} share one implementation.
     */
    private static LocalDate advanceByBusinessDays(LocalDate tradeDate, int cycleDays) {
        LocalDate d = tradeDate;
        // T+0 still rolls a weekend trade-date forward to the next business day, which is the
        // standard same-day-settle convention when the calendar lands on a weekend.
        if (cycleDays == 0) {
            return rollForwardToBusinessDay(d);
        }
        int remaining = cycleDays;
        while (remaining > 0) {
            d = d.plusDays(1);
            if (isBusinessDay(d)) {
                remaining--;
            }
        }
        return d;
    }

    /** Visible for test: the parsed numeric cycle (e.g. {@code T+1} → {@code 1}). */
    int defaultCycleDays() {
        return defaultCycleDays;
    }

    private static int parseCycleDays(String raw) {
        if (raw == null) {
            throw new IllegalStateException("default settlement cycle must not be null");
        }
        String s = raw.trim().toUpperCase(Locale.ROOT);
        if (!s.startsWith("T+") || s.length() < 3) {
            throw new IllegalStateException(
                    "invalid " + DEFAULT_CYCLE_PROPERTY + " value '" + raw + "'; expected T+0 / T+1 / T+2 / T+3");
        }
        int days;
        try {
            days = Integer.parseInt(s.substring(2));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "invalid " + DEFAULT_CYCLE_PROPERTY + " value '" + raw + "'; expected T+0 / T+1 / T+2 / T+3", e);
        }
        if (days < 0 || days > 5) {
            throw new IllegalStateException(
                    "invalid " + DEFAULT_CYCLE_PROPERTY + " value '" + raw + "'; cycle must be in [0,5]");
        }
        return days;
    }

    private static boolean isBusinessDay(LocalDate d) {
        DayOfWeek dow = d.getDayOfWeek();
        return dow != DayOfWeek.SATURDAY && dow != DayOfWeek.SUNDAY;
    }

    private static LocalDate rollForwardToBusinessDay(LocalDate d) {
        LocalDate cur = d;
        while (!isBusinessDay(cur)) {
            cur = cur.plusDays(1);
        }
        return cur;
    }
}
