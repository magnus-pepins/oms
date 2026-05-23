package com.balh.oms.settlement;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Locale;

import org.springframework.beans.factory.annotation.Value;
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
 * <p><b>Why a placeholder default cycle is honest, not a band-aid.</b> The right rule is
 * "look up the instrument's settlement profile and use its cycle, falling back to the venue
 * default". We do not yet carry that profile, and inventing a venue-MIC heuristic
 * (XNAS → T+1, XSTO → T+2) inside this class would lock in a wrong assumption the day a
 * non-equity instrument trades. Instead we expose a single configured default that an
 * operator can flip per environment (e.g. {@code T+2} now → {@code T+1} on 2027-10-11 when
 * EU migrates), and ship an honest {@code TODO} naming the eventual lookup site. The matcher
 * (next slice) will treat any execution row whose {@code expected_settlement_date} disagrees
 * with the broker's {@code settlementDate} as a {@code settlement_date_mismatch} break — but
 * it will look up the actual rule through the profile by then, not via this class.</p>
 */
@Component
public class SettlementDateCalculator {

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

    public SettlementDateCalculator(
            @Value("${" + DEFAULT_CYCLE_PROPERTY + ":" + DEFAULT_CYCLE_FALLBACK + "}") String defaultCycle) {
        this.defaultCycleDays = parseCycleDays(defaultCycle);
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
     * Returns {@code tradeDate} advanced by the configured cycle's worth of business days,
     * rolling Saturdays and Sundays forward to the next Monday. No holiday calendar.
     */
    public LocalDate computeExpectedSettlementDate(LocalDate tradeDate) {
        if (tradeDate == null) {
            throw new IllegalArgumentException("tradeDate must not be null");
        }
        LocalDate d = tradeDate;
        int remaining = defaultCycleDays;
        // T+0 still rolls a weekend trade-date forward to the next business day, which is the
        // standard same-day-settle convention when the calendar lands on a weekend.
        if (remaining == 0) {
            return rollForwardToBusinessDay(d);
        }
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
