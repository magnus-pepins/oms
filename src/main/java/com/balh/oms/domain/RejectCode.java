package com.balh.oms.domain;

/**
 * Canonical reject taxonomy. Mirrors the {@code reject_code} Postgres enum and
 * the wire-format reject codes shipped on domain events.
 *
 * <p><strong>Actively emitted on the hot path today:</strong> {@link #RISK_STALE_QUEUE} (stale control jobs),
 * plus the risk / venue / internal codes listed in {@code oms/docs/risk-checks.md}.
 *
 * <p>{@link #RISK_DUPLICATE} exists in the Postgres enum for forward compatibility with plan §5.11, but
 * <strong>ingress duplicate idempotency</strong> ({@code UNIQUE (account_id, client_idempotency_key)}) returns
 * HTTP 200 with the existing order and does <strong>not</strong> emit this code or enqueue control — see
 * {@code OrdersControllerIntegrationTest#duplicateIdempotencyKeyReturnsExistingOrder}.
 */
public enum RejectCode {
    RISK_STALE_QUEUE,
    RISK_DUPLICATE,
    RISK_KILL_SWITCH,
    RISK_BUYING_POWER,
    RISK_INVALID_INSTRUMENT,
    /** Order symbol not in configured tradable universe (v1 list; future: instruments cache / venue). */
    RISK_INSTRUMENT_NOT_ALLOWED,
    RISK_FAT_FINGER_PRICE,
    RISK_FAT_FINGER_SIZE,
    RISK_RATE_LIMIT,
    RISK_NOTIONAL_CAP,
    INTERNAL_ERROR,
    /** FIX venue/broker new-order or similar reject ({@code ExecType=Rejected} / cancel reject). */
    VENUE_REJECT,
    /** Outbound FIX job exceeded {@code oms.fix.max-outbound-job-age-ms} at dequeue (slice 4). */
    FIX_OUTBOUND_JOB_EXPIRED,
    /** Symbol is on an operator-configured halt list (slice 5). */
    RISK_SYMBOL_HALT,
    /** Reserved for concentration / position limit checks (slice 5 catalogue). */
    RISK_CONCENTRATION_LIMIT,
    /** SELL quantity exceeds {@code positions.quantity_total} for account+symbol+custody. */
    RISK_INSUFFICIENT_POSITION,
    /**
     * ISK tax-wrapper account trading an instrument whose active settlement profile has
     * {@code isk_eligible=false} (gap plan §5.10 / Phase E Slice 12a).
     */
    RISK_ISK_INSTRUMENT_NOT_ELIGIBLE,
    /** Reserved for session / calendar gates (slice 5 catalogue). */
    RISK_MARKET_SESSION_CLOSED,
    /** Sanctions / PEP cache stale or screening failed when execution-time re-check is enabled (slice 8). */
    RISK_COMPLIANCE_SANCTIONS,
    /** Limit price / quote not aligned to instrument tick grid when tick check is enabled (slice 8). */
    RISK_TICK_SIZE_VIOLATION,
    /** Straight-through processing / venue session gate failed (slice 8). */
    RISK_STP_GATE,
    /**
     * Trade currency differs from source-balance currency and the source balance
     * has {@code auto_fx_enabled=false} (or no eligible balance was passed by the
     * BFF picker). See plans/oms-multi-currency-invest-accounts.md §8.6.
     */
    RISK_FX_REQUIRED,
    /**
     * Order arrived with a {@code quoteId} that {@code FxQuoteService.recall}
     * returns null/expired. The BFF should normally surface this earlier as a
     * "refresh and reconfirm" prompt; OMS emits this only for genuinely stale
     * submits.
     */
    RISK_FX_QUOTE_EXPIRED,
    /**
     * Vendor mid feed older than the configured staleness window at quote or
     * accept time. Loud signal that {@code OmsFxMidSubscriber} stopped seeing
     * ticks; the operator should investigate before approving cross-currency
     * trades.
     */
    RISK_FX_STALE_QUOTE,
    /**
     * Operator has an active row in {@code fx_pair_tier_kills} covering
     * the requested (pair, tier). The streaming customer-quote publisher
     * stops emitting that combination, and the HTTP {@code /fx/quote}
     * path refuses to mint so the BFF cannot route around the kill.
     * BFF should surface "this rate is not available right now" rather
     * than auto-retry — kills are tactical and short-lived. Plan A2 in
     * {@code system-documentation/plans/fx-treasury-auto-hedger-and-publisher-controls.md}.
     */
    RISK_FX_TIER_KILLED
}
