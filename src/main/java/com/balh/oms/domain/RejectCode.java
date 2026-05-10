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
    /** Reserved for session / calendar gates (slice 5 catalogue). */
    RISK_MARKET_SESSION_CLOSED,
    /** Sanctions / PEP cache stale or screening failed when execution-time re-check is enabled (slice 8). */
    RISK_COMPLIANCE_SANCTIONS,
    /** Limit price / quote not aligned to instrument tick grid when tick check is enabled (slice 8). */
    RISK_TICK_SIZE_VIOLATION,
    /** Straight-through processing / venue session gate failed (slice 8). */
    RISK_STP_GATE
}
