package com.balh.oms.domain;

/**
 * Canonical reject taxonomy. Mirrors the {@code reject_code} Postgres enum and
 * the wire-format reject codes shipped on domain events.
 *
 * <p>Slice 1 actively uses {@link #RISK_STALE_QUEUE} and {@link #RISK_DUPLICATE};
 * the rest are reserved so that adding new checks does not require schema
 * migration churn.
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
    FIX_OUTBOUND_JOB_EXPIRED
}
