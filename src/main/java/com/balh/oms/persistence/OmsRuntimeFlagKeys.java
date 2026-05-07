package com.balh.oms.persistence;

/**
 * Keys in {@code oms_runtime_flags.flag_key}. Interim Postgres-backed toggles;
 * Ops Console may later mirror these to Redis-backed flags.
 */
public final class OmsRuntimeFlagKeys {

    private OmsRuntimeFlagKeys() {}

    /** When {@code true}, {@link com.balh.oms.risk.ControlRiskEvaluator} rejects with {@code RISK_KILL_SWITCH}. */
    public static final String GLOBAL_HALT = "global_halt";
}
