package com.balh.oms.config;

/**
 * Where {@code OrderAccepted} control-plane Postgres mutations run (see {@code plans/oms-ingress-control-fix-topology.md} P1).
 *
 * <p>{@link #TAIL} — legacy: {@link com.balh.oms.tailer.ControlTailer} performs CAS, {@code control_decisions}, and
 * {@code domain_event_outbox} rows when consuming Chronicle.
 *
 * <p>{@link #INGRESS} — ingress transaction performs those writes; Chronicle tail only enqueues outbound routing when
 * the order is already {@code WORKING} at the expected version (cluster must use the same value everywhere).
 */
public enum ControlPostgresWritePath {

    /** Default: Chronicle tail applies control to Postgres. */
    TAIL,

    /**
     * Ingress applies control to Postgres in the accept transaction; tail is dispatch-only for routing/FIX handoff.
     */
    INGRESS;

    public static ControlPostgresWritePath fromProperty(String raw) {
        if (raw == null || raw.isBlank()) {
            return TAIL;
        }
        return switch (raw.trim().toLowerCase()) {
            case "tail" -> TAIL;
            case "ingress" -> INGRESS;
            default -> throw new IllegalArgumentException(
                    "oms.control.postgres-write-path must be 'tail' or 'ingress' (got: " + raw + ")");
        };
    }
}
