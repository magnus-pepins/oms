package com.balh.oms.cluster;

/**
 * How an {@link OmsClusterIngressClient} uses its Aeron cluster session on a given JVM.
 *
 * <p>{@link #ER_OFFER_ONLY} is used on {@code oms-venue-egress}: pipelined venue routes only need
 * fire-and-forget {@code ApplyExecutionReport} offers. Skipping the egress {@code pollEgress}
 * drain loop removes lock contention between ER bursts and session polling at 12k+ on-book RPS.
 */
public enum ClusterClientRole {
    /** Admit batching, egress poller, and ER-offer daemon (ingress / fix-egress). */
    FULL,
    /**
     * ER-offer daemon + session keepalive/egress-drain loop (venue-egress). No dedicated egress
     * poller thread — {@code pollEgress} runs inside ER lock passes and the keepalive loop so
     * offers do not stall on publication back-pressure.
     */
    ER_OFFER_ONLY
}
