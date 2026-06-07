package com.balh.oms.observability.metrics;

/** Pipeline / gRPC queue gauges and throttle counters on {@code oms-venue-egress}. */
public final class OmsVenueEgressPipelineMetrics {

    public static final String GAUGE_EFFECTIVE_DISPATCH_CAPACITY =
            "oms.venue.egress.pipeline.effective_dispatch_capacity";
    public static final String COUNTER_DISPATCH_THROTTLED =
            "oms.venue.egress.pipeline.dispatch_throttled_total";
    public static final String GAUGE_GRPC_WRITE_QUEUE_DEPTH =
            "oms.venue.egress.grpc.write_queue_depth";
    public static final String GAUGE_GRPC_OUTSTANDING_ACKS =
            "oms.venue.egress.grpc.outstanding_acks";

    private OmsVenueEgressPipelineMetrics() {}

}
