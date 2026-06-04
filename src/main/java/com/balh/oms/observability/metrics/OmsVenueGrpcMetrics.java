package com.balh.oms.observability.metrics;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Client-side venue gRPC failure counters on {@code oms-venue-egress}. */
public final class OmsVenueGrpcMetrics {

    public static final String EGRESS_FAILURES = "oms.venue.egress.grpc_failures_total";

    public static final String RPC_ROUTE_ORDER = "route_order";
    public static final String RPC_ROUTE_CANCEL = "route_cancel";
    public static final String RPC_ROUTE_REPLACE = "route_replace";
    public static final String RPC_QUERY_ORDER_STATUS = "query_order_status";

    private OmsVenueGrpcMetrics() {}

    public static void recordEgressFailure(MeterRegistry registry, String rpc, StatusRuntimeException e) {
        if (registry == null) {
            return;
        }
        Status status = e == null ? null : e.getStatus();
        String code =
                status == null || status.getCode() == null
                        ? "unknown"
                        : status.getCode().name().toLowerCase();
        Counter.builder(EGRESS_FAILURES)
                .description("Venue gRPC client route failures on oms-venue-egress")
                .tag("rpc", rpc)
                .tag("status", code)
                .register(registry)
                .increment();
    }
}
