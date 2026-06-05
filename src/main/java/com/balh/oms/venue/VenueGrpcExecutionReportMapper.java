package com.balh.oms.venue;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.venue.grpc.v1.ExecType;
import com.balh.venue.grpc.v1.ExecutionReport;
import com.balh.venue.grpc.v1.LiquidityRole;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.UUID;

/** Maps venue gRPC {@link ExecutionReport} to cluster {@link ApplyExecutionReportCommand}. */
public final class VenueGrpcExecutionReportMapper {

    private VenueGrpcExecutionReportMapper() {}

    public static ApplyExecutionReportCommand toApplyCommand(
            ExecutionReport er, String venueId, ObjectMapper objectMapper) {
        UUID orderId = UUID.fromString(er.getOmsOrderId());
        byte execTypeCode =
                switch (er.getExecType()) {
                    case ExecType.EXEC_TYPE_CANCEL -> ApplyExecutionReportCommand.EXEC_TYPE_CANCEL;
                    case ExecType.EXEC_TYPE_REPLACE -> ApplyExecutionReportCommand.EXEC_TYPE_REPLACE;
                    case ExecType.EXEC_TYPE_NEW -> ApplyExecutionReportCommand.EXEC_TYPE_VENUE_NEW;
                    default -> ApplyExecutionReportCommand.EXEC_TYPE_TRADE;
                };
        // The internal venue's pure-rest ack carries an empty venueExecRef; the cluster dedupes
        // execution reports on (orderId, venueExecRef), so synthesise a deterministic, non-empty ref
        // for the venue-new acknowledgement.
        String venueExecRef = er.getVenueExecRef();
        if (execTypeCode == ApplyExecutionReportCommand.EXEC_TYPE_VENUE_NEW
                && (venueExecRef == null || venueExecRef.isEmpty())) {
            venueExecRef = "venue-new-" + orderId;
        }
        return new ApplyExecutionReportCommand(
                0L,
                orderId,
                er.getLastQtyScaled(),
                er.getLastPxScaled(),
                er.getVenueTsNanos(),
                0,
                execTypeCode,
                (byte) 0,
                venueId,
                venueExecRef,
                "",
                compactRawJson(er));
    }

    /** Compact audit blob — must fit {@link com.balh.oms.cluster.OmsClusterWireFormat#MAX_STRING_BYTES}. */
    private static String compactRawJson(ExecutionReport er) {
        String liquidity =
                switch (er.getLiquidityRole()) {
                    case LIQUIDITY_ROLE_TAKER -> "TAKER";
                    case LIQUIDITY_ROLE_MAKER -> "MAKER";
                    default -> "UNSPECIFIED";
                };
        return "{\"source\":\"venue-grpc\",\"execType\":"
                + er.getExecType().getNumber()
                + ",\"liquidityRole\":\""
                + liquidity
                + "\"}";
    }
}
