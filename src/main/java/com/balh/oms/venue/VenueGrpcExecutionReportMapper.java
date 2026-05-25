package com.balh.oms.venue;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.venue.grpc.v1.ExecType;
import com.balh.venue.grpc.v1.ExecutionReport;
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
                    default -> ApplyExecutionReportCommand.EXEC_TYPE_TRADE;
                };
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
                er.getVenueExecRef(),
                "",
                compactRawJson(er));
    }

    /** Compact audit blob — must fit {@link com.balh.oms.cluster.OmsClusterWireFormat#MAX_STRING_BYTES}. */
    private static String compactRawJson(ExecutionReport er) {
        return "{\"source\":\"venue-grpc\",\"execType\":"
                + er.getExecType().getNumber()
                + "}";
    }
}
