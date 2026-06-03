package com.balh.oms.venue;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.venue.grpc.v1.ExecType;
import com.balh.venue.grpc.v1.ExecutionReport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class VenueGrpcExecutionReportMapperTest {

    private static final String VENUE = "BALH_VENUE";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void execTypeNew_mapsToVenueNew_andSynthesisesRefWhenVenueExecRefEmpty() {
        UUID orderId = UUID.fromString("11111111-1111-4111-8111-111111111111");
        ExecutionReport er =
                ExecutionReport.newBuilder()
                        .setOmsOrderId(orderId.toString())
                        .setExecType(ExecType.EXEC_TYPE_NEW)
                        .setLastQtyScaled(0L)
                        .setLastPxScaled(0L)
                        .setVenueExecRef("")
                        .build();

        ApplyExecutionReportCommand cmd =
                VenueGrpcExecutionReportMapper.toApplyCommand(er, VENUE, objectMapper);

        assertThat(cmd.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_VENUE_NEW);
        assertThat(cmd.orderId()).isEqualTo(orderId);
        assertThat(cmd.lastQtyScaled()).isZero();
        assertThat(cmd.lastPxScaled()).isZero();
        assertThat(cmd.venueId()).isEqualTo(VENUE);
        assertThat(cmd.venueExecRef())
                .as("empty venue ref on a pure-rest ack is synthesised so the cluster can dedupe")
                .isEqualTo("venue-new-" + orderId);
    }

    @Test
    void execTypeNew_keepsExplicitVenueExecRefWhenPresent() {
        UUID orderId = UUID.fromString("22222222-2222-4222-8222-222222222222");
        ExecutionReport er =
                ExecutionReport.newBuilder()
                        .setOmsOrderId(orderId.toString())
                        .setExecType(ExecType.EXEC_TYPE_NEW)
                        .setVenueExecRef("venue-ack-7")
                        .build();

        ApplyExecutionReportCommand cmd =
                VenueGrpcExecutionReportMapper.toApplyCommand(er, VENUE, objectMapper);

        assertThat(cmd.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_VENUE_NEW);
        assertThat(cmd.venueExecRef()).isEqualTo("venue-ack-7");
    }

    @Test
    void execTypeTrade_mapsToTrade_withFillQuantities() {
        UUID orderId = UUID.fromString("33333333-3333-4333-8333-333333333333");
        ExecutionReport er =
                ExecutionReport.newBuilder()
                        .setOmsOrderId(orderId.toString())
                        .setExecType(ExecType.EXEC_TYPE_TRADE)
                        .setLastQtyScaled(5_000_000_000L)
                        .setLastPxScaled(660_000L)
                        .setVenueExecRef("venue-exec-9")
                        .build();

        ApplyExecutionReportCommand cmd =
                VenueGrpcExecutionReportMapper.toApplyCommand(er, VENUE, objectMapper);

        assertThat(cmd.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_TRADE);
        assertThat(cmd.lastQtyScaled()).isEqualTo(5_000_000_000L);
        assertThat(cmd.venueExecRef()).isEqualTo("venue-exec-9");
    }
}
