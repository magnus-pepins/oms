package com.balh.oms.venue;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.venue.grpc.v1.ExecType;
import com.balh.venue.grpc.v1.ExecutionReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class VenueInboundClusterSinkTest {

    @Mock OmsClusterIngressClient clusterIngressClient;

    @Test
    void tradeExecutionReport_submitsApplyCommand() throws Exception {
        OmsConfig cfg = new OmsConfig();
        cfg.getVenue().setVenueId("INTERNAL-VENUE");
        UUID id = UUID.randomUUID();
        ExecutionReport er =
                ExecutionReport.newBuilder()
                        .setOmsOrderId(id.toString())
                        .setVenueExecRef("e1")
                        .setLastQtyScaled(1_000_000_000L)
                        .setLastPxScaled(650_000L)
                        .setVenueTsNanos(42L)
                        .setExecType(ExecType.EXEC_TYPE_TRADE)
                        .build();
        new VenueInboundClusterSink(cfg, clusterIngressClient).handleExecutionReport(er);
        ArgumentCaptor<ApplyExecutionReportCommand> cap = ArgumentCaptor.forClass(ApplyExecutionReportCommand.class);
        verify(clusterIngressClient).submitApplyExecutionReport(cap.capture(), eq(Duration.ofSeconds(30)));
        assertThat(cap.getValue().orderId()).isEqualTo(id);
        assertThat(cap.getValue().venueId()).isEqualTo("INTERNAL-VENUE");
    }
}
