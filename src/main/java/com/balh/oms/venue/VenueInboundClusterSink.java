package com.balh.oms.venue;
import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.venue.grpc.v1.ExecType;
import com.balh.venue.grpc.v1.ExecutionReport;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.UUID;
@Component
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(prefix = "oms.routing", name = "backend", havingValue = "internal-venue")
public class VenueInboundClusterSink {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private final OmsConfig omsConfig;
    private final OmsClusterIngressClient clusterIngressClient;
    public VenueInboundClusterSink(OmsConfig omsConfig, OmsClusterIngressClient clusterIngressClient) {
        this.omsConfig = omsConfig;
        this.clusterIngressClient = clusterIngressClient;
    }
    public void handleExecutionReport(ExecutionReport report) throws java.util.concurrent.TimeoutException, InterruptedException {
        UUID orderId = UUID.fromString(report.getOmsOrderId());
        byte exec = report.getExecType() == ExecType.EXEC_TYPE_CANCEL ? ApplyExecutionReportCommand.EXEC_TYPE_CANCEL : ApplyExecutionReportCommand.EXEC_TYPE_TRADE;
        String venueId = omsConfig.getVenue().getVenueId();
        ApplyExecutionReportCommand cmd = new ApplyExecutionReportCommand(0L, orderId, report.getLastQtyScaled(), report.getLastPxScaled(), report.getVenueTsNanos(), 0, exec, (byte)0, venueId, report.getVenueExecRef(), venueId, "{\"source\":\"balh-venue-grpc\"}");
        clusterIngressClient.submitApplyExecutionReport(cmd, TIMEOUT);
    }
}
