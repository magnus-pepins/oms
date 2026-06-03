package com.balh.oms.venueresolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.venue.cluster.TradeMatchedEvent;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Phase G slice 2: oms-venue-resolver delivers the maker side of a venue trade to the OMS cluster. */
class OmsVenueResolverServiceTest {

    private OmsClusterIngressClient clusterIngressClient;
    private OmsVenueResolverService service;

    @BeforeEach
    void setUp() {
        clusterIngressClient = mock(OmsClusterIngressClient.class);
        service =
                new OmsVenueResolverService(
                        new OmsConfig(), mock(OmsVenueResolverCursorRepository.class), clusterIngressClient);
    }

    @Test
    void submitsMakerExecutionReportForRealMaker() throws Exception {
        UUID taker = UUID.randomUUID();
        UUID maker = UUID.randomUUID();
        TradeMatchedEvent event =
                new TradeMatchedEvent(
                        taker, 4_000_000_000L, 410_000L, 99L, "venue-exec-T-M", "PREDMKT-TEST-1", maker, 590_000L);

        service.submitMakerFill(event);

        ArgumentCaptor<ApplyExecutionReportCommand> captor =
                ArgumentCaptor.forClass(ApplyExecutionReportCommand.class);
        verify(clusterIngressClient).submitApplyExecutionReport(captor.capture(), any(Duration.class));
        ApplyExecutionReportCommand cmd = captor.getValue();
        assertThat(cmd.orderId()).isEqualTo(maker);
        assertThat(cmd.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_TRADE);
        assertThat(cmd.lastQtyScaled()).isEqualTo(4_000_000_000L);
        // Maker ER applies the maker's own-leg price, not the canonical print (410_000).
        assertThat(cmd.lastPxScaled()).isEqualTo(590_000L);
        assertThat(cmd.venueExecRef()).isEqualTo("venue-exec-T-M");
        assertThat(cmd.senderCompId()).isEmpty();
    }

    @Test
    void skipsLegacyEventWithNoMaker() throws Exception {
        TradeMatchedEvent legacy =
                new TradeMatchedEvent(
                        UUID.randomUUID(), 1_000L, 500_000L, 1L, "exec-legacy", "PREDMKT-TEST-1",
                        TradeMatchedEvent.NO_MAKER, 500_000L);

        service.submitMakerFill(legacy);

        verify(clusterIngressClient, never()).submitApplyExecutionReport(any(), any());
    }
}
