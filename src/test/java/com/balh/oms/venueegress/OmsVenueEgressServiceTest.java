package com.balh.oms.venueegress;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.venue.VenueRouteOrderClient;
import com.balh.venue.grpc.v1.ExecType;
import com.balh.venue.grpc.v1.ExecutionReport;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OmsVenueEgressServiceTest {

    @Mock OmsVenueEgressCursorRepository cursorRepository;
    @Mock VenueRouteOrderClient routeClient;
    @Mock OmsClusterIngressClient clusterIngressClient;

    OmsVenueEgressService service;

    @BeforeEach
    void setUp() {
        service =
                new OmsVenueEgressService(
                        new OmsConfig(),
                        cursorRepository,
                        new SimpleMeterRegistry(),
                        new ObjectMapper(),
                        Clock.systemUTC(),
                        routeClient,
                        clusterIngressClient);
        service.setCurrentRecordingIdForTesting(3L);
        when(cursorRepository.advanceWithRecording(any(), anyInt(), eq(3L), anyLong())).thenReturn(true);
    }

    @Test
    void applyAdmittedEvent_skipsNonVenueSymbolWhenPrefixRoutingEnabled() throws Exception {
        OmsConfig config = new OmsConfig();
        config.getRouting().setVenueSymbolPrefixRoutingEnabled(true);
        service =
                new OmsVenueEgressService(
                        config,
                        cursorRepository,
                        new SimpleMeterRegistry(),
                        new ObjectMapper(),
                        Clock.systemUTC(),
                        routeClient,
                        clusterIngressClient);
        service.setCurrentRecordingIdForTesting(3L);

        OrderAdmittedEvent ev =
                new OrderAdmittedEvent(
                        UUID.randomUUID(),
                        1L,
                        1L,
                        10_000_000_000L,
                        650_000L,
                        0,
                        0,
                        (byte) 0,
                        (byte) 0,
                        (byte) 2,
                        "a",
                        "i",
                        "h",
                        "AAPL",
                        null,
                        null);

        assertThat(service.applyAdmittedEvent(ev, 100L)).isTrue();
        verify(routeClient, times(0)).routeAdmittedOrder(any());
        verify(clusterIngressClient, times(0)).submitApplyExecutionReport(any(), any());
    }

    @Test
    void applyAdmittedEvent_routesToVenueAndSubmitsExecutionReport() throws Exception {
        OrderAdmittedEvent ev =
                new OrderAdmittedEvent(
                        UUID.randomUUID(),
                        1L,
                        1L,
                        10_000_000_000L,
                        650_000L,
                        0,
                        0,
                        (byte) 0,
                        (byte) 0,
                        (byte) 2,
                        "a",
                        "i",
                        "h",
                        "PREDMKT-TEST-1",
                        null,
                        null);
        ExecutionReport er =
                ExecutionReport.newBuilder()
                        .setOmsOrderId(ev.orderId().toString())
                        .setVenueExecRef("venue-exec-1")
                        .setLastQtyScaled(10_000_000_000L)
                        .setLastPxScaled(650_000L)
                        .setVenueTsNanos(1L)
                        .setExecType(ExecType.EXEC_TYPE_TRADE)
                        .build();
        when(routeClient.routeAdmittedOrder(ev)).thenReturn(Optional.of(er));

        assertThat(service.applyAdmittedEvent(ev, 100L)).isTrue();

        verify(clusterIngressClient).submitApplyExecutionReport(any(), any());
    }

    @Test
    void applyAdmittedEvent_clusterSubmitRetry_doesNotRouteToVenueTwice() throws Exception {
        OrderAdmittedEvent ev =
                new OrderAdmittedEvent(
                        UUID.randomUUID(),
                        1L,
                        1L,
                        10_000_000_000L,
                        650_000L,
                        0,
                        0,
                        (byte) 0,
                        (byte) 0,
                        (byte) 2,
                        "a",
                        "i",
                        "h",
                        "PREDMKT-TEST-1",
                        null,
                        null);
        ExecutionReport er =
                ExecutionReport.newBuilder()
                        .setOmsOrderId(ev.orderId().toString())
                        .setVenueExecRef("venue-exec-1")
                        .setLastQtyScaled(10_000_000_000L)
                        .setLastPxScaled(650_000L)
                        .setVenueTsNanos(1L)
                        .setExecType(ExecType.EXEC_TYPE_TRADE)
                        .build();
        when(routeClient.routeAdmittedOrder(ev)).thenReturn(Optional.of(er));
        doThrow(new java.util.concurrent.TimeoutException("parked"))
                .doNothing()
                .when(clusterIngressClient)
                .submitApplyExecutionReport(any(ApplyExecutionReportCommand.class), any());

        assertThat(service.applyAdmittedEvent(ev, 100L)).isFalse();
        assertThat(service.applyAdmittedEvent(ev, 101L)).isTrue();

        verify(routeClient, times(1)).routeAdmittedOrder(ev);
        verify(clusterIngressClient, times(2)).submitApplyExecutionReport(any(), any());
    }
}
