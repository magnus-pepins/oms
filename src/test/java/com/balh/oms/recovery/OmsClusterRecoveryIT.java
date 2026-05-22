package com.balh.oms.recovery;

import com.balh.oms.OmsClusterNodeBootstrap;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.CancelOrderCommand;
import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAcceptedEvent;
import com.balh.oms.cluster.OrderCancelAppliedEvent;
import com.balh.oms.cluster.snapshot.OmsClusterSnapshotOnShutdown;
import com.balh.oms.cluster.snapshot.OmsClusterSnapshotScheduler;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 4 recovery IT — {@code plans/oms-cluster-recovery-and-hardening.md} §6.
 *
 * <p>End-to-end proof that OMS admission state survives snapshot + restart and archive-only
 * replay. Mirrors {@code ledger-cluster} {@code LedgerClusterRecoveryIT}.
 */
class OmsClusterRecoveryIT {

    private static final Duration CLUSTER_CONNECT_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration EGRESS_WAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration SNAPSHOT_LAND_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration ROLE_CHANGE_TIMEOUT = Duration.ofSeconds(15);
    private static final long AERON_MESSAGE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10L);
    private static final int COMMAND_BUFFER_BYTES = OmsClusterWireFormat.MAX_COMMAND_BYTES;
    private static final long EGRESS_POLL_PARK_NANOS = 100_000L;

    private static final int ORDERS_TO_ADMIT = 3;

    private static final String CLUSTER_MEMBERS_SINGLE_NODE =
            "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010";

    /** Cancelled status code in {@link OmsAdmissionClusteredService} (package-private constant). */
    private static final byte STATUS_CANCELLED = 5;

    private ClusterCycle cycle;

    @AfterEach
    void tearDown() {
        if (cycle != null) {
            cycle.close();
            cycle = null;
        }
    }

    @Test
    void snapshotThenRestart_loadsStateFromSnapshot_andCancelStillWorks(@TempDir Path tempDir) {
        OmsClusterNodeBootstrap.ClusterNodePaths paths = pathsUnder(tempDir);
        ensureDirsFirstBoot(paths);

        cycle = ClusterCycle.boot(paths);
        List<UUID> orderIds = admitOrders(cycle, ORDERS_TO_ADMIT);
        awaitCluster(cycle);

        long beforeSnapshot = cycle.service.snapshotTakenCountForTest();
        assertThat(triggerSnapshotViaClusterTool(paths.clusterDir())).isTrue();
        await().atMost(SNAPSHOT_LAND_TIMEOUT)
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(cycle.service.snapshotTakenCountForTest())
                        .isEqualTo(beforeSnapshot + 1L));

        assertThat(cycle.service.admittedOrderCount()).isEqualTo(ORDERS_TO_ADMIT);
        cycle.close();

        ensureDirsRestart(paths);
        cycle = ClusterCycle.boot(paths);
        awaitReplayCompleted(cycle.service);
        assertThat(cycle.service.snapshotLoadedAtStartForTest())
                .as("restart after snapshot must load snapshot image on onStart")
                .isTrue();
        assertThat(cycle.service.admittedOrderCount()).isEqualTo(ORDERS_TO_ADMIT);
        assertThat(cycle.service.readinessCounterValueForTest())
                .isEqualTo(OmsAdmissionClusteredService.READINESS_VALUE_READY);

        cancelOrderAndAssertApplied(cycle, orderIds.getFirst());
        assertThat(cycle.service.openOrdersCountCounterValueForTest())
                .as("one cancelled order must drop open-order counter")
                .isEqualTo(ORDERS_TO_ADMIT - 1L);
    }

    @Test
    void restartWithoutSnapshot_replaysStateFromArchive_andCancelStillWorks(@TempDir Path tempDir) {
        OmsClusterNodeBootstrap.ClusterNodePaths paths = pathsUnder(tempDir);
        ensureDirsFirstBoot(paths);

        cycle = ClusterCycle.boot(paths);
        List<UUID> orderIds = admitOrders(cycle, ORDERS_TO_ADMIT);
        awaitCluster(cycle);
        long preCloseMessages = cycle.service.sessionMessageCountSinceStartForTest();
        assertThat(preCloseMessages).isGreaterThanOrEqualTo(ORDERS_TO_ADMIT);
        cycle.close();

        ensureDirsRestart(paths);
        cycle = ClusterCycle.boot(paths);
        awaitReplayCompleted(cycle.service);
        assertThat(cycle.service.snapshotLoadedAtStartForTest())
                .as("archive-only restart must not load a snapshot image")
                .isFalse();
        assertThat(cycle.service.sessionMessageCountSinceStartForTest())
                .as("archive replay must reproduce admitted commands (pop incident class when 0)")
                .isGreaterThanOrEqualTo(preCloseMessages);
        assertThat(cycle.service.admittedOrderCount()).isEqualTo(ORDERS_TO_ADMIT);
        assertThat(cycle.service.readinessCounterValueForTest())
                .isEqualTo(OmsAdmissionClusteredService.READINESS_VALUE_READY);

        cancelOrderAndAssertApplied(cycle, orderIds.get(1));
    }

    @Test
    void shutdownSnapshotViaBootstrapPolicy_whenReady_survivesRestart(@TempDir Path tempDir) {
        OmsClusterNodeBootstrap.ClusterNodePaths paths = pathsUnder(tempDir);
        ensureDirsFirstBoot(paths);

        cycle = ClusterCycle.boot(paths);
        OmsClusterSnapshotScheduler scheduler = new OmsClusterSnapshotScheduler(
                new File(paths.clusterDir()),
                OmsClusterSnapshotScheduler.DEFAULT_INTERVAL_MS,
                OmsClusterSnapshotScheduler.DEFAULT_INITIAL_DELAY_MS);
        try {
            await().atMost(ROLE_CHANGE_TIMEOUT)
                    .pollInterval(Duration.ofMillis(10))
                    .untilAsserted(() -> assertThat(cycle.service.readinessCounterValueForTest())
                            .isNotEqualTo(-1L));
            if (!cycle.service.isReadyForClusterAdmission()) {
                assertThat(OmsClusterSnapshotOnShutdown.takeIfReady(cycle.service, scheduler, false))
                        .as("must not snapshot while replay is incomplete")
                        .isFalse();
            }

            admitOrders(cycle, ORDERS_TO_ADMIT);
            awaitCluster(cycle);
            assertThat(cycle.service.isReadyForClusterAdmission()).isTrue();

            long before = cycle.service.snapshotTakenCountForTest();
            assertThat(OmsClusterSnapshotOnShutdown.takeIfReady(cycle.service, scheduler, false))
                    .isTrue();
            await().atMost(SNAPSHOT_LAND_TIMEOUT)
                    .pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() -> assertThat(cycle.service.snapshotTakenCountForTest())
                            .isGreaterThan(before));
        } finally {
            scheduler.close();
        }
        cycle.close();
        cycle = null;

        ensureDirsRestart(paths);
        cycle = ClusterCycle.boot(paths);
        awaitReplayCompleted(cycle.service);
        assertThat(cycle.service.snapshotLoadedAtStartForTest()).isTrue();
        assertThat(cycle.service.admittedOrderCount()).isEqualTo(ORDERS_TO_ADMIT);
    }

    private static final class ClusterCycle implements AutoCloseable {

        final ClusteredMediaDriver driver;
        final ClusteredServiceContainer container;
        final OmsAdmissionClusteredService service;
        final OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording;

        private ClusterCycle(
                ClusteredMediaDriver driver,
                ClusteredServiceContainer container,
                OmsAdmissionClusteredService service,
                OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording) {
            this.driver = driver;
            this.container = container;
            this.service = service;
            this.eventsRecording = eventsRecording;
        }

        static ClusterCycle boot(OmsClusterNodeBootstrap.ClusterNodePaths paths) {
            ClusteredMediaDriver driver = ClusteredMediaDriver.launch(
                    OmsClusterNodeBootstrap.buildMediaDriverContext(paths),
                    OmsClusterNodeBootstrap.buildArchiveContext(paths),
                    OmsClusterNodeBootstrap.buildConsensusModuleContext(
                            paths, 0, CLUSTER_MEMBERS_SINGLE_NODE));
            OmsAdmissionClusteredService service = new OmsAdmissionClusteredService();
            OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording =
                    OmsClusterNodeBootstrap.startEventsRecording(paths);
            ClusteredServiceContainer container = ClusteredServiceContainer.launch(
                    OmsClusterNodeBootstrap.buildServiceContainerContext(paths, service));
            return new ClusterCycle(driver, container, service, eventsRecording);
        }

        @Override
        public void close() {
            try {
                if (container != null) {
                    container.close();
                }
            } finally {
                if (eventsRecording != null) {
                    try {
                        eventsRecording.close();
                    } catch (RuntimeException ignored) {
                        // best-effort
                    }
                }
                if (driver != null) {
                    driver.close();
                }
            }
        }
    }

    private static List<UUID> admitOrders(ClusterCycle cycle, int count) {
        List<UUID> orderIds = new ArrayList<>(count);
        AtomicInteger acks = new AtomicInteger();
        try (AeronCluster client = connect(cycle.driver, acks, null, null)) {
            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
            for (int i = 0; i < count; i++) {
                UUID orderId = orderIdForIndex(i + 1);
                orderIds.add(orderId);
                offerUntilSuccess(
                        client,
                        buffer,
                        new AcceptOrderCommand(
                                        (long) (i + 1),
                                        orderId,
                                        System.nanoTime(),
                                        10_000_000_000L,
                                        0L,
                                        0,
                                        AcceptOrderCommand.SIDE_BUY,
                                        AcceptOrderCommand.TIF_DAY,
                                        "recovery-account",
                                        "recovery-idem-" + (i + 1),
                                        "recovery-hash-" + (i + 1),
                                        "AAPL",
                                        null)
                                ::encode);
            }
            await().atMost(EGRESS_WAIT_TIMEOUT)
                    .pollDelay(Duration.ZERO)
                    .pollInterval(Duration.ofMillis(20))
                    .untilAsserted(() -> {
                        client.pollEgress();
                        assertThat(acks.get()).isEqualTo(count);
                        assertThat(cycle.service.admittedOrderCount()).isEqualTo(count);
                    });
        }
        return orderIds;
    }

    private static void cancelOrderAndAssertApplied(ClusterCycle cycle, UUID orderId) {
        try (AeronCluster client = connect(cycle.driver, new AtomicInteger(), null, null)) {
            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
            offerUntilSuccess(
                    client,
                    buffer,
                    new CancelOrderCommand(99L, orderId, 1L, "recovery-probe-cancel")::encode);
            await().atMost(EGRESS_WAIT_TIMEOUT)
                    .pollDelay(Duration.ZERO)
                    .pollInterval(Duration.ofMillis(20))
                    .untilAsserted(() -> {
                        client.pollEgress();
                        assertThat(cycle.service.lookupByOrderId(orderId))
                                .as("cancel must mutate in-memory admission state")
                                .isNotNull();
                        assertThat(cycle.service.lookupByOrderId(orderId).statusCode())
                                .isEqualTo(STATUS_CANCELLED);
                    });
        }
    }

    private static AeronCluster connect(
            ClusteredMediaDriver driver,
            AtomicInteger acceptedCount,
            AtomicReference<OrderAcceptedEvent> acceptedSink,
            AtomicReference<OrderCancelAppliedEvent> cancelSink) {
        EgressListener listener = new EgressListener() {
            @Override
            public void onMessage(
                    long clusterSessionId,
                    long timestamp,
                    DirectBuffer buffer,
                    int offset,
                    int length,
                    Header header) {
                int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
                byte[] copy = new byte[length];
                buffer.getBytes(offset, copy);
                UnsafeBuffer wrapped = new UnsafeBuffer(copy);
                if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED) {
                    acceptedCount.incrementAndGet();
                    if (acceptedSink != null) {
                        acceptedSink.set(OrderAcceptedEvent.decode(wrapped, 0));
                    }
                } else if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_CANCEL_APPLIED
                        && cancelSink != null) {
                    cancelSink.set(OrderCancelAppliedEvent.decode(wrapped, 0, length));
                }
            }

            @Override
            public void onSessionEvent(
                    long correlationId,
                    long clusterSessionId,
                    long leadershipTermId,
                    int leaderMemberId,
                    EventCode code,
                    String detail) {}
        };

        AeronCluster.Context ctx = new AeronCluster.Context()
                .aeronDirectoryName(driver.mediaDriver().aeronDirectoryName())
                .ingressChannel("aeron:udp?endpoint=localhost:0")
                .ingressEndpoints("0=localhost:20110")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .egressListener(listener)
                .messageTimeoutNs(AERON_MESSAGE_TIMEOUT_NS);

        return await()
                .atMost(CLUSTER_CONNECT_TIMEOUT)
                .pollInterval(Duration.ofMillis(100))
                .ignoreExceptions()
                .until(() -> AeronCluster.connect(ctx.clone()), c -> c != null);
    }

    private static boolean triggerSnapshotViaClusterTool(String clusterDir) {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
            return ClusterTool.snapshot(new File(clusterDir), out);
        }
    }

    private static void awaitCluster(ClusterCycle cycle) {
        await().atMost(ROLE_CHANGE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    assertThat(cycle.service.replayValidationLoggedForTest()).isTrue();
                    assertThat(cycle.service.isReadyForClusterAdmission()).isTrue();
                });
    }

    private static void awaitReplayCompleted(OmsAdmissionClusteredService service) {
        await().atMost(ROLE_CHANGE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .until(service::replayValidationLoggedForTest);
    }

    private static UUID orderIdForIndex(int index) {
        return UUID.fromString(String.format("00000000-0000-4000-8000-0000000007%02d", index));
    }

    private static void offerUntilSuccess(
            AeronCluster client, ExpandableArrayBuffer buffer, CommandEncoder encoder) {
        int written = encoder.encode(buffer, 0);
        long offerResult;
        do {
            offerResult = client.offer(buffer, 0, written);
            if (offerResult < 0L) {
                LockSupport.parkNanos(EGRESS_POLL_PARK_NANOS);
            }
        } while (offerResult < 0L);
    }

    @FunctionalInterface
    private interface CommandEncoder {
        int encode(ExpandableArrayBuffer buffer, int offset);
    }

    private static OmsClusterNodeBootstrap.ClusterNodePaths pathsUnder(Path base) {
        return new OmsClusterNodeBootstrap.ClusterNodePaths(
                base.toString(),
                base.resolve("media-driver").toString(),
                base.resolve("archive").toString(),
                base.resolve("consensus-module").toString(),
                base.resolve("cluster-services").toString());
    }

    private static void ensureDirsFirstBoot(OmsClusterNodeBootstrap.ClusterNodePaths paths) {
        for (String dir :
                new String[] {paths.aeronDirBase(), paths.archiveDir(), paths.clusterDir(), paths.clusterServicesDir()}) {
            File f = new File(dir);
            if (!f.exists() && !f.mkdirs()) {
                throw new IllegalStateException("could not create test dir: " + dir);
            }
        }
        IoUtil.delete(new File(paths.aeronDirectory()), true);
    }

    private static void ensureDirsRestart(OmsClusterNodeBootstrap.ClusterNodePaths paths) {
        IoUtil.delete(new File(paths.aeronDirectory()), true);
    }
}
