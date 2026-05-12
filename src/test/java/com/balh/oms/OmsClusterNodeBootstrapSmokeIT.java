package com.balh.oms;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAcceptedEvent;
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

import java.io.File;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 0 spike (per the topology-aeron-cluster plan): boots a single-node
 * Aeron Cluster in-process via {@link OmsClusterNodeBootstrap}, connects an
 * {@link AeronCluster} client, sends one {@link AcceptOrderCommand}, and
 * verifies the cluster emits an {@link OrderAcceptedEvent}.
 *
 * <p>This is the "did the team operationalize Aeron Cluster" smoke check the
 * plan calls for, executed as JUnit so the answer is reproducible in CI rather
 * than living only in someone's local notes.
 *
 * <p>The test uses the default loopback ports baked into
 * {@link OmsClusterNodeBootstrap}. JUnit suite runs sequentially
 * ({@code junit.jupiter.execution.parallel.enabled=false}); two cluster nodes
 * on these ports at once will collide.
 */
class OmsClusterNodeBootstrapSmokeIT {

    /** How long to wait for the cluster to boot and the client to connect. */
    private static final java.time.Duration CLUSTER_CONNECT_TIMEOUT = java.time.Duration.ofSeconds(20);

    /** How long to wait for the egress event after offering the command. */
    private static final java.time.Duration EGRESS_WAIT_TIMEOUT = java.time.Duration.ofSeconds(10);

    /** AeronCluster command/response timeout. Must accommodate single-node startup latency. */
    private static final long AERON_MESSAGE_TIMEOUT_NS = java.util.concurrent.TimeUnit.SECONDS.toNanos(10);

    /** Encoded buffer capacity for the test command (header + AcceptOrder fields fit comfortably). */
    private static final int COMMAND_BUFFER_BYTES = OmsClusterWireFormat.MAX_COMMAND_BYTES;

    /** Polling cadence inside the egress wait loop. Avoids busy-spinning the CPU. */
    private static final long EGRESS_POLL_PARK_NANOS = 100_000L;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording;

    @AfterEach
    void tearDown() {
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
            if (clusteredMediaDriver != null) {
                clusteredMediaDriver.close();
            }
        }
    }

    @Test
    void singleNodeCluster_acceptsCommand_emitsAcceptedEvent(@TempDir Path tempDir) {
        OmsClusterNodeBootstrap.ClusterNodePaths paths = pathsUnder(tempDir);
        ensureDirs(paths);

        clusteredMediaDriver = ClusteredMediaDriver.launch(
                OmsClusterNodeBootstrap.buildMediaDriverContext(paths),
                OmsClusterNodeBootstrap.buildArchiveContext(paths),
                OmsClusterNodeBootstrap.buildConsensusModuleContext(
                        paths,
                        /* memberId = */ 0,
                        "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010"));
        eventsRecording = OmsClusterNodeBootstrap.startEventsRecording(paths);
        container = ClusteredServiceContainer.launch(OmsClusterNodeBootstrap.buildServiceContainerContext(paths));

        AtomicReference<OrderAcceptedEvent> received = new AtomicReference<>();
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
                if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED) {
                    byte[] copy = new byte[length];
                    buffer.getBytes(offset, copy);
                    received.set(OrderAcceptedEvent.decode(new UnsafeBuffer(copy), 0));
                }
            }

            @Override
            public void onSessionEvent(
                    long correlationId,
                    long clusterSessionId,
                    long leadershipTermId,
                    int leaderMemberId,
                    EventCode code,
                    String detail) {
                // Logged elsewhere by Aeron; no-op here.
            }
        };

        AeronCluster.Context clientContext = new AeronCluster.Context()
                .aeronDirectoryName(paths.aeronDirectory())
                .ingressChannel("aeron:udp?endpoint=localhost:0")
                .ingressEndpoints("0=localhost:20110")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .egressListener(listener)
                .messageTimeoutNs(AERON_MESSAGE_TIMEOUT_NS);

        try (AeronCluster client = await()
                .atMost(CLUSTER_CONNECT_TIMEOUT)
                .pollInterval(java.time.Duration.ofMillis(100))
                .ignoreExceptions()
                .until(() -> AeronCluster.connect(clientContext.clone()), c -> c != null)) {

            UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000777");
            AcceptOrderCommand cmd = new AcceptOrderCommand(
                    /* correlationId = */ 1L,
                    orderId,
                    /* clientTimestampNanos = */ System.nanoTime(),
                    /* quantityScaled = */ 10_000_000_000L,
                    /* limitPriceScaledOrZero = */ 0L,
                    /* shardId = */ 0,
                    AcceptOrderCommand.SIDE_BUY,
                    AcceptOrderCommand.TIF_DAY,
                    "smoke-account",
                    "smoke-idem",
                    "smoke-hash",
                    "AAPL",
                    /* ledgerBalanceIdOrNull = */ null);

            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
            int written = cmd.encode(buffer, 0);

            long offerResult;
            do {
                offerResult = client.offer(buffer, 0, written);
                if (offerResult < 0L) {
                    java.util.concurrent.locks.LockSupport.parkNanos(EGRESS_POLL_PARK_NANOS);
                }
            } while (offerResult < 0L);

            await()
                    .atMost(EGRESS_WAIT_TIMEOUT)
                    .pollDelay(java.time.Duration.ZERO)
                    .pollInterval(java.time.Duration.ofMillis(20))
                    .untilAsserted(() -> {
                        client.pollEgress();
                        assertThat(received.get()).isNotNull();
                    });

            OrderAcceptedEvent ev = received.get();
            assertThat(ev.correlationId()).isEqualTo(1L);
            assertThat(ev.orderId()).isEqualTo(orderId);
            assertThat(ev.duplicate()).isFalse();
        }
    }

    private static OmsClusterNodeBootstrap.ClusterNodePaths pathsUnder(Path base) {
        return new OmsClusterNodeBootstrap.ClusterNodePaths(
                base.toString(),
                base.resolve("media-driver").toString(),
                base.resolve("archive").toString(),
                base.resolve("consensus-module").toString(),
                base.resolve("cluster-services").toString());
    }

    private static void ensureDirs(OmsClusterNodeBootstrap.ClusterNodePaths paths) {
        for (String dir : new String[] {
                paths.aeronDirBase(), paths.archiveDir(), paths.clusterDir(), paths.clusterServicesDir()
        }) {
            File f = new File(dir);
            if (!f.exists() && !f.mkdirs()) {
                throw new IllegalStateException("could not create test dir: " + dir);
            }
        }
        // Aeron media driver creates its own directory and complains if it pre-exists when dirDeleteOnStart=true.
        IoUtil.delete(new File(paths.aeronDirectory()), /* ignoreFailures = */ true);
    }
}
