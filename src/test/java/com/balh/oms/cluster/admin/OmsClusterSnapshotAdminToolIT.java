package com.balh.oms.cluster.admin;

import com.balh.oms.OmsClusterNodeBootstrap;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAcceptedEvent;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 4 slice 4a — operator-driven snapshot smoke test.
 *
 * <p>Boots a single-node Aeron Cluster in-process via {@link OmsClusterNodeBootstrap}, submits N
 * {@link AcceptOrderCommand}s, requests a snapshot via {@link ClusterTool#snapshot(File, PrintStream)}
 * (the same call the {@code clusterSnapshot} Gradle task wraps), polls the cluster's recording log
 * via {@link ClusterTool#describeLatestConsensusModuleSnapshot(PrintStream, File)} until a snapshot
 * is observed on disk, then **closes the entire cluster (service container + clustered media driver)**
 * and re-launches it against the same cluster / archive / media-driver dirs. The new
 * {@link OmsAdmissionClusteredService} instance must observe a non-null {@code snapshotImage} on
 * {@code onStart} and log {@code loaded admission snapshot: orders=N} — proving snapshot →
 * load round-trip works against an admission state with N orders.
 *
 * <p>Without this test, "we can take an Aeron Cluster snapshot of OMS admission state and recover
 * from it" is unverified — slice-3c bumped the snapshot schema to v3, but no IT exercises the full
 * write-then-load path against {@code OmsAdmissionClusteredService}'s on-disk binary format. Slice
 * 4a is where that gap closes.
 *
 * <p>Slow IT — full cluster boot + snapshot + cluster restart. Generous timeouts; uses
 * {@code @TempDir} so each run is isolated.
 */
class OmsClusterSnapshotAdminToolIT {

    /** How long to wait for the cluster to boot and the client to connect. */
    private static final java.time.Duration CLUSTER_CONNECT_TIMEOUT = java.time.Duration.ofSeconds(20);

    /** How long to wait for each egress accepted event after offering a command. */
    private static final java.time.Duration EGRESS_WAIT_TIMEOUT = java.time.Duration.ofSeconds(10);

    /** How long to wait for {@code ClusterTool.snapshot} to be acknowledged by the leader. */
    private static final java.time.Duration SNAPSHOT_REQUEST_TIMEOUT = java.time.Duration.ofSeconds(15);

    /**
     * How long to wait for the snapshot to land on disk after {@code ClusterTool.snapshot} returns
     * — the consensus module commits asynchronously after acknowledging the request.
     */
    private static final java.time.Duration SNAPSHOT_PERSISTED_TIMEOUT = java.time.Duration.ofSeconds(20);

    /** How long to wait for the restarted service container's {@code onStart} to log loadSnapshot. */
    private static final java.time.Duration LOAD_SNAPSHOT_TIMEOUT = java.time.Duration.ofSeconds(20);

    /** AeronCluster command/response timeout. Must accommodate single-node startup latency. */
    private static final long AERON_MESSAGE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /** Encoded buffer capacity for each command (header + AcceptOrder fields fit comfortably). */
    private static final int COMMAND_BUFFER_BYTES = OmsClusterWireFormat.MAX_COMMAND_BYTES;

    /** Polling cadence inside the egress / snapshot wait loops. Avoids busy-spinning the CPU. */
    private static final long POLL_PARK_NANOS = 100_000L;

    /** Number of orders to admit before snapshotting — must be >0 for the loadSnapshot assertion. */
    private static final int ORDERS_TO_ADMIT = 3;

    /** Loopback cluster-members tuple. Reused from {@link OmsClusterNodeBootstrap} default. */
    private static final String CLUSTER_MEMBERS_SINGLE_NODE =
            "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010";

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording;

    /**
     * Slice 4b: shared across both boots so the registered meters survive the cluster restart and we
     * can assert at the end that {@code outcome=write} (first boot's onTakeSnapshot) and
     * {@code outcome=load} (second boot's loadSnapshot) both fired.
     */
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @AfterEach
    void tearDown() {
        closeContainerQuietly();
        closeEventsRecordingQuietly();
        closeDriverQuietly();
    }

    @Test
    void snapshotRequest_persists_andRecoveryReloadsAdmissionState(@TempDir Path tempDir) {
        OmsClusterNodeBootstrap.ClusterNodePaths paths = pathsUnder(tempDir);
        ensureDirs(paths);

        bootCluster(paths);

        AtomicInteger acks = new AtomicInteger();
        AtomicReference<OrderAcceptedEvent> last = new AtomicReference<>();
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
                    last.set(OrderAcceptedEvent.decode(new UnsafeBuffer(copy), 0));
                    acks.incrementAndGet();
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

            for (int i = 0; i < ORDERS_TO_ADMIT; i++) {
                offerAcceptOrder(client, i + 1);
            }

            await()
                    .atMost(EGRESS_WAIT_TIMEOUT)
                    .pollDelay(java.time.Duration.ZERO)
                    .pollInterval(java.time.Duration.ofMillis(20))
                    .untilAsserted(() -> {
                        client.pollEgress();
                        assertThat(acks.get()).isEqualTo(ORDERS_TO_ADMIT);
                    });
        }

        File clusterDir = new File(paths.clusterDir());

        // Step 1: ask the leader to snapshot. ClusterTool.snapshot returns true once the cluster
        // acknowledges the toggle; the snapshot itself completes asynchronously.
        await()
                .atMost(SNAPSHOT_REQUEST_TIMEOUT)
                .pollInterval(java.time.Duration.ofMillis(200))
                .untilAsserted(() -> {
                    boolean accepted;
                    try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
                         PrintStream out = new PrintStream(buf, true, StandardCharsets.UTF_8)) {
                        accepted = ClusterTool.snapshot(clusterDir, out);
                    }
                    assertThat(accepted).isTrue();
                });

        // Step 2: wait until the snapshot is persisted to the recording log. describeLatest...
        // returns true once the recording log has a snapshot entry for the consensus module.
        await()
                .atMost(SNAPSHOT_PERSISTED_TIMEOUT)
                .pollInterval(java.time.Duration.ofMillis(250))
                .untilAsserted(() -> {
                    boolean snapshotExists;
                    try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
                         PrintStream out = new PrintStream(buf, true, StandardCharsets.UTF_8)) {
                        snapshotExists = ClusterTool.describeLatestConsensusModuleSnapshot(out, clusterDir);
                    }
                    assertThat(snapshotExists).isTrue();
                });

        // Step 3: close the entire cluster (service container + clustered media driver). The
        // cluster dir, archive dir, and media-driver dir are kept on disk so the next boot can
        // recover from the snapshot we just took.
        closeContainerQuietly();
        closeEventsRecordingQuietly();
        closeDriverQuietly();

        // Step 4: attach a logback ListAppender to capture the loadSnapshot log line BEFORE the
        // restarted service container's onStart fires. We're asserting on the production log line
        // {@code loaded admission snapshot: orders={}} from OmsAdmissionClusteredService.loadSnapshot.
        Logger admissionLogger = (Logger) LoggerFactory.getLogger(OmsAdmissionClusteredService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.setContext(admissionLogger.getLoggerContext());
        appender.start();
        admissionLogger.addAppender(appender);
        Level previousLevel = admissionLogger.getLevel();
        admissionLogger.setLevel(Level.INFO);
        try {
            // The media driver's dir was kept (dirDeleteOnShutdown=false in buildMediaDriverContext)
            // so the next boot reuses everything on disk including the recorded events stream and
            // the consensus module's recording log entry that points at the new snapshot.
            // dirDeleteOnStart=true in buildMediaDriverContext would clobber the media-driver dir
            // on the second boot, so reach in and delete it manually only for the test path.
            IoUtil.delete(new File(paths.aeronDirectory()), /* ignoreFailures = */ true);

            bootCluster(paths);

            await()
                    .atMost(LOAD_SNAPSHOT_TIMEOUT)
                    .pollInterval(java.time.Duration.ofMillis(200))
                    .untilAsserted(() ->
                            assertThat(appender.list)
                                    .as("OmsAdmissionClusteredService must log loadSnapshot on restart")
                                    .anySatisfy(event -> {
                                        assertThat(event.getFormattedMessage())
                                                .startsWith("loaded admission snapshot: orders=");
                                        assertThat(event.getFormattedMessage())
                                                .endsWith("=" + ORDERS_TO_ADMIT);
                                    }));
        } finally {
            admissionLogger.detachAppender(appender);
            admissionLogger.setLevel(previousLevel);
            appender.stop();
        }

        // Slice 4b: assert snapshot observability fired across the round-trip.
        // First boot's onTakeSnapshot -> outcome=write; second boot's loadSnapshot -> outcome=load.
        assertSnapshotMetersFired();
    }

    private void assertSnapshotMetersFired() {
        Tags writeTags = Tags.of(Tag.of("outcome", "write"));
        Tags loadTags = Tags.of(Tag.of("outcome", "load"));

        assertThat(meterRegistry.find("oms.cluster.snapshot.duration").tags(writeTags).timer())
                .as("oms.cluster.snapshot.duration{outcome=write} must be registered after onTakeSnapshot")
                .isNotNull()
                .satisfies(t -> assertThat(t.count()).as("write timer count").isGreaterThanOrEqualTo(1));
        assertThat(meterRegistry.find("oms.cluster.snapshot.duration").tags(loadTags).timer())
                .as("oms.cluster.snapshot.duration{outcome=load} must be registered after loadSnapshot")
                .isNotNull()
                .satisfies(t -> assertThat(t.count()).as("load timer count").isGreaterThanOrEqualTo(1));

        assertThat(meterRegistry.find("oms.cluster.snapshot.events").tags(writeTags).counter())
                .as("oms.cluster.snapshot.events{outcome=write} must increment on snapshot write")
                .isNotNull()
                .satisfies(c -> assertThat(c.count()).as("write counter").isGreaterThanOrEqualTo(1.0));
        assertThat(meterRegistry.find("oms.cluster.snapshot.events").tags(loadTags).counter())
                .as("oms.cluster.snapshot.events{outcome=load} must increment on snapshot load")
                .isNotNull()
                .satisfies(c -> assertThat(c.count()).as("load counter").isGreaterThanOrEqualTo(1.0));

        assertThat(meterRegistry.find("oms.cluster.snapshot.bytes").tags(writeTags).summary())
                .as("oms.cluster.snapshot.bytes{outcome=write} must record snapshot size on write")
                .isNotNull()
                .satisfies(s -> {
                    assertThat(s.count()).as("write bytes summary count").isGreaterThanOrEqualTo(1);
                    assertThat(s.totalAmount()).as("write bytes total > 0").isGreaterThan(0.0);
                });
        assertThat(meterRegistry.find("oms.cluster.snapshot.bytes").tags(loadTags).summary())
                .as("oms.cluster.snapshot.bytes{outcome=load} must record snapshot size on load")
                .isNotNull()
                .satisfies(s -> {
                    assertThat(s.count()).as("load bytes summary count").isGreaterThanOrEqualTo(1);
                    assertThat(s.totalAmount()).as("load bytes total > 0").isGreaterThan(0.0);
                });
    }

    private void bootCluster(OmsClusterNodeBootstrap.ClusterNodePaths paths) {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
                OmsClusterNodeBootstrap.buildMediaDriverContext(paths),
                OmsClusterNodeBootstrap.buildArchiveContext(paths),
                OmsClusterNodeBootstrap.buildConsensusModuleContext(
                        paths,
                        /* memberId = */ 0,
                        CLUSTER_MEMBERS_SINGLE_NODE));
        eventsRecording = OmsClusterNodeBootstrap.startEventsRecording(paths);
        // Slice 4b: pass the shared SimpleMeterRegistry into the service so both boots register
        // against the same registry and the test can assert outcome=write|load both fired.
        container = ClusteredServiceContainer.launch(OmsClusterNodeBootstrap.buildServiceContainerContext(
                paths, new OmsAdmissionClusteredService(meterRegistry)));
    }

    private void offerAcceptOrder(AeronCluster client, long correlationId) {
        UUID orderId = UUID.fromString(String.format("00000000-0000-4000-8000-0000000007%02d", correlationId));
        AcceptOrderCommand cmd = new AcceptOrderCommand(
                correlationId,
                orderId,
                /* clientTimestampNanos = */ System.nanoTime(),
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "snap-account",
                "snap-idem-" + correlationId,
                "snap-hash-" + correlationId,
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);
        long offerResult;
        do {
            offerResult = client.offer(buffer, 0, written);
            if (offerResult < 0L) {
                java.util.concurrent.locks.LockSupport.parkNanos(POLL_PARK_NANOS);
            }
        } while (offerResult < 0L);
    }

    private void closeContainerQuietly() {
        if (container != null) {
            try {
                container.close();
            } catch (RuntimeException ignored) {
                // best-effort
            }
            container = null;
        }
    }

    private void closeEventsRecordingQuietly() {
        if (eventsRecording != null) {
            try {
                eventsRecording.close();
            } catch (RuntimeException ignored) {
                // best-effort
            }
            eventsRecording = null;
        }
    }

    private void closeDriverQuietly() {
        if (clusteredMediaDriver != null) {
            try {
                clusteredMediaDriver.close();
            } catch (RuntimeException ignored) {
                // best-effort
            }
            clusteredMediaDriver = null;
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
        IoUtil.delete(new File(paths.aeronDirectory()), /* ignoreFailures = */ true);
    }
}
