package com.balh.oms.recovery;

import com.balh.oms.OmsClusterNodeBootstrap;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.cluster.OmsClusterWireFormat;
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
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 7 chaos IT — {@code plans/oms-cluster-recovery-and-hardening.md} §8.
 *
 * <p>End-to-end proof that an OMS cluster-node can be closed and reopened repeatedly with
 * in-flight admission state surviving every cycle, AND that the self-heal Aeron counter stays at
 * zero on a healthy stack. Targets the user-stated requirement: "I should be able to randomly
 * kill components and data should never be corrupted and it should come up clean again."
 *
 * <p>Companion tests:
 * <ul>
 *   <li>{@code OmsAdmissionSnapshotSelfHealTest} — proves the corrupt-snapshot self-heal path
 *       does NOT throw out of {@code onStart}, using a Mockito-mocked snapshot {@code Image}
 *       (cheap, no real cluster, runs on every {@code ./gradlew test}).</li>
 *   <li>{@code OmsClusterRecoveryIT} — Phase 4 happy-path snapshot+restart and archive-replay
 *       cycle. This IT layers chaos (multi-cycle, mid-cycle snapshots) on top of the same
 *       in-process {@link ClusteredMediaDriver} pattern.</li>
 * </ul>
 *
 * <p>Why this is an IT and not a unit test: it boots a real {@link ClusteredMediaDriver} +
 * {@link ClusteredServiceContainer}, which means real Aeron Archive recording, real consensus
 * module, real snapshot via {@link ClusterTool#snapshot}. The mocking pattern used in the unit
 * suite cannot exercise those paths.
 */
class OmsClusterChaosIT {

    private static final Duration CLUSTER_CONNECT_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration EGRESS_WAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration ROLE_CHANGE_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration SNAPSHOT_LAND_TIMEOUT = Duration.ofSeconds(15);
    private static final long AERON_MESSAGE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10L);
    private static final int COMMAND_BUFFER_BYTES = OmsClusterWireFormat.MAX_COMMAND_BYTES;
    private static final long EGRESS_POLL_PARK_NANOS = 100_000L;

    private static final String CLUSTER_MEMBERS_SINGLE_NODE =
            "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010";

    /** Five full cycles is enough to catch state leaks that only surface across multiple restarts. */
    private static final int CYCLE_COUNT = 5;

    /**
     * Approximate encoded size per {@link com.balh.oms.cluster.OmsAdmissionClusteredService.AdmittedOrder}
     * in the snapshot: 6 longs + 2 ints + 4 status bytes + UTF-8 strings (accountId, idempotencyKey,
     * accountIdHash, symbol, optional ledgerBalanceId) each with a 4-byte length prefix. Used only to
     * size {@link #ORDER_COUNT_FOR_MULTI_FRAGMENT_SNAPSHOT} with a clear margin over Aeron's MTU.
     */
    private static final int APPROX_BYTES_PER_ADMITTED_ORDER = 150;

    /**
     * Order count chosen so the resulting snapshot reliably exceeds Aeron's default fragment MTU
     * (~1408 bytes). 100 orders ≈ 15 KB ≈ 11 fragments — comfortable margin against future
     * AdmittedOrder size growth and any MTU bump.
     */
    private static final int ORDER_COUNT_FOR_MULTI_FRAGMENT_SNAPSHOT = 100;

    /** Larger snapshots need more time to land than the single-fragment case. */
    private static final Duration LARGE_SNAPSHOT_LAND_TIMEOUT = Duration.ofSeconds(30);

    /** Egress poll cadence inside the batch admit loop so the client buffers don't fill. */
    private static final int BATCH_ADMIT_EGRESS_POLL_INTERVAL = 10;

    /** Total wait budget for all batch acks to arrive on the egress listener. */
    private static final Duration BATCH_ADMIT_EGRESS_WAIT_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Stable MSB used by {@link #largeOrderIdForIndex} so generated UUIDs stay collision-free with
     * the small-index {@link #orderIdForIndex} helper (which uses MSB=0) and survive across runs.
     */
    private static final long LARGE_ORDER_ID_MSB = 0xCAFEBABEDEAD0001L;

    private ClusterCycle cycle;

    @AfterEach
    void tearDown() {
        if (cycle != null) {
            cycle.close();
            cycle = null;
        }
    }

    /**
     * {@value #CYCLE_COUNT} kill+restart cycles. Each cycle admits one order, takes a snapshot
     * mid-cycle, closes the cluster, reopens it from disk, and asserts every prior order is still
     * known. Failure mode this catches: state that's only in JVM heap (not round-tripped through
     * snapshot or replayed from log) would surface as a missing order on cycle 2+.
     */
    @Test
    void killRestartCycles_preserveAdmittedStateAcrossEachCycle(@TempDir Path tempDir) {
        OmsClusterNodeBootstrap.ClusterNodePaths paths = pathsUnder(tempDir);
        ensureDirsFirstBoot(paths);

        List<UUID> admittedAcrossCycles = new ArrayList<>();
        for (int i = 0; i < CYCLE_COUNT; i++) {
            cycle = ClusterCycle.boot(paths);
            awaitReplayCompleted(cycle.service);

            assertThat(cycle.service.snapshotLoadFailedCountForTest())
                    .as("cycle %d must NOT have self-healed past a corrupt snapshot", i)
                    .isZero();
            assertThat(cycle.service.snapshotLoadFailedCounterValueForTest())
                    .as("cycle %d published Aeron load-failed counter must be 0", i)
                    .isZero();

            if (!admittedAcrossCycles.isEmpty()) {
                assertThat(cycle.service.admittedOrderCount())
                        .as("cycle %d must recover all %d orders from prior cycles", i, admittedAcrossCycles.size())
                        .isEqualTo(admittedAcrossCycles.size());
                for (UUID prior : admittedAcrossCycles) {
                    assertThat(cycle.service.lookupByOrderId(prior))
                            .as("cycle %d must still know order %s", i, prior)
                            .isNotNull();
                }
            }

            UUID added = admitOneOrder(cycle, i + 1);
            admittedAcrossCycles.add(added);
            // After the first admit on this cycle, readiness MUST be READY — that proves
            // maybePromoteReadinessAfterAdmission fired and the cluster is admitting commands
            // for real on this boot, not just sitting in NOT_READY behind a 503 wall.
            awaitReady(cycle);

            // Take a snapshot every other cycle so we exercise BOTH the snapshot-load path AND
            // the pure-replay path across the run. Cycles with no snapshot mid-cycle will be
            // recovered purely from archive replay on the next boot.
            if (i % 2 == 0) {
                long before = cycle.service.snapshotTakenCountForTest();
                assertThat(triggerSnapshot(paths.clusterDir()))
                        .as("ClusterTool.snapshot must accept the request on cycle %d", i)
                        .isTrue();
                await().atMost(SNAPSHOT_LAND_TIMEOUT)
                        .pollInterval(Duration.ofMillis(50))
                        .untilAsserted(() -> assertThat(cycle.service.snapshotTakenCountForTest())
                                .isGreaterThan(before));
            }

            cycle.close();
            cycle = null;
            ensureDirsRestart(paths);
        }

        // Final cycle: boot one more time and assert the final state is the union of every cycle.
        cycle = ClusterCycle.boot(paths);
        awaitReplayCompleted(cycle.service);
        assertThat(cycle.service.admittedOrderCount())
                .as("final boot must recover all %d orders across all cycles", admittedAcrossCycles.size())
                .isEqualTo(admittedAcrossCycles.size());
        for (UUID prior : admittedAcrossCycles) {
            assertThat(cycle.service.lookupByOrderId(prior))
                    .as("final boot must still know order %s", prior)
                    .isNotNull();
        }
        assertThat(cycle.service.snapshotLoadFailedCountForTest()).isZero();
    }

    /**
     * Specifically targets the "snapshot + immediate close + immediate reopen" race — what the
     * 2026-05-23 pop incident did. Snapshot triggered, cluster closed mid-snapshot-write window,
     * cluster reopened. Either the snapshot landed and is loaded cleanly, OR the snapshot did
     * NOT land and we replay from log. Either way: cluster MUST come up READY with the right
     * state and the self-heal counter MUST stay zero (i.e. we don't accidentally land a poison
     * snapshot the next boot self-heals past).
     */
    @Test
    void snapshotImmediatelyFollowedByClose_doesNotLandPoisonSnapshot(@TempDir Path tempDir) {
        OmsClusterNodeBootstrap.ClusterNodePaths paths = pathsUnder(tempDir);
        ensureDirsFirstBoot(paths);

        cycle = ClusterCycle.boot(paths);
        awaitReplayCompleted(cycle.service);
        UUID first = admitOneOrder(cycle, 1);
        UUID second = admitOneOrder(cycle, 2);
        awaitReady(cycle);

        long beforeSnapshot = cycle.service.snapshotTakenCountForTest();
        assertThat(triggerSnapshot(paths.clusterDir())).isTrue();
        // Don't wait for the snapshot to land. Close immediately to maximise the window where a
        // half-written snapshot could become a poison pill.
        cycle.close();
        cycle = null;
        ensureDirsRestart(paths);

        cycle = ClusterCycle.boot(paths);
        awaitReplayCompleted(cycle.service);
        assertThat(cycle.service.snapshotLoadFailedCountForTest())
                .as("reboot after snapshot-then-close must NOT self-heal a poison snapshot")
                .isZero();
        assertThat(cycle.service.admittedOrderCount()).isEqualTo(2);
        assertThat(cycle.service.lookupByOrderId(first)).isNotNull();
        assertThat(cycle.service.lookupByOrderId(second)).isNotNull();

        // Force a successful snapshot now so this run leaves the cluster in a "snapshot exists"
        // shape future iterations of this test would also exercise on reboot.
        long beforeSecondSnapshot = cycle.service.snapshotTakenCountForTest();
        if (beforeSecondSnapshot == beforeSnapshot) {
            assertThat(triggerSnapshot(paths.clusterDir())).isTrue();
            await().atMost(SNAPSHOT_LAND_TIMEOUT)
                    .pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() -> assertThat(cycle.service.snapshotTakenCountForTest())
                            .isGreaterThan(beforeSecondSnapshot));
        }
    }

    /**
     * Phase 7 fragment-reassembly proof — what's missing from the other two tests in this class.
     *
     * <p>The 2026-05-23 pop incident root cause was that {@code OmsAdmissionClusteredService}'s
     * {@code SnapshotLoader} implemented {@code FragmentHandler} directly, not wrapped in
     * {@code ImageFragmentAssembler}. Snapshots that fit in one Aeron fragment (small clusters
     * in the existing tests, ≤2 orders) loaded fine; snapshots that spanned multiple fragments
     * (pop with ~74 balances / 195 transactions / etc.) caused {@code onFragment} to be invoked
     * once per fragment with the SECOND fragment having no magic header at offset 0 — the
     * loader then threw {@code snapshot magic mismatch: 0x{middle-of-payload}}.
     *
     * <p>This test admits enough orders that the snapshot payload comfortably exceeds Aeron's
     * default MTU (~1408 bytes), forcing the fragmentation path. With the pre-fix code this
     * test FAILS with {@code snapshotLoadFailedCount &gt; 0} on the second boot and the cluster
     * comes up empty (state recovered only via the self-heal + replay fallback, which is the
     * band-aid not the real fix). With the post-fix code (loader wrapped in
     * {@code ImageFragmentAssembler}) the cluster loads the snapshot cleanly and
     * {@code snapshotLoadFailedCount == 0}.
     */
    @Test
    void largeSnapshotSurvivesFragmentationAcrossRestart(@TempDir Path tempDir) {
        // ~150 bytes/order × ORDER_COUNT_FOR_MULTI_FRAGMENT_SNAPSHOT → snapshot easily exceeds the
        // default Aeron MTU (~1408 B), forcing fragment reassembly on load. 100 was chosen as a
        // safety margin: even if AdmittedOrder grows or the MTU rises, this stays multi-fragment.
        final int orderCount = ORDER_COUNT_FOR_MULTI_FRAGMENT_SNAPSHOT;
        OmsClusterNodeBootstrap.ClusterNodePaths paths = pathsUnder(tempDir);
        ensureDirsFirstBoot(paths);

        cycle = ClusterCycle.boot(paths);
        awaitReplayCompleted(cycle.service);

        List<UUID> admitted = admitOrderBatch(cycle, orderCount, 1);
        assertThat(cycle.service.admittedOrderCount()).isEqualTo(orderCount);
        awaitReady(cycle);

        long beforeSnap = cycle.service.snapshotTakenCountForTest();
        assertThat(triggerSnapshot(paths.clusterDir())).isTrue();
        await().atMost(LARGE_SNAPSHOT_LAND_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(cycle.service.snapshotTakenCountForTest()).isGreaterThan(beforeSnap));

        cycle.close();
        cycle = null;
        ensureDirsRestart(paths);

        cycle = ClusterCycle.boot(paths);
        awaitReplayCompleted(cycle.service);

        assertThat(cycle.service.snapshotLoadFailedCountForTest())
                .as(
                        "multi-fragment snapshot (%d orders, ~%d bytes/order, expected >1 Aeron"
                                + " fragment) must load cleanly. Pre-fix this would self-heal past"
                                + " a 'snapshot magic mismatch' on fragment 2 (2026-05-23 pop"
                                + " incident).",
                        orderCount,
                        APPROX_BYTES_PER_ADMITTED_ORDER)
                .isZero();
        assertThat(cycle.service.admittedOrderCount())
                .as("all %d orders must survive multi-fragment snapshot + restart", orderCount)
                .isEqualTo(orderCount);
        for (UUID orderId : admitted) {
            assertThat(cycle.service.lookupByOrderId(orderId))
                    .as("order %s must round-trip through multi-fragment snapshot", orderId)
                    .isNotNull();
        }
    }

    private static void awaitReplayCompleted(OmsAdmissionClusteredService service) {
        await().atMost(ROLE_CHANGE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .until(service::replayValidationLoggedForTest);
    }

    // ---- helpers (kept aligned with OmsClusterRecoveryIT for ease of comparison) ----------

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
                    }
                }
                if (driver != null) {
                    driver.close();
                }
            }
        }
    }

    private static UUID admitOneOrder(ClusterCycle cycle, int idx) {
        AtomicInteger acks = new AtomicInteger();
        UUID orderId = orderIdForIndex(idx);
        try (AeronCluster client = connect(cycle.driver, acks)) {
            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
            offerUntilSuccess(
                    client,
                    buffer,
                    new AcceptOrderCommand(
                                    (long) idx,
                                    orderId,
                                    System.nanoTime(),
                                    10_000_000_000L,
                                    0L,
                                    0,
                                    AcceptOrderCommand.SIDE_BUY,
                                    AcceptOrderCommand.TIF_DAY,
                                    "chaos-account",
                                    "chaos-idem-" + idx,
                                    "chaos-hash-" + idx,
                                    "AAPL",
                                    null)
                            ::encode);
            await().atMost(EGRESS_WAIT_TIMEOUT)
                    .pollDelay(Duration.ZERO)
                    .pollInterval(Duration.ofMillis(20))
                    .untilAsserted(() -> {
                        client.pollEgress();
                        assertThat(acks.get()).isGreaterThanOrEqualTo(1);
                    });
        }
        return orderId;
    }

    /**
     * Admits {@code count} orders over a single cluster session. Returns the order ids in submission
     * order so the caller can assert each one round-tripped. Used by
     * {@link #largeSnapshotSurvivesFragmentationAcrossRestart} to build a snapshot large enough to
     * span multiple Aeron fragments without paying {@link #connect} cost per order.
     */
    private static List<UUID> admitOrderBatch(ClusterCycle cycle, int count, int baseIdx) {
        AtomicInteger acks = new AtomicInteger();
        List<UUID> ids = new ArrayList<>(count);
        try (AeronCluster client = connect(cycle.driver, acks)) {
            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
            for (int i = 0; i < count; i++) {
                int idx = baseIdx + i;
                UUID orderId = largeOrderIdForIndex(idx);
                ids.add(orderId);
                offerUntilSuccess(
                        client,
                        buffer,
                        new AcceptOrderCommand(
                                        (long) idx,
                                        orderId,
                                        System.nanoTime(),
                                        10_000_000_000L,
                                        0L,
                                        0,
                                        AcceptOrderCommand.SIDE_BUY,
                                        AcceptOrderCommand.TIF_DAY,
                                        "chaos-account-batch",
                                        "chaos-idem-batch-" + idx,
                                        "chaos-hash-batch-" + idx,
                                        "AAPL",
                                        null)
                                ::encode);
                if (i % BATCH_ADMIT_EGRESS_POLL_INTERVAL == 0) {
                    client.pollEgress();
                }
            }
            await().atMost(BATCH_ADMIT_EGRESS_WAIT_TIMEOUT)
                    .pollDelay(Duration.ZERO)
                    .pollInterval(Duration.ofMillis(20))
                    .untilAsserted(() -> {
                        client.pollEgress();
                        assertThat(acks.get()).isGreaterThanOrEqualTo(count);
                    });
        }
        return ids;
    }

    private static AeronCluster connect(ClusteredMediaDriver driver, AtomicInteger acceptedCount) {
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
                    acceptedCount.incrementAndGet();
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

    private static boolean triggerSnapshot(String clusterDir) {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
            return ClusterTool.snapshot(new File(clusterDir), out);
        }
    }

    private static void awaitReady(ClusterCycle cycle) {
        await().atMost(ROLE_CHANGE_TIMEOUT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    assertThat(cycle.service.replayValidationLoggedForTest()).isTrue();
                    assertThat(cycle.service.isReadyForClusterAdmission()).isTrue();
                });
    }

    private static UUID orderIdForIndex(int index) {
        return UUID.fromString(String.format("00000000-0000-4000-8000-0000000099%02d", index));
    }

    /**
     * UUID generator for the large-snapshot test, supporting arbitrary positive indexes (the
     * %02d-formatted {@link #orderIdForIndex} only spans 0-99). MSB is a constant marker so the
     * two helpers never collide.
     */
    private static UUID largeOrderIdForIndex(int index) {
        return new UUID(LARGE_ORDER_ID_MSB, (long) index);
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
