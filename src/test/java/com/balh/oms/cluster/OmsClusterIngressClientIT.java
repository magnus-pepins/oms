package com.balh.oms.cluster;

import com.balh.oms.OmsClusterNodeBootstrap;
import com.balh.oms.config.OmsConfig;
import org.agrona.IoUtil;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1a integration test for the Aeron Cluster substrate plan: boots a
 * single-node cluster (same way {@link com.balh.oms.OmsClusterNodeBootstrapSmokeIT}
 * does), then drives an {@link OmsClusterIngressClient} bean against it and
 * asserts the cluster admission contract — fresh accept and idempotent re-hit.
 *
 * <p>Phase 0 already proved a bare {@code AeronCluster} client can talk to the
 * cluster; this test proves the {@code OmsClusterIngressClient} wrapper used by
 * the OMS ingress JVM also works end-to-end. Phase 1b will use this same bean
 * from {@link com.balh.oms.ingress.OrderIngressService}.
 *
 * <p>Not a {@code @SpringBootTest} — the cluster client is exercised in
 * isolation here. A Spring context boot test follows in Phase 1b once the
 * bean is wired into the ingress hot path.
 */
class OmsClusterIngressClientIT {

    private static final Duration SUBMIT_TIMEOUT = Duration.ofSeconds(10);

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private OmsClusterNodeBootstrap.EventsRecordingHandle eventsRecording;
    private OmsClusterIngressClient ingressClient;

    @AfterEach
    void tearDown() {
        try {
            if (ingressClient != null) {
                ingressClient.close();
            }
        } finally {
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
    }

    @Test
    void submitAcceptOrder_freshThenIdempotent_emitsAcceptedTwiceWithDuplicateFlagOnSecond(
            @TempDir Path tempDir) throws Exception {
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

        OmsConfig omsConfig = new OmsConfig();
        omsConfig.getCluster().getClient().setEnabled(true);
        omsConfig.getCluster().getClient().setAeronDirectory(paths.aeronDirectory());
        omsConfig.getCluster().getClient().setIngressEndpoints("0=localhost:20110");

        // Slice 4c: pass a SimpleMeterRegistry so we can assert oms.cluster.client.commit_round_trip
        // fires across this IT's two submitAcceptOrder calls plus the trailing
        // submitApplyExecutionReport. Cold-boot pre-registration guarantees all 6 (command × outcome)
        // timer instances exist before the first submit, so absent series can't quietly hide a bug.
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        ingressClient = new OmsClusterIngressClient(omsConfig, meterRegistry);
        ingressClient.connect();
        assertThat(ingressClient.isConnected()).isTrue();

        // Pre-registration assertion: all 6 timer instances exist with count=0 before any submit.
        assertSlice4cTimersPreRegistered(meterRegistry);

        UUID orderId = UUID.fromString("00000000-0000-4000-8000-000000000abc");
        AcceptOrderCommand fresh = new AcceptOrderCommand(
                ingressClient.nextCorrelationId(),
                orderId,
                System.nanoTime(),
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "ingress-account",
                "ingress-idem-1",
                "ingress-hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);

        AdmissionResult firstResult = ingressClient.submitAcceptOrder(fresh, SUBMIT_TIMEOUT);
        assertThat(firstResult).isInstanceOf(AdmissionResult.Accepted.class);
        AdmissionResult.Accepted firstAccepted = (AdmissionResult.Accepted) firstResult;
        assertThat(firstAccepted.event().correlationId()).isEqualTo(fresh.correlationId());
        assertThat(firstAccepted.event().orderId()).isEqualTo(orderId);
        assertThat(firstAccepted.event().duplicate()).isFalse();

        AcceptOrderCommand replay = new AcceptOrderCommand(
                ingressClient.nextCorrelationId(),
                /* orderId = */ UUID.fromString("00000000-0000-4000-8000-000000000def"),
                System.nanoTime(),
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "ingress-account",
                "ingress-idem-1",
                "ingress-hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);

        AdmissionResult secondResult = ingressClient.submitAcceptOrder(replay, SUBMIT_TIMEOUT);
        assertThat(secondResult).isInstanceOf(AdmissionResult.Accepted.class);
        AdmissionResult.Accepted secondAccepted = (AdmissionResult.Accepted) secondResult;
        assertThat(secondAccepted.event().correlationId()).isEqualTo(replay.correlationId());
        assertThat(secondAccepted.event().orderId())
                .as("idempotent re-hit returns the original order id, not the replay's tentative id")
                .isEqualTo(orderId);
        assertThat(secondAccepted.event().duplicate()).isTrue();

        // Slice 4c: also exercise submitApplyExecutionReport so the {command=apply_execution_report,
        // outcome=commit} timer fires. Synthetic ER targeting the order admitted above; the
        // cluster's state machine accepts it as a venue trade and emits ExecutionAppliedEvent on the
        // events stream (consumed by the projector, not by this client). The submit returns once the
        // offer is in the cluster log.
        ApplyExecutionReportCommand applyCmd = new ApplyExecutionReportCommand(
                ingressClient.nextCorrelationId(),
                orderId,
                /* lastQtyScaled = */ 5_000_000_000L,
                /* lastPxScaled = */ 100_000_000L,
                /* venueTsNanos = */ System.nanoTime(),
                /* msgSeqNum = */ 0,
                /* execTypeCode = */ ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                /* rejectCodeOrZero = */ (byte) 0,
                /* venueId = */ "TEST",
                /* venueExecRef = */ "slice4c-er-001",
                /* senderCompId = */ "",
                /* rawEnvelopeJson = */ "{}");
        ingressClient.submitApplyExecutionReport(applyCmd, SUBMIT_TIMEOUT);

        // Slice 4c: assert the commit-round-trip timer recorded all three commits.
        // submitAcceptOrder × 2 -> count=2 on {accept_order, commit};
        // submitApplyExecutionReport × 1 -> count=1 on {apply_execution_report, commit};
        // {*, timeout} and {*, error} stay at 0 — we never trip those code paths in this IT.
        assertSlice4cCommitsRecorded(meterRegistry);
    }

    private static void assertSlice4cTimersPreRegistered(SimpleMeterRegistry meterRegistry) {
        for (String command : new String[] {
                OmsClusterIngressClient.COMMAND_ACCEPT_ORDER,
                OmsClusterIngressClient.COMMAND_APPLY_EXECUTION_REPORT
        }) {
            for (OmsClusterIngressClient.Outcome outcome : OmsClusterIngressClient.Outcome.values()) {
                Tags tags = Tags.of(
                        Tag.of(OmsClusterIngressClient.TAG_COMMAND, command),
                        Tag.of(OmsClusterIngressClient.TAG_OUTCOME, outcome.lowerName()));
                assertThat(meterRegistry.find(OmsClusterIngressClient.TIMER_NAME).tags(tags).timer())
                        .as("oms.cluster.client.commit_round_trip{%s, %s} pre-registered", command, outcome.lowerName())
                        .isNotNull()
                        .satisfies(t -> assertThat(t.count())
                                .as("pre-register count must be 0 before any submit")
                                .isEqualTo(0L));
            }
        }
    }

    private static void assertSlice4cCommitsRecorded(SimpleMeterRegistry meterRegistry) {
        Tags acceptCommit = Tags.of(
                Tag.of(OmsClusterIngressClient.TAG_COMMAND, OmsClusterIngressClient.COMMAND_ACCEPT_ORDER),
                Tag.of(OmsClusterIngressClient.TAG_OUTCOME,
                        OmsClusterIngressClient.Outcome.COMMIT.lowerName()));
        Tags applyCommit = Tags.of(
                Tag.of(OmsClusterIngressClient.TAG_COMMAND, OmsClusterIngressClient.COMMAND_APPLY_EXECUTION_REPORT),
                Tag.of(OmsClusterIngressClient.TAG_OUTCOME,
                        OmsClusterIngressClient.Outcome.COMMIT.lowerName()));

        assertThat(meterRegistry.find(OmsClusterIngressClient.TIMER_NAME).tags(acceptCommit).timer())
                .as("submitAcceptOrder × 2 must record into the accept_order/commit timer")
                .isNotNull()
                .satisfies(t -> {
                    assertThat(t.count()).as("accept_order/commit count").isEqualTo(2L);
                    assertThat(t.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS))
                            .as("accept_order/commit total nanos > 0")
                            .isGreaterThan(0.0);
                });

        assertThat(meterRegistry.find(OmsClusterIngressClient.TIMER_NAME).tags(applyCommit).timer())
                .as("submitApplyExecutionReport × 1 must record into the apply_execution_report/commit timer")
                .isNotNull()
                .satisfies(t -> {
                    assertThat(t.count()).as("apply_execution_report/commit count").isEqualTo(1L);
                    assertThat(t.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS))
                            .as("apply_execution_report/commit total nanos > 0")
                            .isGreaterThan(0.0);
                });

        // No errors / timeouts in this happy-path IT: assert the other 4 series stay at 0 so a
        // future regression that mis-tags into {*, error} can't quietly hide.
        for (String command : new String[] {
                OmsClusterIngressClient.COMMAND_ACCEPT_ORDER,
                OmsClusterIngressClient.COMMAND_APPLY_EXECUTION_REPORT
        }) {
            for (OmsClusterIngressClient.Outcome outcome : new OmsClusterIngressClient.Outcome[] {
                    OmsClusterIngressClient.Outcome.TIMEOUT, OmsClusterIngressClient.Outcome.ERROR
            }) {
                Tags tags = Tags.of(
                        Tag.of(OmsClusterIngressClient.TAG_COMMAND, command),
                        Tag.of(OmsClusterIngressClient.TAG_OUTCOME, outcome.lowerName()));
                assertThat(meterRegistry.find(OmsClusterIngressClient.TIMER_NAME).tags(tags).timer())
                        .as("non-happy-path timer {%s, %s} must remain at 0", command, outcome.lowerName())
                        .isNotNull()
                        .satisfies(t -> assertThat(t.count())
                                .as("non-happy-path count must be 0")
                                .isEqualTo(0L));
            }
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
