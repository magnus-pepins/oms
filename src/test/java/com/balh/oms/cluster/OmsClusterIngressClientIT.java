package com.balh.oms.cluster;

import com.balh.oms.OmsClusterNodeBootstrap;
import com.balh.oms.config.OmsConfig;
import org.agrona.IoUtil;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
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

        ingressClient = new OmsClusterIngressClient(omsConfig);
        ingressClient.connect();
        assertThat(ingressClient.isConnected()).isTrue();

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
