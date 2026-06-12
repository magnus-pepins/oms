package com.balh.oms.venue;

import com.balh.venue.cluster.OrderMatchingClusteredService;
import com.balh.venue.cluster.PlaceOrderAckEvent;
import com.balh.venue.cluster.PlaceOrderCommand;
import com.balh.venue.cluster.VenueClusterNodeBootstrap;
import com.balh.venue.gateway.VenueClusterIngressClient;
import com.balh.venue.gateway.VenueGatewayConfig;
import com.balh.venue.gateway.VenueOrderGrpcService;
import com.balh.venue.matching.VenueOrderSide;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.agrona.IoUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Phase 0 test harness: single-node {@code venue-cluster-node} plus gRPC gateway on an ephemeral
 * port. Mutex with {@code @ResourceLock("oms-venue-wire-it")} — cluster ports are fixed (30110
 * series) like {@code VenueClusterNodeBootstrapSmokeIT}.
 */
public final class EmbeddedVenueStack implements AutoCloseable {

    private final Path baseDir;
    private final OrderMatchingClusteredService service = new OrderMatchingClusteredService();

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private VenueClusterNodeBootstrap.EventsRecordingHandle eventsRecording;
    private VenueClusterIngressClient clusterIngressClient;
    private Server grpcServer;
    private int grpcPort;

    private EmbeddedVenueStack(Path baseDir) {
        this.baseDir = baseDir;
    }

    public static EmbeddedVenueStack start() throws IOException {
        Path base = Files.createTempDirectory("oms-embedded-venue-");
        EmbeddedVenueStack stack = new EmbeddedVenueStack(base);
        stack.boot();
        return stack;
    }

    public String aeronDirectory() {
        return paths().aeronDirectory();
    }

    public int grpcPort() {
        return grpcPort;
    }

    public OrderMatchingClusteredService service() {
        return service;
    }

    /** Resting sell liquidity so incoming buys can cross the CLOB in ITs. */
    public void seedRestingSell(String symbol, double qty, double limitPrice)
            throws TimeoutException, InterruptedException {
        long qtyScaled = Math.round(qty * 1_000_000_000L);
        long pxScaled = Math.round(limitPrice * 1_000_000L);
        PlaceOrderCommand sell =
                new PlaceOrderCommand(
                        System.nanoTime(),
                        UUID.randomUUID(),
                        qtyScaled,
                        pxScaled,
                        VenueOrderSide.SELL,
                        symbol);
        PlaceOrderAckEvent ack = clusterIngressClient.submitPlaceOrder(sell, Duration.ofSeconds(10));
        if (ack.lastQtyScaled() != 0L) {
            throw new IllegalStateException("expected resting sell seed, got fill qty=" + ack.lastQtyScaled());
        }
    }

    public int restingOrderCount(String symbol) {
        var book = service.matchingEngineForTest().bookFor(symbol);
        return book == null ? 0 : book.snapshotRestingOrders().size();
    }

    public long tradeCount() {
        return service.tradeCountForTest();
    }

    /** Phase B: resolve a binary contract on the embedded venue cluster (operator role). */
    public void resolveContractYes(String symbol, String evidenceHash)
            throws TimeoutException, InterruptedException {
        submitResolveContract(symbol, com.balh.venue.cluster.VenueClusterWireFormat.OUTCOME_YES, evidenceHash);
    }

    /** Phase C: resolve contract NO (base symbol, e.g. {@code PREDMKT-TEST-1}). */
    public void resolveContractNo(String symbol, String evidenceHash)
            throws TimeoutException, InterruptedException {
        submitResolveContract(symbol, com.balh.venue.cluster.VenueClusterWireFormat.OUTCOME_NO, evidenceHash);
    }

    private void submitResolveContract(String symbol, byte outcome, String evidenceHash)
            throws TimeoutException, InterruptedException {
        com.balh.venue.cluster.ResolveContractCommand cmd =
                new com.balh.venue.cluster.ResolveContractCommand(
                        System.nanoTime(),
                        symbol,
                        outcome,
                        "it-oracle",
                        System.currentTimeMillis(),
                        evidenceHash,
                        com.balh.venue.matching.VenueOperatorRole.OPERATOR);
        clusterIngressClient.submitResolveContract(cmd, Duration.ofSeconds(10));
    }

    public void resetMatchingState() {
        service.matchingEngineForTest().clearForTest();
    }

    private void boot() {
        VenueClusterNodeBootstrap.ClusterNodePaths paths = paths();
        ensureDirs(paths);

        clusteredMediaDriver =
                ClusteredMediaDriver.launch(
                        VenueClusterNodeBootstrap.buildMediaDriverContext(paths),
                        VenueClusterNodeBootstrap.buildArchiveContext(paths),
                        VenueClusterNodeBootstrap.buildConsensusModuleContext(
                                paths,
                                0,
                                "0,localhost:30110,localhost:30220,localhost:30330,localhost:30440,localhost:9010"));
        eventsRecording = VenueClusterNodeBootstrap.startEventsRecording(paths);
        container =
                ClusteredServiceContainer.launch(
                        VenueClusterNodeBootstrap.buildServiceContainerContext(paths, service));

        VenueGatewayConfig gatewayConfig =
                VenueGatewayConfig.forClusterClient(
                        paths.aeronDirectory(),
                        "aeron:udp?endpoint=localhost:0",
                        "0=localhost:30110",
                        "aeron:udp?endpoint=localhost:0",
                        0,
                        1_000L);
        clusterIngressClient = VenueClusterIngressClient.connect(gatewayConfig);
        VenueOrderGrpcService grpcService =
                new VenueOrderGrpcService(clusterIngressClient, new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        try {
            grpcServer = ServerBuilder.forPort(0).addService(grpcService).build().start();
            grpcPort = grpcServer.getPort();
        } catch (IOException e) {
            throw new IllegalStateException("failed to start venue gRPC server", e);
        }
    }

    private VenueClusterNodeBootstrap.ClusterNodePaths paths() {
        return new VenueClusterNodeBootstrap.ClusterNodePaths(
                baseDir.toString(),
                baseDir.resolve("media-driver").toString(),
                baseDir.resolve("archive").toString(),
                baseDir.resolve("consensus-module").toString(),
                baseDir.resolve("cluster-services").toString());
    }

    private static void ensureDirs(VenueClusterNodeBootstrap.ClusterNodePaths paths) {
        for (String dir :
                new String[] {
                    paths.aeronDirBase(), paths.archiveDir(), paths.clusterDir(), paths.clusterServicesDir()
                }) {
            File f = new File(dir);
            if (!f.exists() && !f.mkdirs()) {
                throw new IllegalStateException("could not create test dir: " + dir);
            }
        }
        IoUtil.delete(new File(paths.aeronDirectory()), false);
    }

    @Override
    public void close() {
        try {
            if (grpcServer != null) {
                grpcServer.shutdown();
                grpcServer.awaitTermination(3, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (clusterIngressClient != null) {
                clusterIngressClient.close();
            }
            if (container != null) {
                container.close();
            }
            if (eventsRecording != null) {
                eventsRecording.close();
            }
            if (clusteredMediaDriver != null) {
                clusteredMediaDriver.close();
            }
        }
    }
}
