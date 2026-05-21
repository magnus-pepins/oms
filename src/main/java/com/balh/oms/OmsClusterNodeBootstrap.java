package com.balh.oms;

import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.admin.OmsClusterNodeMetricsExporter;
import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

/**
 * Cluster-node JVM entrypoint per ADR 0001.
 *
 * <p>Boots an Aeron {@link ClusteredMediaDriver} (MediaDriver + Archive +
 * ConsensusModule) together with a {@link ClusteredServiceContainer} hosting
 * {@link OmsAdmissionClusteredService}. Source of truth for OMS admission
 * state; Postgres becomes a downstream projection.
 *
 * <p>This is intentionally <em>not</em> a Spring Boot application. Cluster
 * nodes do not need HTTP, gRPC, JDBC, or Flyway; everything that does live in
 * the Spring Boot OMS today (cluster client, Postgres projector, FIX egress)
 * runs in different JVMs with their own profiles.
 *
 * <h3>Local single-node default</h3>
 *
 * <p>Without env-var overrides this process runs as a one-member cluster with
 * Aeron working dirs under {@code build/aeron-cluster/}. That is enough for
 * local dev, integration smoke tests, and the Phase 0 spike. Multi-member
 * deployments override the cluster-members string and the Aeron / archive /
 * cluster directories per node (typically via k8s StatefulSet env vars).
 *
 * <h3>Configuration</h3>
 *
 * <p>All overridable values come from environment variables; named constants
 * give the dev defaults. There is no magic-number literal in this file
 * (per {@code .cursor/rules/config-and-limits.mdc}).
 */
public final class OmsClusterNodeBootstrap {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterNodeBootstrap.class);

    /**
     * Default cluster member-id when running single-node locally. Overridden
     * via {@code OMS_AERON_CLUSTER_MEMBER_ID} per node in multi-member deployments.
     */
    private static final int DEFAULT_MEMBER_ID = 0;

    /**
     * Single-member cluster string for local dev: one member at member-id 0,
     * loopback endpoints with the default Aeron Cluster ports. The format is
     * Aeron's standard "id,ingress,consensus,log,catchup,archive" tuple
     * separated by '|' between members.
     *
     * <p>Production overrides this via {@code OMS_AERON_CLUSTER_MEMBERS}.
     */
    private static final String DEFAULT_CLUSTER_MEMBERS_SINGLE_NODE =
            "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010";

    /** Default Aeron working directory base (under Gradle build dir for clean ergonomics). */
    private static final String DEFAULT_AERON_DIR_BASE = "build/aeron-cluster";

    /** Default archive subdirectory under {@link #DEFAULT_AERON_DIR_BASE}. */
    private static final String DEFAULT_ARCHIVE_DIR_NAME = "archive";

    /** Default consensus-module / cluster state subdirectory under {@link #DEFAULT_AERON_DIR_BASE}. */
    private static final String DEFAULT_CLUSTER_DIR_NAME = "consensus-module";

    /** Default service-container state subdirectory under {@link #DEFAULT_AERON_DIR_BASE}. */
    private static final String DEFAULT_CLUSTER_SERVICES_DIR_NAME = "cluster-services";

    /** Default media-driver subdirectory under {@link #DEFAULT_AERON_DIR_BASE}. */
    private static final String DEFAULT_AERON_MEDIA_DRIVER_DIR_NAME = "media-driver";

    /**
     * Default {@code maxConcurrentSessions} for the cluster node. Aeron's stock default is 10
     * (see {@code ConsensusModule.Configuration.MAX_CONCURRENT_SESSIONS_DEFAULT}), which is too
     * tight for OMS where every ingress replica plus every operator tool holds a long-lived
     * session: one cluster will easily see hundreds. Single tunable per node since cluster nodes
     * don't share Spring config; override via {@link #ENV_MAX_CONCURRENT_SESSIONS} in k8s.
     */
    private static final int DEFAULT_MAX_CONCURRENT_SESSIONS = 1024;

    /** Env var: override {@link #DEFAULT_MAX_CONCURRENT_SESSIONS}. */
    private static final String ENV_MAX_CONCURRENT_SESSIONS = "OMS_AERON_CLUSTER_MAX_SESSIONS";

    /** Env var: comma-separated cluster member tuple (Aeron's {@code clusterMembers} format). */
    private static final String ENV_CLUSTER_MEMBERS = "OMS_AERON_CLUSTER_MEMBERS";

    /** Env var: this node's member id. Must match the corresponding entry in {@link #ENV_CLUSTER_MEMBERS}. */
    private static final String ENV_MEMBER_ID = "OMS_AERON_CLUSTER_MEMBER_ID";

    /** Env var: base directory for all Aeron working state on this node. */
    private static final String ENV_AERON_DIR_BASE = "OMS_AERON_DIR_BASE";

    /**
     * Env var: explicit archive directory (overrides the
     * {@code <aeronDirBase>/archive} default).
     */
    private static final String ENV_ARCHIVE_DIR = "OMS_AERON_ARCHIVE_DIR";

    /** Env var: explicit cluster directory. Defaults to {@code <aeronDirBase>/consensus-module}. */
    private static final String ENV_CLUSTER_DIR = "OMS_AERON_CLUSTER_DIR";

    /**
     * Env var: explicit cluster-services directory (state for the
     * {@link ClusteredServiceContainer}). Defaults to
     * {@code <aeronDirBase>/cluster-services}.
     */
    private static final String ENV_CLUSTER_SERVICES_DIR = "OMS_AERON_CLUSTER_SERVICES_DIR";

    /**
     * Env var: explicit media-driver directory. Defaults to
     * {@code <aeronDirBase>/media-driver}.
     */
    private static final String ENV_AERON_DIRECTORY = "OMS_AERON_MEDIA_DRIVER_DIR";

    /**
     * Env var: Archive {@link Archive.Context#controlChannel control channel} for the cluster's
     * local Archive. Must match field 6 (the archive endpoint) of the corresponding member entry
     * in {@link #ENV_CLUSTER_MEMBERS}. Defaults to {@link #DEFAULT_ARCHIVE_CONTROL_CHANNEL}, which
     * is the single-host {@code localhost:8010} that pairs with
     * {@link #DEFAULT_CLUSTER_MEMBERS_SINGLE_NODE}.
     *
     * <p>Phase 4 Tier 2.5 phase E-3a: required to be overridable so two cluster-node JVMs can run
     * side-by-side on one host (each with its own archive listen port). Production/multi-host
     * deployments override this together with {@link #ENV_CLUSTER_MEMBERS}.
     */
    static final String ENV_ARCHIVE_CONTROL_CHANNEL = "OMS_AERON_ARCHIVE_CONTROL_CHANNEL";

    /**
     * Default Archive control channel for the local single-node cluster. Pairs with the archive
     * endpoint ({@code localhost:8010}) in {@link #DEFAULT_CLUSTER_MEMBERS_SINGLE_NODE}.
     */
    static final String DEFAULT_ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8010";

    private OmsClusterNodeBootstrap() {}

    public static void main(String[] args) {
        ClusterNodePaths paths = resolvePaths();
        ensureDirectoriesExist(paths);

        int memberId = parseMemberId();
        String clusterMembers = envOrDefault(ENV_CLUSTER_MEMBERS, DEFAULT_CLUSTER_MEMBERS_SINGLE_NODE);

        log.info(
                "Booting OMS cluster-node memberId={} clusterMembers='{}' aeronDirBase='{}'",
                memberId,
                clusterMembers,
                paths.aeronDirBase());

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        // Slice 4b: stand the metrics exporter up first so OmsAdmissionClusteredService can register
        // its snapshot meters into a registry that is already serving /metrics. Order on shutdown is the
        // reverse: container -> events recording -> media driver -> metrics exporter.
        //
        // 2026-05-21 zombie-JVM hardening: the JDK HttpServer that backs this exporter starts a
        // non-daemon dispatch thread on `start()` (see jdk.httpserver). If a later step in this
        // bootstrap throws (e.g. ClusteredMediaDriver.launch() seeing a stale ArchiveMarkFile right
        // after a fast PM2 restart), main() exits but the dispatch thread keeps the JVM alive at
        // PID level. PM2 then reports the cluster-node as "online" forever, never auto-restarts,
        // and every cluster client (ingress / projector / fix-egress) hangs on
        // DriverTimeoutException. The try/catch below guarantees ANY bootstrap failure tears the
        // exporter down and calls System.exit(1) so PM2 sees a real crash and applies its
        // restart_delay / max_restarts loop — which lets the Aeron mark-file liveness window
        // (~20s) age out across attempts and recover unattended.
        OmsClusterNodeMetricsExporter metricsExporter =
                new OmsClusterNodeMetricsExporter(OmsClusterNodeMetricsExporter.resolveMetricsPortFromEnv());

        ClusteredMediaDriver clusteredMediaDriver = null;
        EventsRecordingHandle eventsRecording = null;
        ClusteredServiceContainer container = null;
        try {
            clusteredMediaDriver = ClusteredMediaDriver.launch(
                    buildMediaDriverContext(paths),
                    buildArchiveContext(paths),
                    buildConsensusModuleContext(paths, memberId, clusterMembers));

            // Register Archive recording for the projector event stream BEFORE launching the service container.
            // The cluster service's onStart will then create the publication on EVENTS_CHANNEL/EVENTS_STREAM_ID
            // and the already-started recording captures it from byte 0 — which is what makes the projector's
            // cursor a stable monotonic position into a continuous recording.
            eventsRecording = startEventsRecording(paths);

            container = ClusteredServiceContainer.launch(
                    buildServiceContainerContext(
                            paths, new OmsAdmissionClusteredService(metricsExporter.meterRegistry())));
        } catch (Throwable bootstrapFailure) {
            log.error(
                    "OMS cluster-node bootstrap failed; closing partial state and forcing JVM exit",
                    bootstrapFailure);
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(eventsRecording);
            CloseHelper.quietClose(clusteredMediaDriver);
            try {
                metricsExporter.close();
            } catch (Exception suppressed) {
                log.warn("error closing cluster-node metrics exporter during failed bootstrap", suppressed);
            }
            // Force exit even if the JDK HttpServer dispatch thread is still alive. Anything other
            // than System.exit(1) here keeps the JVM as a zombie that PM2 cannot distinguish from a
            // healthy process.
            System.exit(1);
            return;
        }

        // Effectively-final references for the shutdown hook (the try-block locals may have been
        // reassigned during recovery).
        final ClusteredMediaDriver driverRef = clusteredMediaDriver;
        final EventsRecordingHandle eventsRef = eventsRecording;
        final ClusteredServiceContainer containerRef = container;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("OMS cluster-node received shutdown signal");
            try {
                containerRef.close();
            } catch (Exception e) {
                log.warn("error closing cluster service container", e);
            }
            CloseHelper.quietClose(eventsRef);
            try {
                driverRef.close();
            } catch (Exception e) {
                log.warn("error closing clustered media driver", e);
            }
            try {
                metricsExporter.close();
            } catch (Exception e) {
                log.warn("error closing cluster-node metrics exporter", e);
            }
            shutdownLatch.countDown();
        }, "oms-cluster-node-shutdown"));

        log.info("OMS cluster-node started; awaiting shutdown signal");
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts the Aeron Archive recording for the projector event stream
     * ({@link OmsClusterWireFormat#EVENTS_CHANNEL} / {@link OmsClusterWireFormat#EVENTS_STREAM_ID}).
     *
     * <p>Opens its own short-lived {@link Aeron} client and {@link AeronArchive} session against the cluster
     * member's local Archive, calls {@code startRecording(channel, streamId, LOCAL)}, then keeps the
     * AeronArchive session open for the lifetime of the JVM (closed via the returned handle on shutdown).
     * The Archive treats {@code startRecording} as an interest registration: the recording engages as soon
     * as the cluster service's {@code onStart} adds the matching {@code ExclusivePublication}.
     *
     * <p>The Aeron client used here is independent of the one inside the cluster's
     * {@code ClusteredServiceContainer}. They share the same MediaDriver via the Aeron directory; opening a
     * second client adds a single subscriber session, which is well within
     * {@link #DEFAULT_MAX_CONCURRENT_SESSIONS}.
     */
    public static EventsRecordingHandle startEventsRecording(ClusterNodePaths paths) {
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(paths.aeronDirectory()));
        AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .controlRequestChannel("aeron:ipc?term-length=64k")
                .controlResponseChannel("aeron:ipc?term-length=64k");
        AeronArchive archive = AeronArchive.connect(archiveCtx);
        long subscriptionId = archive.startRecording(
                OmsClusterWireFormat.EVENTS_CHANNEL,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                SourceLocation.LOCAL);
        log.info(
                "Registered events recording subscriptionId={} on {}/{}",
                subscriptionId,
                OmsClusterWireFormat.EVENTS_CHANNEL,
                OmsClusterWireFormat.EVENTS_STREAM_ID);
        return new EventsRecordingHandle(archive, aeron, subscriptionId);
    }

    /**
     * Closeable handle owning the Aeron + AeronArchive clients used to register the events recording.
     * Calling {@link #close()} stops the recording subscription and closes both clients.
     */
    public static final class EventsRecordingHandle implements AutoCloseable {

        private final AeronArchive archive;
        private final Aeron aeron;
        private final long subscriptionId;

        EventsRecordingHandle(AeronArchive archive, Aeron aeron, long subscriptionId) {
            this.archive = archive;
            this.aeron = aeron;
            this.subscriptionId = subscriptionId;
        }

        public long subscriptionId() {
            return subscriptionId;
        }

        @Override
        public void close() {
            try {
                archive.stopRecording(subscriptionId);
            } catch (Exception ignored) {
                // best-effort during shutdown; the JVM exit closes the Aeron client either way.
            }
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
        }
    }

    public static MediaDriver.Context buildMediaDriverContext(ClusterNodePaths paths) {
        return new MediaDriver.Context()
                .aeronDirectoryName(paths.aeronDirectory())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(false);
    }

    public static Archive.Context buildArchiveContext(ClusterNodePaths paths) {
        // Phase 4 Tier 2.5 phase E-3a: control channel resolves from ENV_ARCHIVE_CONTROL_CHANNEL
        // so two cluster-node JVMs can co-exist on one host (e.g. Pop! 2-shard bench with shard 0
        // archive on localhost:8010 and shard 1 archive on localhost:9010). Default is unchanged
        // (DEFAULT_ARCHIVE_CONTROL_CHANNEL) so existing single-node deploys are byte-identical.
        return new Archive.Context()
                .aeronDirectoryName(paths.aeronDirectory())
                .archiveDir(new File(paths.archiveDir()))
                .controlChannel(envOrDefault(ENV_ARCHIVE_CONTROL_CHANNEL, DEFAULT_ARCHIVE_CONTROL_CHANNEL))
                .localControlChannel("aeron:ipc?term-length=64k")
                .recordingEventsEnabled(false)
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(false);
    }

    public static ConsensusModule.Context buildConsensusModuleContext(
            ClusterNodePaths paths, int memberId, String clusterMembers) {
        return new ConsensusModule.Context()
                .aeronDirectoryName(paths.aeronDirectory())
                .clusterMemberId(memberId)
                .clusterMembers(clusterMembers)
                .clusterDir(new File(paths.clusterDir()))
                .ingressChannel("aeron:udp?term-length=64k")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .maxConcurrentSessions(parseMaxConcurrentSessions())
                .archiveContext(new AeronArchive.Context()
                        .aeronDirectoryName(paths.aeronDirectory())
                        .controlRequestChannel("aeron:ipc?term-length=64k")
                        .controlResponseChannel("aeron:ipc?term-length=64k"));
    }

    static int parseMaxConcurrentSessions() {
        String raw = System.getenv(ENV_MAX_CONCURRENT_SESSIONS);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_MAX_CONCURRENT_SESSIONS;
        }
        try {
            int parsed = Integer.parseInt(raw.trim());
            if (parsed < 1) {
                throw new IllegalArgumentException(
                        String.format(
                                Locale.ROOT,
                                "Invalid %s='%s'; expected a positive integer",
                                ENV_MAX_CONCURRENT_SESSIONS,
                                raw));
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            Locale.ROOT,
                            "Invalid %s='%s'; expected a positive integer",
                            ENV_MAX_CONCURRENT_SESSIONS,
                            raw),
                    e);
        }
    }

    /**
     * Default-service overload used by tests that don't care about snapshot metrics. Delegates to
     * {@link #buildServiceContainerContext(ClusterNodePaths, OmsAdmissionClusteredService)} with a
     * fresh {@link OmsAdmissionClusteredService} (default ctor; meters land in
     * {@code Metrics.globalRegistry}).
     */
    public static ClusteredServiceContainer.Context buildServiceContainerContext(ClusterNodePaths paths) {
        return buildServiceContainerContext(paths, new OmsAdmissionClusteredService());
    }

    /**
     * Slice 4b overload: caller supplies the {@link OmsAdmissionClusteredService} (typically built
     * with the cluster-node metrics exporter's {@code MeterRegistry}). Tests that assert on snapshot
     * observability use this overload to register meters into a {@code SimpleMeterRegistry} they can
     * inspect after the cluster runs.
     */
    public static ClusteredServiceContainer.Context buildServiceContainerContext(
            ClusterNodePaths paths, OmsAdmissionClusteredService service) {
        return new ClusteredServiceContainer.Context()
                .aeronDirectoryName(paths.aeronDirectory())
                .clusterDir(new File(paths.clusterServicesDir()))
                .clusteredService(service)
                .archiveContext(new AeronArchive.Context()
                        .aeronDirectoryName(paths.aeronDirectory())
                        .controlRequestChannel("aeron:ipc?term-length=64k")
                        .controlResponseChannel("aeron:ipc?term-length=64k"));
    }

    static int parseMemberId() {
        String raw = System.getenv(ENV_MEMBER_ID);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_MEMBER_ID;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            Locale.ROOT,
                            "Invalid %s='%s'; expected an integer",
                            ENV_MEMBER_ID,
                            raw),
                    e);
        }
    }

    static ClusterNodePaths resolvePaths() {
        String aeronDirBase = envOrDefault(ENV_AERON_DIR_BASE, DEFAULT_AERON_DIR_BASE);
        String aeronDirectory = envOrDefault(
                ENV_AERON_DIRECTORY, aeronDirBase + File.separator + DEFAULT_AERON_MEDIA_DRIVER_DIR_NAME);
        String archiveDir = envOrDefault(
                ENV_ARCHIVE_DIR, aeronDirBase + File.separator + DEFAULT_ARCHIVE_DIR_NAME);
        String clusterDir = envOrDefault(
                ENV_CLUSTER_DIR, aeronDirBase + File.separator + DEFAULT_CLUSTER_DIR_NAME);
        String clusterServicesDir = envOrDefault(
                ENV_CLUSTER_SERVICES_DIR, aeronDirBase + File.separator + DEFAULT_CLUSTER_SERVICES_DIR_NAME);
        return new ClusterNodePaths(aeronDirBase, aeronDirectory, archiveDir, clusterDir, clusterServicesDir);
    }

    private static void ensureDirectoriesExist(ClusterNodePaths paths) {
        for (String dir : new String[] {
                paths.aeronDirBase(), paths.archiveDir(), paths.clusterDir(), paths.clusterServicesDir()
        }) {
            File f = new File(dir);
            if (!f.exists() && !f.mkdirs()) {
                throw new IllegalStateException("failed to create Aeron working directory: " + dir);
            }
        }
    }

    private static String envOrDefault(String name, String defaultValue) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? defaultValue : v;
    }

    /**
     * Resolved on-disk locations for a single cluster node. The media-driver
     * directory lives under {@code aeronDirBase} as well; the archive,
     * consensus-module, and service-container subdirectories are siblings.
     *
     * <p>Visible for tests; not part of any public API contract.
     */
    public record ClusterNodePaths(
            String aeronDirBase,
            String aeronDirectory,
            String archiveDir,
            String clusterDir,
            String clusterServicesDir) {}
}
