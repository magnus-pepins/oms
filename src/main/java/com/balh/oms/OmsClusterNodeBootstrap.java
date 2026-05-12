package com.balh.oms;

import com.balh.oms.cluster.OmsAdmissionClusteredService;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
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

        ClusteredMediaDriver clusteredMediaDriver = ClusteredMediaDriver.launch(
                buildMediaDriverContext(paths),
                buildArchiveContext(paths),
                buildConsensusModuleContext(paths, memberId, clusterMembers));

        ClusteredServiceContainer container = ClusteredServiceContainer.launch(
                buildServiceContainerContext(paths));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("OMS cluster-node received shutdown signal");
            try {
                container.close();
            } catch (Exception e) {
                log.warn("error closing cluster service container", e);
            }
            try {
                clusteredMediaDriver.close();
            } catch (Exception e) {
                log.warn("error closing clustered media driver", e);
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

    public static MediaDriver.Context buildMediaDriverContext(ClusterNodePaths paths) {
        return new MediaDriver.Context()
                .aeronDirectoryName(paths.aeronDirectory())
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(false);
    }

    public static Archive.Context buildArchiveContext(ClusterNodePaths paths) {
        return new Archive.Context()
                .aeronDirectoryName(paths.aeronDirectory())
                .archiveDir(new File(paths.archiveDir()))
                .controlChannel("aeron:udp?endpoint=localhost:8010")
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

    public static ClusteredServiceContainer.Context buildServiceContainerContext(ClusterNodePaths paths) {
        return new ClusteredServiceContainer.Context()
                .aeronDirectoryName(paths.aeronDirectory())
                .clusterDir(new File(paths.clusterServicesDir()))
                .clusteredService(new OmsAdmissionClusteredService())
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
