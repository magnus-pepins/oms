package com.balh.oms.cluster.admin;

import io.aeron.cluster.ClusterTool;

import java.io.File;
import java.util.Locale;

/**
 * Operator entry point that triggers a snapshot on a running Aeron Cluster member.
 *
 * <p>Aeron 1.48.0 has no built-in periodic snapshot setter on
 * {@code ConsensusModule.Context} (verified — the Context exposes
 * {@code snapshotChannel}, {@code snapshotStreamId},
 * {@code totalSnapshotDurationThresholdNs}, {@code snapshotCounter}, and
 * {@code acceptStandbySnapshots}, but no {@code snapshotIntervalNs}). Snapshots fire
 * only via two paths: operator-driven through {@link ClusterTool#snapshot(File, java.io.PrintStream)}
 * (this tool's path), or application-driven from inside {@code ClusteredService} via
 * {@code Cluster.scheduleTimer} + {@code ClusterControl.ToggleState.SNAPSHOT.toggle(...)}.
 * The application-timer path is deliberately deferred to a later phase-4 slice; this tool
 * is the first usable path that bounds the cluster's restart-replay window without changing
 * the {@code OmsAdmissionClusteredService} snapshot schema.
 *
 * <p>Wraps {@link ClusterTool#snapshot(File, java.io.PrintStream)}: writes the toggle
 * counter, returns true if the cluster acknowledges the snapshot request before the
 * underlying ClusterTool timeout. The snapshot itself completes asynchronously on the
 * leader; success here means "command accepted", not "snapshot persisted to recording".
 *
 * <h3>Configuration</h3>
 *
 * <p>Cluster dir resolved (in order): {@link #ENV_CLUSTER_DIR} environment variable,
 * then {@code <}{@link #ENV_AERON_DIR_BASE}{@code >/}{@link #DEFAULT_CLUSTER_DIR_NAME}
 * (mirroring {@code OmsClusterNodeBootstrap.resolvePaths}), then
 * {@code <}{@link #DEFAULT_AERON_DIR_BASE}{@code >/}{@link #DEFAULT_CLUSTER_DIR_NAME}.
 * No magic-number / magic-path literals (per {@code .cursor/rules/config-and-limits.mdc}).
 *
 * <h3>Exit codes</h3>
 *
 * <ul>
 *   <li>{@code 0} — {@link ClusterTool#snapshot} returned {@code true}; snapshot request
 *   was accepted by the cluster.</li>
 *   <li>{@code 1} — {@link ClusterTool#snapshot} returned {@code false} or threw; the
 *   cluster did not accept the request (typically: not running, wrong dir, leader busy
 *   past the ClusterTool timeout).</li>
 * </ul>
 */
public final class OmsClusterSnapshotAdminTool {

    /** Env var: explicit cluster directory (consensus-module dir, contains the cluster mark file). */
    public static final String ENV_CLUSTER_DIR = "OMS_AERON_CLUSTER_DIR";

    /** Env var: base directory for all Aeron working state on this node (matches {@code OmsClusterNodeBootstrap}). */
    public static final String ENV_AERON_DIR_BASE = "OMS_AERON_DIR_BASE";

    /** Default base directory when neither env var is set (matches the cluster-node default). */
    public static final String DEFAULT_AERON_DIR_BASE = "build/aeron-cluster";

    /** Subdirectory name under {@code aeronDirBase} that holds the consensus module / cluster state. */
    public static final String DEFAULT_CLUSTER_DIR_NAME = "consensus-module";

    /** Process exit code for {@code ClusterTool.snapshot} returning {@code true}. */
    public static final int EXIT_SUCCESS = 0;

    /** Process exit code for {@code ClusterTool.snapshot} returning {@code false} or throwing. */
    public static final int EXIT_FAILURE = 1;

    private OmsClusterSnapshotAdminTool() {}

    public static void main(String[] args) {
        File clusterDir = resolveClusterDir();
        System.out.printf(
                Locale.ROOT,
                "OmsClusterSnapshotAdminTool: requesting snapshot for clusterDir=%s%n",
                clusterDir.getAbsolutePath());
        boolean accepted;
        try {
            accepted = ClusterTool.snapshot(clusterDir, System.out);
        } catch (RuntimeException e) {
            System.err.printf(
                    Locale.ROOT,
                    "ClusterTool.snapshot failed for clusterDir=%s: %s%n",
                    clusterDir.getAbsolutePath(),
                    e);
            System.exit(EXIT_FAILURE);
            return;
        }
        if (!accepted) {
            System.err.printf(
                    Locale.ROOT,
                    "ClusterTool.snapshot did not accept the request for clusterDir=%s%n",
                    clusterDir.getAbsolutePath());
            System.exit(EXIT_FAILURE);
            return;
        }
        System.out.println("ClusterTool.snapshot request accepted; snapshot completes asynchronously on the leader.");
        System.exit(EXIT_SUCCESS);
    }

    /**
     * Resolves the cluster directory using the env var precedence documented on the class.
     *
     * <p>Visible for tests so the smoke IT can drive the same resolution path.
     */
    public static File resolveClusterDir() {
        String explicit = System.getenv(ENV_CLUSTER_DIR);
        if (explicit != null && !explicit.isBlank()) {
            return new File(explicit);
        }
        String base = System.getenv(ENV_AERON_DIR_BASE);
        if (base == null || base.isBlank()) {
            base = DEFAULT_AERON_DIR_BASE;
        }
        return new File(base + File.separator + DEFAULT_CLUSTER_DIR_NAME);
    }
}
