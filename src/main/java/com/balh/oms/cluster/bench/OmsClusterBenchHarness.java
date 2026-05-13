package com.balh.oms.cluster.bench;

import com.balh.oms.OmsClusterNodeBootstrap;
import com.balh.oms.OmsClusterNodeBootstrap.ClusterNodePaths;
import com.balh.oms.OmsClusterNodeBootstrap.EventsRecordingHandle;
import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OmsAdmissionClusteredService;
import com.balh.oms.cluster.OmsClusterWireFormat;

import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;

import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Phase 4 slice 4e of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: hand-rolled,
 * in-process cluster load harness. Boots a single-node Aeron Cluster (the same way
 * {@link OmsClusterNodeBootstrap#main} does — {@link ClusteredMediaDriver} + events recording +
 * {@link ClusteredServiceContainer} hosting {@link OmsAdmissionClusteredService}), connects a raw
 * {@link AeronCluster} client (mirrors the snapshot IT pattern; deliberately Spring-free so the
 * harness has the same blast radius as {@code OmsClusterSnapshotAdminTool}), runs a warmup phase
 * followed by a steady-state {@link AcceptOrderCommand} offer loop at a configurable target rate,
 * records per-command commit-round-trip latencies into a lossless {@link Histogram}, and writes
 * {@code summary.md} + {@code histogram.hgrm} to a per-run report directory.
 *
 * <p>This is the foundation slice 4f / 4g build on. It is <em>not</em> a JMH benchmark (JMH lands in
 * 4f for allocation profiling specifically). It is <em>not</em> a multi-process load test (Phase 5
 * adds k8s-driven load). It exists to give us a defensible p50/p95/p99/p99.9 number on the same JVM
 * the cluster runs in, with an HdrHistogram instead of Micrometer's lossy summary buckets.
 *
 * <h2>Configuration</h2>
 *
 * All configuration is via env var with documented defaults — same shape as
 * {@link OmsClusterNodeBootstrap}, no Spring config validation. Run via the {@code clusterBench}
 * Gradle task which forwards env unchanged.
 *
 * <table>
 *   <caption>Env vars</caption>
 *   <tr><th>Env var</th><th>Default</th><th>Meaning</th></tr>
 *   <tr><td>{@value #ENV_DURATION_S}</td><td>{@value #DEFAULT_DURATION_S}</td>
 *       <td>Steady-state run length in seconds.</td></tr>
 *   <tr><td>{@value #ENV_THROUGHPUT_OPS_PER_S}</td><td>{@value #DEFAULT_THROUGHPUT_OPS_PER_S}</td>
 *       <td>Target offer rate during the steady-state phase. The pacer sleeps to this cadence; if the
 *       cluster slows below the target the offer thread keeps firing as fast as it can — see
 *       coordinated-omission note below.</td></tr>
 *   <tr><td>{@value #ENV_WARMUP_S}</td><td>{@value #DEFAULT_WARMUP_S}</td>
 *       <td>Warmup duration in seconds — same offer rate, results discarded. Lets HotSpot tier up
 *       the hot paths in the cluster service before the histogram starts capturing.</td></tr>
 *   <tr><td>{@value #ENV_TIMEOUT_MS}</td><td>{@value #DEFAULT_TIMEOUT_MS}</td>
 *       <td>Per-command deadline. Round-trips that exceed this fall into the {@code timeout}
 *       outcome bucket.</td></tr>
 *   <tr><td>{@value #ENV_REPORT_DIR}</td><td>{@code build/reports/cluster-bench/<timestamp>}</td>
 *       <td>Output directory. Created if absent.</td></tr>
 *   <tr><td>{@value #ENV_AERON_DIR_BASE}</td><td>{@code build/cluster-bench/<timestamp>/aeron}</td>
 *       <td>Aeron working directories root. Each run uses a fresh sub-tree to avoid stepping on a
 *       prior run's media driver.</td></tr>
 * </table>
 *
 * <h2>Coordinated omission</h2>
 *
 * <p>Recording uses {@link Histogram#recordValueWithExpectedInterval} so when the cluster lags
 * below the target rate the histogram backfills phantom samples covering the missed intervals.
 * Without this, a slow batch of requests would only contribute one sample each — vastly
 * understating tail latency (the classic "coordinated omission" pitfall). See Gil Tene's writeup
 * for the math.
 *
 * <h2>Threading</h2>
 *
 * <p>Single-threaded. {@link AeronCluster} is thread-confined; the harness offers + polls + records
 * on the calling thread. Slice 4g may add a multi-thread variant (with a single dedicated
 * cluster-client thread fed by an MPSC queue) once we know whether single-thread saturates Aeron
 * before we hit the SLO target. v1 keeps the surface small.
 */
public final class OmsClusterBenchHarness {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterBenchHarness.class);

    public static final String ENV_DURATION_S = "OMS_BENCH_DURATION_S";
    public static final String ENV_THROUGHPUT_OPS_PER_S = "OMS_BENCH_THROUGHPUT_OPS_PER_S";
    public static final String ENV_WARMUP_S = "OMS_BENCH_WARMUP_S";
    public static final String ENV_TIMEOUT_MS = "OMS_BENCH_TIMEOUT_MS";
    public static final String ENV_REPORT_DIR = "OMS_BENCH_REPORT_DIR";
    public static final String ENV_AERON_DIR_BASE = "OMS_BENCH_AERON_DIR_BASE";

    public static final int DEFAULT_DURATION_S = 30;
    public static final int DEFAULT_THROUGHPUT_OPS_PER_S = 1_000;
    public static final int DEFAULT_WARMUP_S = 5;
    public static final int DEFAULT_TIMEOUT_MS = 5_000;

    /** Loopback cluster-members tuple matching {@link OmsClusterNodeBootstrap}'s default. */
    private static final String CLUSTER_MEMBERS_SINGLE_NODE =
            "0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010";

    /**
     * Connect-time wall-clock budget. Generous because cold cluster boot includes media driver +
     * archive + consensus module + service container; on a slow CI runner this can take a few
     * seconds.
     */
    private static final long CONNECT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(20);

    /** Park granularity inside hot loops (offer back-pressure + pacer wait). */
    private static final long POLL_PARK_NANOS = 10_000L;

    /** Aeron message-timeout for the {@link AeronCluster} client. */
    private static final long AERON_MESSAGE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /** Encoded buffer capacity per command. */
    private static final int COMMAND_BUFFER_BYTES = OmsClusterWireFormat.MAX_COMMAND_BYTES;

    /** Live-progress log cadence — once per second so a long run shows it's not hung. */
    private static final long PROGRESS_LOG_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    /** Histogram covers 1 µs .. 60 s with 3 significant digits — total memory ~1 MB. */
    private static final long HISTOGRAM_LOWEST_DISCERNIBLE_MICROS = 1L;

    private static final long HISTOGRAM_HIGHEST_TRACKABLE_MICROS = TimeUnit.SECONDS.toMicros(60);

    private static final int HISTOGRAM_SIGNIFICANT_DIGITS = 3;

    private OmsClusterBenchHarness() {}

    public static void main(String[] args) {
        try {
            run(Config.fromEnv());
        } catch (Exception e) {
            log.error("OmsClusterBenchHarness failed", e);
            System.exit(1);
        }
    }

    /**
     * Programmatic entry point. Same behaviour as {@link #main} but takes a {@link Config} directly
     * — used by {@link OmsClusterBenchHarnessIT} so the smoke test can drive the harness without
     * shimming env vars. Returns the {@link BenchResult} for additional assertions; the on-disk
     * {@code summary.md} / {@code histogram.hgrm} are also written before return.
     */
    public static BenchResult run(Config config) throws IOException {
        log.info("OmsClusterBenchHarness starting: {}", config);

        ensureReportDir(config.reportDir);

        ClusterNodePaths paths = pathsUnder(config.aeronDirBase);
        ensureClusterDirs(paths);
        IoUtil.delete(new File(paths.aeronDirectory()), /* ignoreFailures = */ true);

        ClusteredMediaDriver clusteredMediaDriver = null;
        EventsRecordingHandle eventsRecording = null;
        ClusteredServiceContainer container = null;
        AeronCluster client = null;
        try {
            clusteredMediaDriver = ClusteredMediaDriver.launch(
                    OmsClusterNodeBootstrap.buildMediaDriverContext(paths),
                    OmsClusterNodeBootstrap.buildArchiveContext(paths),
                    OmsClusterNodeBootstrap.buildConsensusModuleContext(
                            paths, /* memberId = */ 0, CLUSTER_MEMBERS_SINGLE_NODE));
            eventsRecording = OmsClusterNodeBootstrap.startEventsRecording(paths);
            container = ClusteredServiceContainer.launch(OmsClusterNodeBootstrap
                    .buildServiceContainerContext(paths, new OmsAdmissionClusteredService()));

            EgressDemux demux = new EgressDemux();
            AeronCluster.Context clientContext = new AeronCluster.Context()
                    .aeronDirectoryName(paths.aeronDirectory())
                    .ingressChannel("aeron:udp?endpoint=localhost:0")
                    .ingressEndpoints("0=localhost:20110")
                    .egressChannel("aeron:udp?endpoint=localhost:0")
                    .egressListener(demux)
                    .messageTimeoutNs(AERON_MESSAGE_TIMEOUT_NS);

            client = connectWithRetry(clientContext);
            log.info("Cluster client connected — starting warmup ({} s @ {} ops/s)",
                    config.warmupSeconds, config.throughputOpsPerSec);

            BenchResult result = runBench(client, demux, config);

            writeOutputs(result, config);
            log.info("OmsClusterBenchHarness done: see {}", config.reportDir.toAbsolutePath());
            return result;
        } finally {
            closeQuietly("AeronCluster client", client);
            closeQuietly("ClusteredServiceContainer", container);
            closeQuietly("EventsRecording", eventsRecording);
            closeQuietly("ClusteredMediaDriver", clusteredMediaDriver);
        }
    }

    /* ------------------------------------------------------------------ *
     *  Bench loop                                                        *
     * ------------------------------------------------------------------ */

    static BenchResult runBench(AeronCluster client, EgressDemux demux, Config config) {
        long opIntervalNanos = Math.max(1L, TimeUnit.SECONDS.toNanos(1) / config.throughputOpsPerSec);
        long warmupEndNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.warmupSeconds);
        long steadyEndNanos = warmupEndNanos + TimeUnit.SECONDS.toNanos(config.durationSeconds);
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(config.timeoutMs);

        Histogram commitHistogram = newHistogram();
        long offered = 0L;
        long commits = 0L;
        long timeouts = 0L;
        long errors = 0L;

        long phaseStartNanos = System.nanoTime();
        long nextScheduledNanos = phaseStartNanos;
        long nextProgressLogNanos = phaseStartNanos + PROGRESS_LOG_INTERVAL_NS;
        boolean inSteadyState = false;
        long correlationId = 1L;

        while (true) {
            long now = System.nanoTime();
            if (!inSteadyState && now >= warmupEndNanos) {
                inSteadyState = true;
                phaseStartNanos = now;
                log.info("Warmup done after {} commits / {} timeouts / {} errors — entering steady state ({} s)",
                        commits, timeouts, errors, config.durationSeconds);
                commits = 0L;
                timeouts = 0L;
                errors = 0L;
                offered = 0L;
            }
            if (inSteadyState && now >= steadyEndNanos) {
                break;
            }
            if (nextScheduledNanos > now) {
                LockSupport.parkNanos(Math.min(POLL_PARK_NANOS, nextScheduledNanos - now));
                continue;
            }

            long latencyNanos = submitOnce(client, demux, correlationId++, timeoutNanos);
            offered++;
            if (latencyNanos == OUTCOME_TIMEOUT) {
                timeouts++;
            } else if (latencyNanos == OUTCOME_ERROR) {
                errors++;
            } else {
                commits++;
                if (inSteadyState) {
                    long latencyMicros = TimeUnit.NANOSECONDS.toMicros(latencyNanos);
                    commitHistogram.recordValueWithExpectedInterval(
                            clamp(latencyMicros, HISTOGRAM_LOWEST_DISCERNIBLE_MICROS,
                                    HISTOGRAM_HIGHEST_TRACKABLE_MICROS),
                            TimeUnit.NANOSECONDS.toMicros(opIntervalNanos));
                }
            }

            nextScheduledNanos += opIntervalNanos;
            if (now >= nextProgressLogNanos) {
                log.info("[{}] offered={} commits={} timeouts={} errors={}",
                        inSteadyState ? "steady" : "warmup", offered, commits, timeouts, errors);
                nextProgressLogNanos = now + PROGRESS_LOG_INTERVAL_NS;
            }
        }

        return new BenchResult(config, commitHistogram, offered, commits, timeouts, errors);
    }

    /** Internal sentinels distinguishing outcome categories without throwing on the hot path. */
    private static final long OUTCOME_TIMEOUT = -1L;

    private static final long OUTCOME_ERROR = -2L;

    /**
     * Offers one {@link AcceptOrderCommand} and waits for the matching egress.
     *
     * @return latency in nanoseconds on commit; {@link #OUTCOME_TIMEOUT} if the deadline expired;
     *     {@link #OUTCOME_ERROR} on any other failure.
     */
    static long submitOnce(AeronCluster client, EgressDemux demux, long correlationId, long timeoutNanos) {
        AcceptOrderCommand cmd = buildCommand(correlationId);
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(COMMAND_BUFFER_BYTES);
        int written = cmd.encode(buffer, 0);

        long start = System.nanoTime();
        long deadline = start + timeoutNanos;
        try {
            long offerResult;
            do {
                offerResult = client.offer(buffer, 0, written);
                if (offerResult < 0L) {
                    if (System.nanoTime() > deadline) {
                        return OUTCOME_TIMEOUT;
                    }
                    LockSupport.parkNanos(POLL_PARK_NANOS);
                }
            } while (offerResult < 0L);

            while (true) {
                if (demux.consume(correlationId)) {
                    return System.nanoTime() - start;
                }
                client.pollEgress();
                if (demux.consume(correlationId)) {
                    return System.nanoTime() - start;
                }
                if (System.nanoTime() > deadline) {
                    return OUTCOME_TIMEOUT;
                }
                LockSupport.parkNanos(POLL_PARK_NANOS);
            }
        } catch (RuntimeException e) {
            log.warn("submitOnce failed for correlationId={}: {}", correlationId, e.toString());
            return OUTCOME_ERROR;
        }
    }

    private static AcceptOrderCommand buildCommand(long correlationId) {
        return new AcceptOrderCommand(
                correlationId,
                UUID.randomUUID(),
                System.nanoTime(),
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "bench-account",
                "bench-idem-" + correlationId,
                "bench-hash-" + correlationId,
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);
    }

    /* ------------------------------------------------------------------ *
     *  Output                                                            *
     * ------------------------------------------------------------------ */

    static void writeOutputs(BenchResult result, Config config) throws IOException {
        Histogram h = result.histogram;
        Path summary = config.reportDir.resolve("summary.md");
        try (PrintStream out = new PrintStream(Files.newOutputStream(summary), true, StandardCharsets.UTF_8)) {
            out.println("# OMS cluster bench summary");
            out.println();
            out.println("Generated " + Instant.now() + " by `OmsClusterBenchHarness`.");
            out.println();
            out.println("## Configuration");
            out.println();
            out.println("| Parameter | Value |");
            out.println("| --- | --- |");
            out.println("| `" + ENV_WARMUP_S + "` | " + config.warmupSeconds + " s |");
            out.println("| `" + ENV_DURATION_S + "` | " + config.durationSeconds + " s |");
            out.println("| `" + ENV_THROUGHPUT_OPS_PER_S + "` | " + config.throughputOpsPerSec + " ops/s |");
            out.println("| `" + ENV_TIMEOUT_MS + "` | " + config.timeoutMs + " ms |");
            out.println();
            out.println("## Outcomes (steady-state phase only)");
            out.println();
            out.println("| Outcome | Count |");
            out.println("| --- | ---: |");
            out.println("| commit | " + result.commits + " |");
            out.println("| timeout | " + result.timeouts + " |");
            out.println("| error | " + result.errors + " |");
            out.println();
            out.println("Total offered (steady): " + result.offered);
            out.println();
            out.println("## Latency (commit only, microseconds)");
            out.println();
            out.println("| Percentile | µs |");
            out.println("| --- | ---: |");
            out.println("| p50 | " + h.getValueAtPercentile(50.0) + " |");
            out.println("| p90 | " + h.getValueAtPercentile(90.0) + " |");
            out.println("| p95 | " + h.getValueAtPercentile(95.0) + " |");
            out.println("| p99 | " + h.getValueAtPercentile(99.0) + " |");
            out.println("| p99.9 | " + h.getValueAtPercentile(99.9) + " |");
            out.println("| p99.99 | " + h.getValueAtPercentile(99.99) + " |");
            out.println("| max | " + h.getMaxValue() + " |");
            out.println();
            out.println("Recorded with `Histogram.recordValueWithExpectedInterval(...)` "
                    + "(coordinated-omission corrected at expected interval = "
                    + (TimeUnit.SECONDS.toMicros(1) / config.throughputOpsPerSec) + " µs).");
            out.println();
            out.println("Full distribution: see `histogram.hgrm` (HdrHistogram standard format; load with "
                    + "`HistogramLogProcessor` or any HdrHistogram-aware viewer).");
        }

        Path histogram = config.reportDir.resolve("histogram.hgrm");
        try (PrintStream out = new PrintStream(Files.newOutputStream(histogram), true, StandardCharsets.UTF_8)) {
            h.outputPercentileDistribution(out, /* outputValueUnitScalingRatio = */ 1.0);
        }
    }

    /* ------------------------------------------------------------------ *
     *  Egress demux                                                      *
     * ------------------------------------------------------------------ */

    /**
     * Tracks which correlation ids have egressed back. The harness offers commands serially so a
     * plain {@link HashMap} on the calling thread is safe — same property as
     * {@link com.balh.oms.cluster.OmsClusterIngressClient}.
     */
    static final class EgressDemux implements EgressListener {

        private final Map<Long, Boolean> received = new HashMap<>();

        /** Returns {@code true} if this correlation id has been acked since the last call. */
        boolean consume(long correlationId) {
            return received.remove(correlationId) != null;
        }

        @Override
        public void onMessage(
                long clusterSessionId,
                long timestamp,
                DirectBuffer buffer,
                int offset,
                int length,
                Header header) {
            if (length < OmsClusterWireFormat.HEADER_LENGTH) {
                return;
            }
            int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
            if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED
                    || typeId == OmsClusterWireFormat.TYPE_ID_ORDER_REJECTED) {
                long correlationId =
                        buffer.getLong(offset + OmsClusterWireFormat.HEADER_CORRELATION_ID_OFFSET);
                received.put(correlationId, Boolean.TRUE);
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
    }

    /* ------------------------------------------------------------------ *
     *  Plumbing                                                          *
     * ------------------------------------------------------------------ */

    static AeronCluster connectWithRetry(AeronCluster.Context ctx) {
        long deadline = System.nanoTime() + CONNECT_TIMEOUT_NANOS;
        RuntimeException last = null;
        while (System.nanoTime() < deadline) {
            try {
                return AeronCluster.connect(ctx.clone());
            } catch (RuntimeException e) {
                last = e;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
        throw new IllegalStateException("AeronCluster connect timed out", last);
    }

    static Histogram newHistogram() {
        return new Histogram(
                HISTOGRAM_LOWEST_DISCERNIBLE_MICROS,
                HISTOGRAM_HIGHEST_TRACKABLE_MICROS,
                HISTOGRAM_SIGNIFICANT_DIGITS);
    }

    static long clamp(long value, long lo, long hi) {
        if (value < lo) {
            return lo;
        }
        if (value > hi) {
            return hi;
        }
        return value;
    }

    static ClusterNodePaths pathsUnder(Path base) {
        return new ClusterNodePaths(
                base.toString(),
                base.resolve("media-driver").toString(),
                base.resolve("archive").toString(),
                base.resolve("consensus-module").toString(),
                base.resolve("cluster-services").toString());
    }

    static void ensureClusterDirs(ClusterNodePaths paths) {
        for (String dir : new String[] {
                paths.aeronDirBase(), paths.archiveDir(), paths.clusterDir(), paths.clusterServicesDir()
        }) {
            File f = new File(dir);
            if (!f.exists() && !f.mkdirs()) {
                throw new IllegalStateException("could not create dir: " + dir);
            }
        }
    }

    static void ensureReportDir(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new IllegalStateException("could not create report dir: " + dir, e);
        }
    }

    static void closeQuietly(String label, AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("Failed to close {}: {}", label, e.toString());
        }
    }

    /* ------------------------------------------------------------------ *
     *  Config + result records                                           *
     * ------------------------------------------------------------------ */

    /** Resolved bench configuration. Single source of truth — no other code reads env directly. */
    public record Config(
            int warmupSeconds,
            int durationSeconds,
            int throughputOpsPerSec,
            int timeoutMs,
            Path reportDir,
            Path aeronDirBase) {

        public static Config fromEnv() {
            String runStamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
                    .withZone(ZoneOffset.UTC)
                    .format(Instant.now());
            int warmup = parsePositiveInt(ENV_WARMUP_S, DEFAULT_WARMUP_S);
            int duration = parsePositiveInt(ENV_DURATION_S, DEFAULT_DURATION_S);
            int throughput = parsePositiveInt(ENV_THROUGHPUT_OPS_PER_S, DEFAULT_THROUGHPUT_OPS_PER_S);
            int timeout = parsePositiveInt(ENV_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
            Path reportDir = Path.of(System.getenv()
                    .getOrDefault(ENV_REPORT_DIR, "build/reports/cluster-bench/" + runStamp));
            Path aeronDir = Path.of(System.getenv()
                    .getOrDefault(ENV_AERON_DIR_BASE, "build/cluster-bench/" + runStamp + "/aeron"));
            return new Config(warmup, duration, throughput, timeout, reportDir, aeronDir);
        }

        private static int parsePositiveInt(String envName, int defaultValue) {
            String raw = System.getenv(envName);
            if (raw == null || raw.isBlank()) {
                return defaultValue;
            }
            try {
                int v = Integer.parseInt(raw.trim());
                if (v <= 0) {
                    log.warn("{}={} not positive; using default {}", envName, raw, defaultValue);
                    return defaultValue;
                }
                return v;
            } catch (NumberFormatException e) {
                log.warn("{}={} is not an int; using default {}", envName, raw, defaultValue);
                return defaultValue;
            }
        }
    }

    /** Result of a single bench run. {@code histogram} covers the steady-state phase only. */
    record BenchResult(
            Config config,
            Histogram histogram,
            long offered,
            long commits,
            long timeouts,
            long errors) {}
}
