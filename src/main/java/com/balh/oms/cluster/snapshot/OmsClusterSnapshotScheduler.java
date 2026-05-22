package com.balh.oms.cluster.snapshot;

import io.aeron.cluster.ClusterTool;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Periodic {@link ClusterTool#snapshot} trigger (Phase 2.2). */
public final class OmsClusterSnapshotScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterSnapshotScheduler.class);

    public static final long DEFAULT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5L);
    public static final long DEFAULT_INITIAL_DELAY_MS = TimeUnit.MINUTES.toMillis(1L);
    public static final long NEVER_SUCCEEDED_EPOCH_MS = 0L;
    public static final long NEVER_SUCCEEDED_AGE_SECONDS = TimeUnit.DAYS.toSeconds(3650L);

    public static final String METRIC_SECONDS_SINCE_LAST_SNAPSHOT =
            "oms_cluster_seconds_since_last_snapshot";

    private final File clusterDir;
    private final long intervalMs;
    private final long initialDelayMs;
    private final Thread schedulerThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong attemptCount = new AtomicLong(0L);
    private final AtomicLong successCount = new AtomicLong(0L);
    private final AtomicLong lastSuccessEpochMs = new AtomicLong(NEVER_SUCCEEDED_EPOCH_MS);
    private final Counter attemptsCounter;
    private final Counter successesCounter;
    private final Counter failuresCounter;

    public OmsClusterSnapshotScheduler(File clusterDir, long intervalMs, long initialDelayMs) {
        this(clusterDir, intervalMs, initialDelayMs, null);
    }

    public OmsClusterSnapshotScheduler(
            File clusterDir, long intervalMs, long initialDelayMs, MeterRegistry meterRegistry) {
        if (intervalMs <= 0L) {
            throw new IllegalArgumentException("intervalMs must be > 0");
        }
        if (initialDelayMs < 0L) {
            throw new IllegalArgumentException("initialDelayMs must be >= 0");
        }
        this.clusterDir = Objects.requireNonNull(clusterDir, "clusterDir");
        this.intervalMs = intervalMs;
        this.initialDelayMs = initialDelayMs;
        this.schedulerThread = new Thread(this::loop, "oms-cluster-snapshot-scheduler");
        this.schedulerThread.setDaemon(true);
        if (meterRegistry != null) {
            Gauge.builder(METRIC_SECONDS_SINCE_LAST_SNAPSHOT, this, s -> (double) s.secondsSinceLastSuccess())
                    .description("Seconds since last successful OMS cluster snapshot trigger.")
                    .baseUnit("seconds")
                    .register(meterRegistry);
            this.attemptsCounter =
                    Counter.builder("oms_cluster_snapshot_attempts_total").register(meterRegistry);
            this.successesCounter =
                    Counter.builder("oms_cluster_snapshot_successes_total").register(meterRegistry);
            this.failuresCounter =
                    Counter.builder("oms_cluster_snapshot_failures_total").register(meterRegistry);
        } else {
            this.attemptsCounter = null;
            this.successesCounter = null;
            this.failuresCounter = null;
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info(
                    "starting OMS cluster snapshot scheduler: clusterDir='{}' intervalMs={}",
                    clusterDir.getAbsolutePath(),
                    intervalMs);
            schedulerThread.start();
        }
    }

    public boolean snapshotNow() {
        attemptCount.incrementAndGet();
        if (attemptsCounter != null) {
            attemptsCounter.increment();
        }
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
            boolean triggered = ClusterTool.snapshot(clusterDir, out);
            String output = captured.toString(StandardCharsets.UTF_8).trim();
            if (triggered) {
                successCount.incrementAndGet();
                lastSuccessEpochMs.set(System.currentTimeMillis());
                if (successesCounter != null) {
                    successesCounter.increment();
                }
                log.info("OMS cluster snapshot triggered: output='{}'", output);
            } else {
                if (failuresCounter != null) {
                    failuresCounter.increment();
                }
                log.warn("OMS cluster snapshot trigger refused: output='{}'", output);
            }
            return triggered;
        } catch (RuntimeException ex) {
            if (failuresCounter != null) {
                failuresCounter.increment();
            }
            log.warn("OMS cluster snapshot trigger threw", ex);
            return false;
        }
    }

    public long secondsSinceLastSuccess() {
        long last = lastSuccessEpochMs.get();
        if (last == NEVER_SUCCEEDED_EPOCH_MS) {
            return NEVER_SUCCEEDED_AGE_SECONDS;
        }
        long elapsedMs = System.currentTimeMillis() - last;
        return elapsedMs < 0L ? 0L : TimeUnit.MILLISECONDS.toSeconds(elapsedMs);
    }

    public long lastSuccessEpochMsForTest() {
        return lastSuccessEpochMs.get();
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            schedulerThread.interrupt();
            try {
                schedulerThread.join(TimeUnit.SECONDS.toMillis(5L));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void loop() {
        try {
            if (initialDelayMs > 0L) {
                Thread.sleep(initialDelayMs);
            }
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                snapshotNow();
                Thread.sleep(intervalMs);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
