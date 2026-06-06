package com.balh.oms.cluster.disk;

import io.aeron.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Polls the Aeron data directory mount and publishes pressure on the shared media-driver counter.
 * Invokes {@code onCriticalShutdown} once when CRITICAL is observed.
 */
public final class AeronDiskPressureMonitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AeronDiskPressureMonitor.class);

    private final AeronDiskPressurePolicy policy;
    private final Counter diskPressureCounter;
    private final Runnable onCriticalShutdown;
    private final MeterRegistry meterRegistry;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean criticalShutdownSignalled = new AtomicBoolean(false);
    private final AtomicReference<AeronDiskPressureLevel> latestLevel =
            new AtomicReference<>(AeronDiskPressureLevel.OK);
    private final AtomicLong latestUsableBytes = new AtomicLong(-1L);
    private final AtomicLong latestTotalBytes = new AtomicLong(-1L);

    public AeronDiskPressureMonitor(
            AeronDiskPressurePolicy policy,
            Counter diskPressureCounter,
            Runnable onCriticalShutdown,
            MeterRegistry meterRegistry) {
        this.policy = policy;
        this.diskPressureCounter = diskPressureCounter;
        this.onCriticalShutdown = onCriticalShutdown == null ? () -> {} : onCriticalShutdown;
        this.meterRegistry = meterRegistry;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "aeron-disk-pressure-monitor");
            t.setDaemon(true);
            return t;
        });
        if (meterRegistry != null) {
            Gauge.builder("aeron.disk.bytes_free", latestUsableBytes, AtomicLong::get)
                    .description("Usable bytes on the Aeron data directory mount")
                    .register(meterRegistry);
            Gauge.builder("aeron.disk.free_ratio", this, m -> {
                        long total = latestTotalBytes.get();
                        long usable = latestUsableBytes.get();
                        if (total <= 0L || usable < 0L) {
                            return 0.0d;
                        }
                        return (double) usable / (double) total;
                    })
                    .description("Free-space ratio on the Aeron data directory mount")
                    .register(meterRegistry);
            Gauge.builder("aeron.disk.pressure_level", latestLevel, ref -> ref.get().ordinal())
                    .description("Disk pressure level ordinal (0=OK .. 3=CRITICAL)")
                    .register(meterRegistry);
        }
    }

    public void start() {
        if (!policy.enabled()) {
            log.info("Aeron disk pressure monitor disabled via {}", AeronDiskPressurePolicy.ENV_ENABLED);
            return;
        }
        scheduler.scheduleAtFixedRate(
                this::pollSafely,
                0L,
                policy.pollIntervalMs(),
                TimeUnit.MILLISECONDS);
        log.info(
                "Aeron disk pressure monitor started: dataDir='{}' pollIntervalMs={}",
                policy.dataDir(),
                policy.pollIntervalMs());
    }

    public AeronDiskPressureLevel latestLevel() {
        return latestLevel.get();
    }

    public long latestUsableBytes() {
        return latestUsableBytes.get();
    }

    void pollForTest() {
        pollSafely();
    }

    private void pollSafely() {
        try {
            AeronDiskSpaceSample sample = AeronDiskSpaceSampler.sample(policy.dataDir());
            latestUsableBytes.set(sample.usableBytes());
            latestTotalBytes.set(sample.totalBytes());
            AeronDiskPressureLevel level = policy.evaluate(sample);
            latestLevel.set(level);
            if (diskPressureCounter != null) {
                diskPressureCounter.setOrdered(level.counterValue());
            }
            if (level == AeronDiskPressureLevel.WARN) {
                log.warn(
                        "Aeron data dir disk pressure WARN: dir='{}' usableBytes={} freeRatio={}",
                        policy.dataDir(),
                        sample.usableBytes(),
                        sample.freeRatio());
            } else if (level == AeronDiskPressureLevel.REJECT) {
                log.error(
                        "Aeron data dir disk pressure REJECT — ingress writes will be refused: dir='{}' usableBytes={} freeRatio={}",
                        policy.dataDir(),
                        sample.usableBytes(),
                        sample.freeRatio());
            } else if (level == AeronDiskPressureLevel.CRITICAL) {
                log.error(
                        "Aeron data dir disk pressure CRITICAL: dir='{}' usableBytes={} freeRatio={}",
                        policy.dataDir(),
                        sample.usableBytes(),
                        sample.freeRatio());
                if (criticalShutdownSignalled.compareAndSet(false, true)) {
                    log.error("signalling cluster-node graceful shutdown due to disk pressure CRITICAL");
                    onCriticalShutdown.run();
                }
            }
        } catch (IOException ex) {
            log.warn("Aeron disk pressure poll failed for dir='{}': {}", policy.dataDir(), ex.getMessage());
        } catch (RuntimeException ex) {
            log.warn("Aeron disk pressure poll threw: {}", ex.getMessage());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2L, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
