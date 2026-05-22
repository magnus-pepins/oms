package com.balh.oms.ingress.readiness;

import com.balh.oms.cluster.OmsAdmissionClusteredService;
import io.aeron.Aeron;
import io.aeron.driver.exceptions.ActiveDriverException;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Polls {@link OmsAdmissionClusteredService#READINESS_COUNTER_LABEL} on the shared media driver. */
public final class OmsClusterReadinessReader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterReadinessReader.class);

    public static final long DEFAULT_POLL_INTERVAL_MS = 500L;
    public static final long DEFAULT_INITIAL_DELAY_MS = 100L;
    public static final long DEFAULT_DRIVER_TIMEOUT_MS = 10_000L;

    private final String aeronDirectory;
    private final long pollIntervalMs;
    private final long initialDelayMs;
    private final long driverTimeoutMs;
    private final ScheduledExecutorService poller;
    private final AtomicReference<ReadinessSnapshot> latest;
    private volatile Aeron aeron;

    public OmsClusterReadinessReader(String aeronDirectory) {
        this(aeronDirectory, DEFAULT_POLL_INTERVAL_MS, DEFAULT_INITIAL_DELAY_MS, DEFAULT_DRIVER_TIMEOUT_MS);
    }

    public OmsClusterReadinessReader(
            String aeronDirectory, long pollIntervalMs, long initialDelayMs, long driverTimeoutMs) {
        if (aeronDirectory == null || aeronDirectory.isBlank()) {
            throw new IllegalArgumentException("aeronDirectory must be set");
        }
        if (pollIntervalMs <= 0L || initialDelayMs < 0L || driverTimeoutMs <= 0L) {
            throw new IllegalArgumentException("invalid poll/driver timing");
        }
        this.aeronDirectory = aeronDirectory;
        this.pollIntervalMs = pollIntervalMs;
        this.initialDelayMs = initialDelayMs;
        this.driverTimeoutMs = driverTimeoutMs;
        this.latest = new AtomicReference<>(ReadinessSnapshot.unknown(System.currentTimeMillis()));
        this.poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oms-cluster-readiness-poller");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        poller.scheduleAtFixedRate(this::pollSafely, initialDelayMs, pollIntervalMs, TimeUnit.MILLISECONDS);
        log.info(
                "OMS cluster readiness reader started: aeronDir='{}' pollIntervalMs={}",
                aeronDirectory,
                pollIntervalMs);
    }

    public ReadinessSnapshot snapshot() {
        return latest.get();
    }

    public boolean isReady() {
        return snapshot().isReady();
    }

    @Override
    public void close() {
        poller.shutdownNow();
        try {
            poller.awaitTermination(2L, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        CloseHelper.quietClose(aeron);
        aeron = null;
    }

    public ReadinessSnapshot pollForTest() {
        pollSafely();
        return latest.get();
    }

    private void pollSafely() {
        try {
            ensureAeronOrUpdateSnapshot();
            if (aeron == null) {
                return;
            }
            CountersReader reader = aeron.countersReader();
            int counterId = findReadinessCounterId(reader);
            if (counterId < 0) {
                latest.set(ReadinessSnapshot.counterMissing(System.currentTimeMillis()));
                return;
            }
            long value = reader.getCounterValue(counterId);
            latest.set(ReadinessSnapshot.observed(
                    value,
                    counterId,
                    System.currentTimeMillis(),
                    OmsAdmissionClusteredService.READINESS_VALUE_READY));
        } catch (RuntimeException ex) {
            log.warn("OMS cluster readiness poll threw: {}", ex.getMessage());
            CloseHelper.quietClose(aeron);
            aeron = null;
        }
    }

    private void ensureAeronOrUpdateSnapshot() {
        if (aeron != null) {
            return;
        }
        try {
            aeron = Aeron.connect(new Aeron.Context()
                    .aeronDirectoryName(aeronDirectory)
                    .driverTimeoutMs(driverTimeoutMs));
        } catch (ActiveDriverException ex) {
            log.warn("OMS readiness reader: driver heartbeat lost on '{}'", aeronDirectory);
            latest.set(ReadinessSnapshot.counterMissing(System.currentTimeMillis()));
        } catch (RuntimeException ex) {
            log.warn("OMS readiness reader: connect failed on '{}': {}", aeronDirectory, ex.getMessage());
            latest.set(ReadinessSnapshot.counterMissing(System.currentTimeMillis()));
        }
    }

    private static int findReadinessCounterId(CountersReader reader) {
        final int[] found = {-1};
        reader.forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == OmsAdmissionClusteredService.READINESS_COUNTER_TYPE_ID
                    && OmsAdmissionClusteredService.READINESS_COUNTER_LABEL.equals(label)) {
                found[0] = counterId;
            }
        });
        return found[0];
    }
}
