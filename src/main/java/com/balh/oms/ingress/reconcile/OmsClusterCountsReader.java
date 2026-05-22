package com.balh.oms.ingress.reconcile;

import io.aeron.Aeron;
import io.aeron.driver.exceptions.ActiveDriverException;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Polls cluster open-order count counters on the shared media driver (Phase 3). */
public final class OmsClusterCountsReader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterCountsReader.class);

    public static final long DEFAULT_POLL_INTERVAL_MS = 5_000L;
    public static final long DEFAULT_INITIAL_DELAY_MS = 1_000L;
    public static final long DEFAULT_DRIVER_TIMEOUT_MS = 10_000L;

    private final String aeronDirectory;
    private final long pollIntervalMs;
    private final long initialDelayMs;
    private final long driverTimeoutMs;
    private final ScheduledExecutorService poller;
    private final AtomicReference<ClusterCountsSnapshot> latest;
    private volatile Aeron aeron;

    public OmsClusterCountsReader(String aeronDirectory) {
        this(aeronDirectory, DEFAULT_POLL_INTERVAL_MS, DEFAULT_INITIAL_DELAY_MS, DEFAULT_DRIVER_TIMEOUT_MS);
    }

    public OmsClusterCountsReader(
            String aeronDirectory, long pollIntervalMs, long initialDelayMs, long driverTimeoutMs) {
        if (aeronDirectory == null || aeronDirectory.isBlank()) {
            throw new IllegalArgumentException("aeronDirectory must be set");
        }
        this.aeronDirectory = aeronDirectory;
        this.pollIntervalMs = pollIntervalMs;
        this.initialDelayMs = initialDelayMs;
        this.driverTimeoutMs = driverTimeoutMs;
        this.latest = new AtomicReference<>(ClusterCountsSnapshot.allMissing(System.currentTimeMillis()));
        this.poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oms-cluster-counts-poller");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        poller.scheduleAtFixedRate(this::pollSafely, initialDelayMs, pollIntervalMs, TimeUnit.MILLISECONDS);
        log.info("OMS cluster counts reader started: aeronDir='{}'", aeronDirectory);
    }

    public ClusterCountsSnapshot snapshot() {
        return latest.get();
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

    public ClusterCountsSnapshot pollForTest() {
        pollSafely();
        return latest.get();
    }

    private void pollSafely() {
        try {
            ensureAeron();
            if (aeron == null) {
                return;
            }
            CountersReader reader = aeron.countersReader();
            EnumMap<ReconcileEntityKind, ClusterCountsSnapshot.EntityCount> observed =
                    new EnumMap<>(ReconcileEntityKind.class);
            for (ReconcileEntityKind kind : ReconcileEntityKind.values()) {
                int counterId = findCounterId(reader, kind);
                if (counterId < 0) {
                    observed.put(kind, ClusterCountsSnapshot.EntityCount.counterMissing());
                } else {
                    observed.put(kind, ClusterCountsSnapshot.EntityCount.observed(reader.getCounterValue(counterId)));
                }
            }
            latest.set(new ClusterCountsSnapshot(observed, System.currentTimeMillis()));
        } catch (RuntimeException ex) {
            log.warn("OMS cluster counts poll threw: {}", ex.getMessage());
            CloseHelper.quietClose(aeron);
            aeron = null;
        }
    }

    private void ensureAeron() {
        if (aeron != null) {
            return;
        }
        try {
            aeron = Aeron.connect(new Aeron.Context()
                    .aeronDirectoryName(aeronDirectory)
                    .driverTimeoutMs(driverTimeoutMs));
        } catch (ActiveDriverException ex) {
            log.warn("OMS cluster counts reader: driver heartbeat lost on '{}'", aeronDirectory);
            latest.set(ClusterCountsSnapshot.allMissing(System.currentTimeMillis()));
        } catch (RuntimeException ex) {
            log.warn("OMS cluster counts reader: connect failed on '{}': {}", aeronDirectory, ex.getMessage());
            latest.set(ClusterCountsSnapshot.allMissing(System.currentTimeMillis()));
        }
    }

    private static int findCounterId(CountersReader reader, ReconcileEntityKind kind) {
        final int[] found = {-1};
        reader.forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == kind.counterTypeId() && kind.counterLabel().equals(label)) {
                found[0] = counterId;
            }
        });
        return found[0];
    }
}
