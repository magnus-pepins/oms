package com.balh.oms.ingress.reconcile;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Glue between {@link OmsClusterCountsReader} and {@link OmsProjectorOrderCountsRepository}. */
public final class OmsReconciliationService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OmsReconciliationService.class);

    public static final long DEFAULT_POLL_INTERVAL_MS = 30_000L;
    public static final long DEFAULT_INITIAL_DELAY_MS = 5_000L;

    public static final String METRIC_CLUSTER_COUNT = "oms_cluster_count";
    public static final String METRIC_PROJECTOR_COUNT = "oms_projector_count";
    public static final String METRIC_DRIFT = "oms_drift";
    public static final String METRIC_IN_SYNC = "oms_cluster_reconcile_in_sync";
    public static final String METRIC_AGE_SECONDS = "oms_cluster_reconcile_age_seconds";
    public static final String TAG_KIND = "kind";

    static final long NEVER_RECONCILED_EPOCH_MS = 0L;
    static final long NEVER_RECONCILED_AGE_SECONDS = 365L * 24L * 3600L;

    private final OmsClusterCountsReader clusterReader;
    private final OmsProjectorOrderCountsRepository projectorRepoOrNull;
    private final long pollIntervalMs;
    private final long initialDelayMs;
    private final ScheduledExecutorService poller;
    private final AtomicReference<OmsReconciliationSnapshot> latest;
    private final AtomicLong lastSuccessEpochMs;
    private final Map<ReconcileEntityKind, AtomicLong> clusterCountByKind;
    private final Map<ReconcileEntityKind, AtomicLong> projectorCountByKind;
    private final Map<ReconcileEntityKind, AtomicLong> driftByKind;
    private final AtomicLong inSync;

    public OmsReconciliationService(
            OmsClusterCountsReader clusterReader, Optional<OmsProjectorOrderCountsRepository> projectorRepoOpt) {
        this(clusterReader, projectorRepoOpt, null, DEFAULT_POLL_INTERVAL_MS, DEFAULT_INITIAL_DELAY_MS);
    }

    public OmsReconciliationService(
            OmsClusterCountsReader clusterReader,
            Optional<OmsProjectorOrderCountsRepository> projectorRepoOpt,
            MeterRegistry meterRegistryOrNull,
            long pollIntervalMs,
            long initialDelayMs) {
        this.clusterReader = clusterReader;
        this.projectorRepoOrNull = projectorRepoOpt.orElse(null);
        this.pollIntervalMs = pollIntervalMs;
        this.initialDelayMs = initialDelayMs;
        this.lastSuccessEpochMs = new AtomicLong(NEVER_RECONCILED_EPOCH_MS);
        this.poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oms-reconcile-poller");
            t.setDaemon(true);
            return t;
        });
        this.clusterCountByKind = new EnumMap<>(ReconcileEntityKind.class);
        this.projectorCountByKind = new EnumMap<>(ReconcileEntityKind.class);
        this.driftByKind = new EnumMap<>(ReconcileEntityKind.class);
        for (ReconcileEntityKind kind : ReconcileEntityKind.values()) {
            clusterCountByKind.put(kind, new AtomicLong(-1L));
            projectorCountByKind.put(kind, new AtomicLong(-1L));
            driftByKind.put(kind, new AtomicLong(0L));
        }
        this.inSync = new AtomicLong(0L);
        OmsReconciliationSnapshot initial = OmsReconciliationSnapshot.compose(
                clusterReader.snapshot(),
                null,
                0L,
                projectorRepoOrNull == null
                        ? OmsReconciliationSnapshot.ProjectorStatus.NOT_CONFIGURED
                        : OmsReconciliationSnapshot.ProjectorStatus.OK,
                null);
        this.latest = new AtomicReference<>(initial);
        if (meterRegistryOrNull != null) {
            registerGauges(meterRegistryOrNull);
        }
    }

    private void registerGauges(MeterRegistry registry) {
        for (ReconcileEntityKind kind : ReconcileEntityKind.values()) {
            Gauge.builder(METRIC_CLUSTER_COUNT, clusterCountByKind.get(kind), AtomicLong::doubleValue)
                    .tag(TAG_KIND, kind.tag())
                    .register(registry);
            Gauge.builder(METRIC_PROJECTOR_COUNT, projectorCountByKind.get(kind), AtomicLong::doubleValue)
                    .tag(TAG_KIND, kind.tag())
                    .register(registry);
            Gauge.builder(METRIC_DRIFT, driftByKind.get(kind), AtomicLong::doubleValue)
                    .tag(TAG_KIND, kind.tag())
                    .register(registry);
        }
        Gauge.builder(METRIC_IN_SYNC, inSync, AtomicLong::doubleValue).register(registry);
        Gauge.builder(METRIC_AGE_SECONDS, this, OmsReconciliationService::ageSeconds).register(registry);
    }

    public void start() {
        poller.scheduleAtFixedRate(this::pollSafely, initialDelayMs, pollIntervalMs, TimeUnit.MILLISECONDS);
        log.info(
                "OMS reconciliation service started: pollIntervalMs={} projectorConfigured={}",
                pollIntervalMs,
                projectorRepoOrNull != null);
    }

    public OmsReconciliationSnapshot snapshot() {
        return latest.get();
    }

    public long ageSeconds() {
        long last = lastSuccessEpochMs.get();
        if (last == NEVER_RECONCILED_EPOCH_MS) {
            return NEVER_RECONCILED_AGE_SECONDS;
        }
        long elapsedMs = System.currentTimeMillis() - last;
        return elapsedMs < 0L ? 0L : TimeUnit.MILLISECONDS.toSeconds(elapsedMs);
    }

    public OmsReconciliationSnapshot pollForTest() {
        pollSafely();
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
    }

    private void pollSafely() {
        try {
            ClusterCountsSnapshot clusterSnapshot = clusterReader.snapshot();
            Map<ReconcileEntityKind, Long> projectorCounts = null;
            OmsReconciliationSnapshot.ProjectorStatus projectorStatus;
            String projectorError = null;
            long projectorObservedAtMillis = System.currentTimeMillis();
            if (projectorRepoOrNull == null) {
                projectorStatus = OmsReconciliationSnapshot.ProjectorStatus.NOT_CONFIGURED;
            } else {
                try {
                    projectorCounts = projectorRepoOrNull.countAll();
                    projectorStatus = OmsReconciliationSnapshot.ProjectorStatus.OK;
                } catch (DataAccessException ex) {
                    projectorStatus = OmsReconciliationSnapshot.ProjectorStatus.ERROR;
                    projectorError = ex.getClass().getSimpleName() + ": " + ex.getMessage();
                    log.warn("OMS reconciliation projector counts threw: {}", projectorError);
                }
            }
            OmsReconciliationSnapshot composed = OmsReconciliationSnapshot.compose(
                    clusterSnapshot,
                    projectorCounts,
                    projectorObservedAtMillis,
                    projectorStatus,
                    projectorError);
            latest.set(composed);
            publishGauges(composed);
            lastSuccessEpochMs.set(System.currentTimeMillis());
        } catch (RuntimeException ex) {
            log.warn("OMS reconciliation poll threw: {}", ex.getMessage(), ex);
        }
    }

    private void publishGauges(OmsReconciliationSnapshot snapshot) {
        for (ReconcileEntityKind kind : ReconcileEntityKind.values()) {
            OmsReconciliationSnapshot.EntityDrift d = snapshot.driftFor(kind);
            clusterCountByKind.get(kind).set(d.clusterCount());
            projectorCountByKind.get(kind).set(d.projectorCount());
            if (d.status() != OmsReconciliationSnapshot.DriftStatus.UNKNOWN) {
                driftByKind.get(kind).set(d.delta());
            }
        }
        inSync.set(snapshot.isAllInSync() ? 1L : 0L);
    }
}
