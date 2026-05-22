package com.balh.oms.cluster.snapshot;

import com.balh.oms.cluster.OmsAdmissionClusteredService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Phase 2.2 — only snapshot on shutdown when admission readiness is READY. */
public final class OmsClusterSnapshotOnShutdown {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterSnapshotOnShutdown.class);

    private OmsClusterSnapshotOnShutdown() {}

    public static boolean takeIfReady(
            OmsAdmissionClusteredService service,
            OmsClusterSnapshotScheduler scheduler,
            boolean disabled) {
        if (disabled) {
            log.info("on-shutdown OMS snapshot disabled via env; skipping");
            return false;
        }
        if (scheduler == null) {
            log.info("on-shutdown OMS snapshot skipped: no scheduler");
            return false;
        }
        if (service == null || !service.isReadyForClusterAdmission()) {
            log.warn(
                    "on-shutdown OMS snapshot skipped: readiness is not READY (ready={})",
                    service != null && service.isReadyForClusterAdmission());
            return false;
        }
        log.info("requesting on-shutdown OMS cluster snapshot (READY)");
        boolean ok = scheduler.snapshotNow();
        log.info("on-shutdown OMS snapshot trigger returned ok={}", ok);
        return ok;
    }
}
