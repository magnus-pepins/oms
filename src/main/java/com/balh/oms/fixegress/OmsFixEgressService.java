package com.balh.oms.fixegress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Phase 3 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: the FIX-egress role.
 *
 * <p><strong>Slice 3a scope.</strong> Skeleton only. Loads under
 * {@value OmsProfiles#FIX_EGRESS} when {@code oms.cluster.fix-egress.enabled=true}, logs the role
 * coming up, and exits cleanly on shutdown. No cluster consumption, no QuickFIX wiring yet — both
 * land in slice 3b (Aeron Archive replay → {@code Session.sendToTarget(NewOrderSingle)}, mirroring
 * {@link com.balh.oms.projector.OmsPostgresProjector}'s slice 2b-2 / 2d work on the FIX side).
 *
 * <p>The bean exists in slice 3a so that:
 *
 * <ol>
 *   <li>The new Spring profile is reachable end-to-end ({@code bootRunFixEgress} / {@code bootJarFixEgress}
 *       + {@link com.balh.oms.OmsFixEgressBootstrap}).</li>
 *   <li>The topology validator ({@link com.balh.oms.config.FixEgressTopologyValidator}) has a real
 *       Spring context to probe; tests can assert the expected bean topology under
 *       {@code oms-fix-egress} without standing up Aeron.</li>
 *   <li>Slice 3b lands the replay loop on top of an already-deployable bootstrap, so the only
 *       behavior change at slice-3b roll-out is "the JVM now sends NewOrderSingle".</li>
 * </ol>
 *
 * <p>The skeleton does not compete with the legacy {@code oms-fix-worker} JVM: profiles are
 * mutex via {@link com.balh.oms.config.TopologyWorkerProfiles}. Production runs one of the two
 * (not both) per FIX route during the Phase 3 transition; once slice 3g lands, only this role
 * remains.
 */
@Component
@Profile(OmsProfiles.FIX_EGRESS)
@ConditionalOnProperty(prefix = "oms.cluster.fix-egress", name = "enabled", havingValue = "true")
public class OmsFixEgressService {

    private static final Logger log = LoggerFactory.getLogger(OmsFixEgressService.class);

    @SuppressWarnings("unused")
    private final OmsConfig config;

    public OmsFixEgressService(OmsConfig config) {
        this.config = config;
    }

    @PostConstruct
    void init() {
        // Slice 3a logs only. Slice 3b will start the Aeron + AeronArchive + replay-thread
        // here, mirroring OmsPostgresProjector.init().
        log.info(
                "oms-fix-egress skeleton starting (slice 3a — no cluster consumption yet; slice 3b adds Aeron Archive replay → NewOrderSingle)");
    }

    @PreDestroy
    void close() {
        log.info("oms-fix-egress skeleton stopping");
    }
}
