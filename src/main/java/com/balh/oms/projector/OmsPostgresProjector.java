package com.balh.oms.projector;

import com.balh.oms.config.OmsProfiles;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.OptionalLong;

/**
 * Phase 2 of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: subscribes to the cluster log and
 * writes the Postgres projection rows ({@code orders}, {@code executions}, {@code control_decisions}, ...).
 *
 * <p><strong>Scope of slice 2a (this commit).</strong> Skeleton only — the bean activates the
 * {@value OmsProfiles#POSTGRES_PROJECTOR} role, reads its persisted cursor at boot, and logs the resume position.
 * It does <strong>not</strong> consume cluster events yet; that lands in slice 2b along with the consumption
 * channel design (auxiliary publication vs Aeron Archive replay — pending decision in the plan).
 *
 * <p>OrderIngressService still writes {@code orders} inline in this slice; the dual-write removal lands in slice
 * 2c after the projector is verified to track cluster state correctly.
 *
 * <h2>Cursor semantics</h2>
 *
 * The cursor is keyed by {@code (projector_id, stream_id)}. {@code projector_id} is hard-coded for now ({@link
 * #PROJECTOR_ID}) because the Postgres projection is one logical consumer; if we ever split it into per-table
 * projectors (e.g. orders vs executions) each gets its own id and its own row in {@code aeron_projector_cursor}.
 * {@code stream_id} is the Aeron Archive recording stream id of the cluster log — taken from the cluster
 * configuration at runtime (not yet wired in slice 2a; placeholder constant {@link #STREAM_ID_PLACEHOLDER}).
 */
@Component
@Profile(OmsProfiles.POSTGRES_PROJECTOR)
public class OmsPostgresProjector {

    private static final Logger log = LoggerFactory.getLogger(OmsPostgresProjector.class);

    /**
     * Stable id for the single Postgres projector consumer. Slice 2b may add a second {@code projector_id}
     * if we need separate orders / executions cursors; today both go through this one.
     */
    public static final String PROJECTOR_ID = "oms-postgres-default";

    /**
     * Placeholder stream id until slice 2b wires the real Aeron Archive recording id from the cluster
     * configuration. Stored here as a constant rather than a magic literal so the slice-2b diff is
     * grep-able. See {@code .cursor/rules/config-and-limits.mdc}.
     */
    static final int STREAM_ID_PLACEHOLDER = 0;

    private final AeronProjectorCursorRepository cursorRepository;

    public OmsPostgresProjector(AeronProjectorCursorRepository cursorRepository) {
        this.cursorRepository = cursorRepository;
    }

    @PostConstruct
    void init() {
        OptionalLong resumePos = cursorRepository.findLastAppliedPosition(PROJECTOR_ID, STREAM_ID_PLACEHOLDER);
        if (resumePos.isPresent()) {
            log.info("oms-postgres-projector starting; resuming from log position {}", resumePos.getAsLong());
        } else {
            log.info("oms-postgres-projector starting; no prior cursor — first replay");
        }
        // Slice 2b: open subscription / archive replay from resumePos and start applying events.
    }
}
