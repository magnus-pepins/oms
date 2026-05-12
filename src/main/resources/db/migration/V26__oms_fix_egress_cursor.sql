-- Phase 3 slice 3b-1 of system-documentation/plans/oms-aeron-cluster-substrate.md: oms-fix-egress
-- consumes the cluster events recording (OmsClusterWireFormat.EVENTS_CHANNEL / EVENTS_STREAM_ID)
-- via Aeron Archive replay, exactly the way oms-postgres-projector does (see V24). This cursor
-- table tracks how far the egress JVM has applied so a restart resumes from the same Aeron log
-- position instead of re-sending NOS for every admitted order since cluster boot.
--
-- Mirrors aeron_projector_cursor's shape and primary-key semantics: one row per
-- (egress_id, stream_id), monotonic advance on the conflict path. We pick a separate physical
-- table (rather than reusing aeron_projector_cursor with a different projector_id) so that the
-- egress role can be operationally observed and managed independently of the Postgres projector,
-- and so the egress JVM never needs to read or write a row that mentions Postgres semantics.
--
-- egress_id is the stable identifier of an egress consumer (e.g. "oms-fix-egress-default" for
-- the single-route v1 deployment; future routes use distinct ids so each gets its own cursor).
-- stream_id is the Aeron stream id the cursor tracks (today only EVENTS_STREAM_ID = 2000; kept
-- explicit in the schema so future stream additions do not require a migration).

CREATE TABLE oms_fix_egress_cursor (
    egress_id             TEXT        NOT NULL,
    stream_id             INTEGER     NOT NULL,
    last_applied_position BIGINT      NOT NULL,
    last_applied_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (egress_id, stream_id)
);

COMMENT ON TABLE oms_fix_egress_cursor IS
    'Cluster-log replay cursor for oms-fix-egress JVMs. Updated atomically with the side-effect (Session.sendToTarget) it just performed so egress restart resumes from last committed log position.';

COMMENT ON COLUMN oms_fix_egress_cursor.egress_id IS
    'Stable identifier for an egress consumer (e.g. "oms-fix-egress-default"). Allows multiple egress JVMs / routes to track independent positions on the same stream.';

COMMENT ON COLUMN oms_fix_egress_cursor.stream_id IS
    'Aeron stream id the cursor advances along (today: OmsClusterWireFormat.EVENTS_STREAM_ID).';

COMMENT ON COLUMN oms_fix_egress_cursor.last_applied_position IS
    'Aeron log position (bytes) of the most recently applied (and side-effected, e.g. NOS-sent) event. Next replay starts at this position; idempotency requires writes ≤ this position to be no-ops on retry.';
