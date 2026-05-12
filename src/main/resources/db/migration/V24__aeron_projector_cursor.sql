-- Phase 2 of system-documentation/plans/oms-aeron-cluster-substrate.md: Postgres becomes a downstream projection of
-- the cluster log. The projector tracks how far it has applied via this cursor table so a restart resumes from the
-- same Aeron log position instead of re-applying every event since cluster boot.
--
-- One row per (projector_id, stream_id). projector_id distinguishes projectors that consume different views of the
-- same cluster log (e.g. orders projector vs executions projector if we split later). stream_id identifies the Aeron
-- stream within the cluster log; today there is only one (the consensus log) but Aeron Archive recordings are keyed by
-- stream so we make this dimension explicit now.

CREATE TABLE aeron_projector_cursor (
    projector_id          TEXT        NOT NULL,
    stream_id             INTEGER     NOT NULL,
    last_applied_position BIGINT      NOT NULL,
    last_applied_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (projector_id, stream_id)
);

COMMENT ON TABLE aeron_projector_cursor IS
    'Cluster-log replay cursor for Aeron projector JVMs (oms-postgres-projector). Updated atomically with the row(s) it produces so projector restart resumes from last committed log position.';

COMMENT ON COLUMN aeron_projector_cursor.projector_id IS
    'Stable identifier for a projector consumer (e.g. "oms-postgres-orders"). Allows multiple projectors to track independent positions on the same stream.';

COMMENT ON COLUMN aeron_projector_cursor.stream_id IS
    'Aeron stream id (Archive recording stream) the cursor advances along.';

COMMENT ON COLUMN aeron_projector_cursor.last_applied_position IS
    'Aeron log position (bytes) of the most recently applied event. Next replay starts at this position; idempotency requires writes ≤ this position to be no-ops on retry.';
