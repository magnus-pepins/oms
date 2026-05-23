-- 2026-05-23 — fix for the OMS projector silently desyncing on cluster restart.
--
-- Context (full narrative: system-documentation/handovers/2026-05-23-... §9):
-- The original V24 cursor stored only `(projector_id, stream_id, last_applied_position)`. Aeron
-- Archive creates a *new recording id* every time the cluster process restarts (each cluster
-- lifetime owns one recording on the events stream). Position N in recording 13 is a different
-- byte than position N in recording 16 — the two recordings are independent byte streams. The
-- old cursor was therefore ambiguous across restarts: a saved position of 42464 means nothing
-- unless you also know which recording wrote it.
--
-- The pop incident: cluster restarted today, Aeron Archive opened recording 16 starting from 0,
-- projector saw `saved_position (42464) > current_recording_upperBound (0)`, fell back to "start
-- of recording 16 = 0", wrote 0 back to this table — destroying the breadcrumb to recording 13.
-- All cluster events from before today's restart are now invisible to the projector. The orders
-- exist on disk in recording 13; the projector just lost the pointer to them.
--
-- This migration adds the recording-id qualifier to make the cursor unambiguous going forward.
-- Legacy rows (this column NULL) are honored by the projector's loud-fail path: it refuses to
-- start until an operator sets the recording id explicitly, which prevents the silent-fallback
-- bug from recurring on the next cluster restart.
--
-- The advance / monotonic-guard logic is implemented in AeronProjectorCursorRepository, not in
-- a SQL constraint. The guard's invariant is lexicographic order on (recording_id, position):
-- (R, P) -> (R, P')   allowed iff P' > P    (continuing within the same recording)
-- (R, P) -> (R', 0)   allowed iff R' > R    (rolling forward to a newer recording)
-- (R, P) -> (R', P')  rejected for R' < R   (Aeron Archive recording ids never decrease)
-- The repository tests cover all three cases.

ALTER TABLE aeron_projector_cursor
    ADD COLUMN last_applied_recording_id BIGINT;

COMMENT ON COLUMN aeron_projector_cursor.last_applied_recording_id IS
    'Aeron Archive recording id this last_applied_position is within. Each cluster process lifetime owns one recording on the events stream; the recording id is monotonically assigned by Aeron Archive across cluster restarts. NULL on rows written by pre-2026-05-23 projector code (the V24 schema did not track this). The projector treats NULL as a poison value: it refuses to start replay until an operator sets the recording id explicitly via SQL or the future ops command. This blocks the silent-fallback bug where the projector would otherwise reset position to 0 of the current recording and lose its pointer to events in earlier recordings. See OmsPostgresProjector#init and system-documentation/handovers/2026-05-23 §9.';
