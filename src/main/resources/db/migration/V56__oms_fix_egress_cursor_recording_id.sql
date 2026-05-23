-- 2026-05-23 — fix for the OMS FIX-egress silently desyncing on cluster restart.
--
-- Same root cause as V55's projector fix (see
-- system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md
-- §9 and §9.6): the V26 oms_fix_egress_cursor stored only
-- (egress_id, stream_id, last_applied_position). Aeron Archive creates a new recording id every
-- time the cluster process restarts (each cluster lifetime owns one recording on the events
-- stream). Position N in recording 13 is a different byte than position N in recording 16 — the
-- two recordings are independent byte streams. The pre-V56 cursor was therefore ambiguous across
-- restarts: a saved position of 42464 means nothing unless you also know which recording wrote it.
--
-- The pre-V56 oms-fix-egress code mirrors the projector's pre-V55 bug bit-for-bit: when
-- saved_position > current_recording.upperBound, clampToRecording returned 0 and openReplay
-- persisted 0 back via cursorRepository.reset — destroying the breadcrumb to the previous
-- recording. The egress' impact is harder to spot than the projector's (a loopback bench broker
-- idempotently re-accepts duplicate NOS messages, and NOS re-send on every replay also hides the
-- "events missing" symptom on a real broker), but the bug is identical and lands on the next
-- non-loopback venue cutover.
--
-- This migration adds the recording-id qualifier to make the cursor unambiguous going forward.
-- Legacy rows (this column NULL) are honoured by the egress' loud-fail path: it refuses to start
-- until an operator sets the recording id explicitly. Same operator UX as the projector's V55.

ALTER TABLE oms_fix_egress_cursor
    ADD COLUMN last_applied_recording_id BIGINT;

COMMENT ON COLUMN oms_fix_egress_cursor.last_applied_recording_id IS
    'Aeron Archive recording id this last_applied_position is within. Each cluster process lifetime owns one recording on the events stream; recording ids are monotonically assigned by Aeron Archive across cluster restarts. NULL on rows written by pre-2026-05-23-V56 egress code (the V26 schema did not track this). The egress treats NULL as a poison value: it refuses to start replay until an operator pins the recording id explicitly via SQL. This blocks the silent-fallback bug where the egress would otherwise reset position to 0 of the current recording and lose the pointer to admit events in earlier recordings (which would manifest on a non-DupClOrdID-tolerant venue as orders that never get sent to the broker after a cluster restart). See OmsFixEgressService#init and the V55 migration on aeron_projector_cursor for the sibling fix on the projector side.';
