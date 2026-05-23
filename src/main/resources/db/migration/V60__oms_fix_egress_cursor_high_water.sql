-- 2026-05-23 — gap 2 of the post-V55/V56 stability review (see
-- system-documentation/handovers/2026-05-23-oms-snapshot-magic-mismatch-and-stability-rework.md §10).
--
-- The V56 hardening made the egress cursor recording-aware and added a lexicographic monotonic
-- guard inside OmsFixEgressCursorRepository.advanceWithRecording — but only for advances driven
-- through the repository API. An operator UPDATE (or a buggy future caller) that writes a
-- (recording_id, position) smaller than the saved one would bypass that guard, and on next
-- restart the egress would dutifully replay from the rewound point and re-ship admit events as
-- duplicate NOS to the broker (option-1 dedupe relies on the broker honouring DupClOrdID; not
-- every venue does, and even on those that do, this is operationally noisy).
--
-- This migration adds a high-water-mark column pair that tracks the greatest
-- (recording_id, position) the egress has ever applied. advanceWithRecording bumps both
-- columns in lockstep; resetWithRecording leaves the high-water alone (so a rewind via
-- resetWithRecording does NOT erase the breadcrumb). On startup, OmsFixEgressService.init()
-- compares (last_applied_*, ) against (high_water_*, ) lexicographically and refuses to start
-- when the cursor has been rewound, naming the high-water value and the operator-explicit
-- SQL needed to acknowledge the rewind.
--
-- The projector intentionally does NOT get this guard: projector writes are idempotent (orders
-- ON CONFLICT, executions UNIQUE on (account_id, venue_exec_ref)), so a projector rewind is
-- always safe to replay and recovery is a routine operator move. The egress has actual
-- side effects (NOS to broker) and a rewind without operator awareness is the failure mode
-- the guard is preventing.

ALTER TABLE oms_fix_egress_cursor
    ADD COLUMN high_water_recording_id BIGINT,
    ADD COLUMN high_water_position     BIGINT;

COMMENT ON COLUMN oms_fix_egress_cursor.high_water_recording_id IS
    'Greatest Aeron Archive recording id this egress has ever advanced through. Bumped in lockstep with last_applied_recording_id by advanceWithRecording (always equals or exceeds last_applied_recording_id once both are non-NULL). resetWithRecording does NOT touch this column, so an operator rewind via resetWithRecording leaves the high-water mark intact — OmsFixEgressService.init() then refuses to start until the operator explicitly zeros the high-water mark too (acknowledging the rewind). NULL on rows written by pre-V60 code; init() treats NULL high-water as "first-ever V60 boot" and seeds it from last_applied on startup. See V56 (recording-id column) and handover §9.6 / §10.';

COMMENT ON COLUMN oms_fix_egress_cursor.high_water_position IS
    'Position component of the high-water mark; see high_water_recording_id. Compared with last_applied_position lexicographically (within the same recording id). NULL only when high_water_recording_id is also NULL (pre-V60).';
