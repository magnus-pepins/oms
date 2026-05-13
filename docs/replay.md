# Replay

OMS state is replayed from the **Aeron Archive** events recording. Postgres is
a downstream projection; it can be rebuilt deterministically from the
recording.

## When you would replay

- Reproducing a production incident in a non-prod environment.
- Latency / throughput profiling against a real workload.
- Rebuilding a Postgres projection from scratch (operator-driven, e.g. to
  recover from a corrupt projection or to backfill after a schema change).

## When you would NOT replay

- Anything that requires a regulator-defensible chain of custody. For
  that, the source is Postgres + the Beard Admin audit trail (the projector
  writes both `domain_event_outbox` and `control_decisions` deterministically
  from the cluster log, but the regulatory record stays Postgres).

## How to replay (per-role JVM)

### Postgres projector (`oms-postgres-projector`)

The projector advances `aeron_projector_cursor` only after each projection
transaction commits. Restart resumes from the last committed cursor.

To rebuild from scratch:

1. Stop all `oms-postgres-projector` instances.
2. Truncate `aeron_projector_cursor` (or set the cursor to 0) — and truncate
   the projection tables you intend to rebuild.
3. Start the projector. It re-reads the events recording from the beginning
   and re-applies every event idempotently (`ON CONFLICT DO NOTHING` /
   version-mismatch CAS).

### FIX egress (`oms-fix-egress`)

The egress JVM advances `oms_fix_egress_cursor` only after each successful
`Session.sendToTarget`. Restart resumes from the last committed cursor.
Replays must NOT replay already-sent NOS (broker would receive a duplicate);
the cursor is the dedupe.

## What is in the recording

- The cluster events emitted by `OmsAdmissionClusteredService` —
  `OrderAdmittedEvent`, `ExecutionAppliedEvent`, etc. (see `OmsClusterWireFormat`
  and the per-event codecs).
- This is enough to deterministically reconstruct every projection row OMS
  writes (`orders`, `executions`, `control_decisions`, `domain_event_outbox`,
  `market_context`, `positions`, `position_history`).

## Limitations

- The events recording lives on the cluster nodes' Aeron Archive. Standard
  Aeron Archive operational practice (segment archival, retention,
  `truncate-recording`) applies.
- Snapshots are taken by the cluster periodically; restoring a snapshot
  alongside the recording is the fastest path to rebuilding a cluster member
  from the journal.
