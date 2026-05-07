# Chronicle replay

Chronicle Queue is **engineering replay only**. It is not a regulatory
system of record. Postgres is.

## When you would replay

- Reproducing a production incident in a non-prod environment.
- Latency / throughput profiling against a real workload.
- Testing a new control-plane component against historical traffic.

## When you would NOT replay

- Anything that requires a regulator-defensible chain of custody. For
  that, the source is Postgres + the Beard Admin audit trail.
- Anything that depends on Chronicle being identical across instances.
  Chronicle is shard-local in slice 1; replicated journals arrive later
  if needed.

## How to replay (slice 1)

1. Stop the OMS.
2. Snapshot the Postgres database alongside the Chronicle queue
   directory (`oms.chronicle.queue-dir`). Both must be from the same
   point in time.
3. Restore both into the replay environment.
4. Start the OMS pointed at the restored datastore. The reconciler will
   skip rows whose `chronicle_enqueued_at` is already set.
5. To replay individual control events, write a Chronicle reader (or use
   the Chronicle Queue CLI tooling) and feed the payloads back through
   `ControlTailer.apply(...)`.

## What is in the journal

- The exact JSON payload that `OrdersController.persistAccepted` wrote
  into `control_outbox.payload`. See
  `chronicle/PendingControlEvent.java`.
- This is enough for a tailer to apply CAS updates idempotently. It is
  NOT enough on its own to reconstruct the order — for that, you need
  the Postgres row.

## Limitations of slice 1

- Single-node Chronicle. If the disk fails, you lose the journal but
  not the orders (Postgres is the SoR).
- Daily roll cycles. Adjust via `oms.chronicle.roll-cycle` if you need
  finer rotation.
- No compaction. The directory grows with traffic; ops procedure for
  archiving / pruning is part of the slice 2 deployment hardening.
