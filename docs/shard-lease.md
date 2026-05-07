# Shard lease and failover (OMS)

Slice 1 runs **single-instance**. Before running multiple OMS replicas against
the same Postgres shard, operators need a **single-writer guarantee** for:

- scheduled reconcilers (`OutboxReconciler`, `DomainFanoutReconciler`,
  `ChronicleControlTailReader`);
- any future FIX session or control worker that assumes exclusive access.

## Options (not implemented in this repo slice)

1. **Postgres advisory lock** — acquire `pg_try_advisory_lock(shard_id)` at
   process start; heartbeat in a background thread. **Caveat:** the lock is
   connection-scoped; it must be held on one dedicated JDBC connection for the
   lifetime of the process, not per-request pool connections.
2. **Kubernetes lease** (`coordination.k8s.io/v1` `Lease`) — sidecar or
   in-process leader election; good when OMS is already on k8s.
3. **External coordinator** (etcd, Consul) — heavier but explicit TTL and
   fencing tokens if split-brain must be impossible across AZs.

## Recovery story

After failover, the new primary should:

1. Acquire the shard lease.
2. Rely on **Postgres** for `orders`, `control_outbox`, and `domain_event_outbox`
   (all durable). Re-drive pending `control_outbox` and `domain_event_outbox`
   rows whose downstream handoff timestamps are null.
3. Treat **Chronicle** as shard-local engineering replay; rebuild from outbox
   if the queue directory was lost on the failed node.

See [architecture.md](architecture.md) for the slice-1 control-plane diagram.
