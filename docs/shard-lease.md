# Shard lease and failover (OMS)

Phase 3 of the [Aeron Cluster substrate plan](../../system-documentation/plans/oms-aeron-cluster-substrate.md)
makes the **cluster** itself the leader-elected, single-writer authority for
order admission and ER state. ConsensusModule (Raft) handles cluster
leadership transparently.

The remaining single-writer concerns sit outside the cluster service:

- **`oms-fix-egress`** — exactly **one** active replica per FIX route (broker
  constraint: one initiator per session). Singleton enforcement comes from the
  external orchestrator (k8s Deployment with `replicas: 1` per route, and/or
  `oms_fix_session_lock` if a Postgres-backed lock is needed).
- **`oms-postgres-projector`** — multiple replicas are safe (idempotent on
  replay) but redundant; standard practice is one active per shard.
- **`DomainFanoutReconciler` / `LedgerInflightOutboxReconciler` /
  `SettlementBrokerConfirmReconciler`** — single-writer over the relevant
  outbox table. Use `pg_try_advisory_lock(shard_id)` (connection-scoped, held
  on a dedicated JDBC connection) or a Kubernetes `Lease` to elect one
  reconciler per shard.

## Options for outbox-reconciler leader election

1. **Postgres advisory lock** — acquire `pg_try_advisory_lock(shard_id)` at
   process start; heartbeat in a background thread. The lock is
   connection-scoped; it must be held on one dedicated JDBC connection for the
   lifetime of the process, not per-request pool connections.

   **Pooler interaction (slice 4o)** — once the OMS DataSource defaults to
   Supavisor's transaction-mode pooler (port `6543`), advisory-lock state is
   incompatible because the pooler may route each transaction to a different
   backend, dropping the lock between calls. If this option is ever
   implemented, register a **second `@Configuration` DataSource** pointed at
   Supavisor's session-mode listener (`5432`) or directly at Postgres, and
   inject it specifically into the leader-election bean. Verified today: zero
   callers of `pg_advisory_lock` / `pg_try_advisory_lock` in `src/main`, so the
   blanket port-6543 default is safe.
2. **Kubernetes Lease** (`coordination.k8s.io/v1` `Lease`) — sidecar or
   in-process leader election; good when OMS is already on k8s.
3. **External coordinator** (etcd, Consul) — heavier but explicit TTL and
   fencing tokens if split-brain must be impossible across AZs.

## Recovery story

After cluster leader failover, the new leader resumes from the last cluster
snapshot + replay of the events recording; in-memory dedupe sets
(`(orderId, venueExecRef)`, `(senderCompId, msgSeqNum)`) are restored from
snapshot v3.

For projector / FIX-egress / outbox-reconciler failover, the new instance
resumes from its persisted Postgres cursor (`aeron_projector_cursor`,
`oms_fix_egress_cursor`, `domain_event_outbox.published_at`) and replays
forward.

See [architecture.md](architecture.md) for the cluster + projection diagram.
