# OMS slice-1 architecture

## Cash / securities boundary

Slice 1 owns the **securities** side: orders, executions, positions. The cash
side stays in [Ledger](../../ledger). The OMS calls Ledger for inflight /
settle / commit; Ledger remains the system of record for money movement.

## Control-plane flow (slice 1)

```mermaid
sequenceDiagram
    autonumber
    participant Client as Customer / BFF / FIX-IN
    participant API as OMS HTTP\n(/internal/v1/orders)
    participant PG as Postgres\n(orders + control_outbox)
    participant REC as OutboxReconciler\n(scheduled)
    participant CHR as Chronicle Queue\n(engineering replay)
    participant TLR as ControlTailer
    participant BUS as DomainEventPublisher\n(NATS in 1.5+)

    Client->>API: POST CreateOrderRequest
    API->>PG: BEGIN
    API->>PG: INSERT INTO orders (...)
    API->>PG: INSERT INTO control_outbox (order_id, version, payload)
    API->>PG: COMMIT
    API-->>Client: 201 Created
    API->>BUS: publish OrderAccepted\n(after commit, fire-and-forget)
    REC->>PG: SELECT ... WHERE chronicle_enqueued_at IS NULL
    REC->>CHR: append(payload)
    REC->>PG: UPDATE control_outbox SET chronicle_enqueued_at = NOW()
    Note over CHR,TLR: Slice 1.5: tailer reads CHR, applies CAS to orders.version
    TLR->>PG: UPDATE orders SET status=WORKING WHERE id=? AND version=?
```

The four invariants encoded by this diagram:

1. **Postgres COMMIT happens before any Chronicle append.** Always.
2. **The outbox row is inside the same transaction** as the orders row, so
   crash recovery is trivial: anything visible in `orders` has a matching
   outbox row (or a `chronicle_enqueued_at` timestamp).
3. **Domain events on NATS / drop copy are published only after commit.**
   Slice 1 ships a no-op publisher; slice 1.5 swaps in NATS.
4. **Tailer mutations are CAS on `orders.version`.** Re-applying the same
   payload is a no-op.

## High availability

Slice 1 runs single-instance. HA arrives in slice 1.5:

- Shard ownership via Postgres advisory lock OR k8s lease (decision pinned
  in slice 1.5 ADR).
- On primary failure, the standby:
  - Acquires the shard lease.
  - Reads `orders` and `control_outbox` to reconstruct in-flight state.
  - Picks up `control_outbox` rows where `chronicle_enqueued_at IS NULL`.
- Chronicle remains shard-local. Replicated journals (Chronicle Enterprise,
  Aeron Cluster, Kafka) are an upgrade path; Postgres-driven recovery is
  the slice-1.5 default.

## What about Chronicle losing data?

Because the outbox is the source of truth for "needs to be appended to
Chronicle", losing a Chronicle file is recoverable:

1. Restart the OMS pointed at a fresh queue directory.
2. The reconciler re-discovers all unsent rows in `control_outbox` and
   replays them.
3. Engineers replaying the journal are aware they must use a Postgres
   snapshot at the same time horizon — Chronicle alone is not enough.

This is what we mean by "Chronicle is engineering replay only, not a
regulatory system of record."

## What about NATS losing events?

The `DomainEventPublisher` is a fire-and-forget hook AFTER Postgres commit.
If NATS is unreachable, downstream consumers (desk UI, drop copy) miss the
event but the order itself is fully committed. Slice 1.5 adds an
optional re-emit-from-outbox path to harden this for the external drop
copy contract.
