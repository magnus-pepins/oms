# OMS slice-1 locked decisions

These decisions are pinned and only change via an explicit ADR (next file in
this directory once any of them is revisited). They mirror the milestone plan
at [.cursor/plans/oms_first_milestone_701a63c0.plan.md](../../.cursor/plans/oms_first_milestone_701a63c0.plan.md)
and v1.15 of [plans/oms-fix-gateway-and-settlement.md](../../system-documentation/plans/oms-fix-gateway-and-settlement.md).

## 1. Posture and runtime

- Posture **A** — Java 21 + Spring Boot 3 minimal + Chronicle Queue OSS +
  LMAX Disruptor + Postgres + Flyway.
- Posture B (TypeScript / Node / Fastify / BullMQ) is rejected for the
  trading core. Where TypeScript appears in the stack (BFF, customer
  frontend) it stays on the customer-facing side of the OMS edge.

## 2. Idempotency

- Uniqueness key: `UNIQUE (account_id, client_idempotency_key)` on the
  `orders` table.
- Duplicate `POST /internal/v1/orders` returns `200 OK` with the existing
  order, never `409`.

## 3. Event sequencing

- `event_seq` for downstream consumers = `orders.version` (per-row
  monotonic int).
- Mutations apply via CAS in `OrdersRepository.updateWithCas`. A failed CAS
  is a no-op, never an exception.

## 4. Sharding

- Shard key: `account_id`.
- Hash function: `xxh64` (via `net.openhft:zero-allocation-hashing`).
- Initial shard count: **1**. The mapping function exists from day one
  (`ShardKey.shardFor`) so call sites do not change when shards grow.

## 5. Control-plane ordering — mandatory invariant

- Postgres `COMMIT` happens **before** any Chronicle append. Always.
- The mechanism is a `control_outbox` row inserted in the same transaction
  as the `orders` mutation. The `OutboxReconciler` drains the outbox into
  Chronicle.
- Restated: there is no path in this codebase that writes to Chronicle
  before Postgres has committed. If you find one, it is a bug.

## 6. HTTP framework

- Spring Boot 3 minimal (Web, Actuator, JDBC, Validation).
- Javalin and Undertow were considered and rejected on grounds of test
  ergonomics (Spring's `TestRestTemplate` and `@DynamicPropertySource`
  Testcontainers integration).

## 7. Time policy

- Business timestamps: `java.time.Instant`. Stored in Postgres as
  `TIMESTAMPTZ`.
- Measurement only: `System.nanoTime()`. Never persisted, never sent on
  the wire, never tagged onto a metric.

## 8. Chronicle's role

- **Engineering replay only.** Not a regulatory system of record. Postgres
  is the SoR for orders. Chronicle exists to let engineers replay traffic
  for performance work and incident reproduction.
- Slice 1 runs Chronicle Queue OSS (single-node). Replicated journals
  (Chronicle Enterprise, Aeron Cluster, Kafka, Redpanda) are an upgrade
  path, not a slice-1 requirement.

## 9. Reject taxonomy

- `RejectCode` is a Java enum from day one, mirrored to a Postgres
  `reject_code` enum via Flyway.
- Slice 1 ships at least `RISK_STALE_QUEUE` and `RISK_DUPLICATE`. The full
  catalogue (plan §5.11) is added incrementally as risk checks land.

## 10. PII policy

- Default everywhere: hashed `account_id` only.
- Raw `account_id` may appear in audit-grade trace logs only behind
  `oms.pii.audit-trace-enabled`, which is `false` in dev and prod by
  default.
- `account_id` MUST NEVER appear as a Micrometer label. A `MeterFilter` in
  `MetricsConfig` strips offenders at runtime; `MetricsLabelsPiiTest`
  catches them in CI.

## 11. NATS / fanout bus

- NATS / JetStream is the **fanout bus** (desk live feed, drop copy, ops
  dashboards). It is **not** the authoritative trading command log.
- Domain fanout uses a **Postgres transactional outbox** (`domain_event_outbox`)
  plus `DomainFanoutReconciler`; `FanoutClient` is NATS-backed when
  `OMS_NATS_ENABLED=true`, otherwise a no-op implementation for tests and
  single-node operation.
