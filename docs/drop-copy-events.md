# Drop-copy and domain events

The OMS publishes domain events for downstream consumers — desk live feed,
internal drop copy, ops dashboards, external drop copy (via FIX, slice 2+).

Slice 1 ships the contract and a **transactional outbox** (`domain_event_outbox`)
drained by `DomainFanoutReconciler`. **NATS JetStream** (`FanoutClient`) is
optional when `OMS_NATS_ENABLED=true`. **`OrderRejected`** and **`OrderWorking`**
are written to the same outbox inside `ControlTailer` after successful CAS.
**`OrderPartiallyFilled`**, **`OrderFilled`**, and **`OrderCancelled`** are written
by `ExecutionReportApplier` (slice 3) after execution rows apply; applied **trades** also write **`positions`** / **`position_history`** (slice 6 — see `docs/settlement.md`). External FIX
drop copy reuses the same envelope shapes in slice 4+.

## Envelope (wire schema)

The NATS message body is a **single JSON object** with a nested `payload` for the
event-specific record:

```json
{
  "schemaVersion": 1,
  "type": "OrderAccepted",
  "occurredAt": "2026-05-07T12:00:00Z",
  "correlationId": "order-uuid",
  "payload": {
    "orderId": "uuid",
    "eventSeq": 0,
    "shardId": 0,
    "accountIdHash": "hex string",
    "side": "BUY",
    "instrumentSymbol": "AAPL",
    "quantity": "10",
    "limitPrice": "150.00",
    "timeInForce": "DAY",
    "acceptedAt": "ISO-8601 instant"
  }
}
```

`OrderWorking` (after successful CAS to `WORKING` in `ControlTailer`; `payload` mirrors accept for desk continuity):

```json
{
  "schemaVersion": 1,
  "type": "OrderWorking",
  "occurredAt": "2026-05-07T12:00:01Z",
  "correlationId": "order-uuid",
  "payload": {
    "orderId": "uuid",
    "eventSeq": 1,
    "shardId": 0,
    "accountIdHash": "hex string",
    "side": "BUY",
    "instrumentSymbol": "AAPL",
    "quantity": "10",
    "limitPrice": "150.00",
    "timeInForce": "DAY",
    "workingAt": "ISO-8601 instant"
  }
}
```

`OrderRejected` (after successful CAS to `REJECTED` in `ControlTailer`):

```json
{
  "schemaVersion": 1,
  "type": "OrderRejected",
  "occurredAt": "2026-05-07T12:00:01Z",
  "correlationId": "order-uuid",
  "payload": {
    "orderId": "uuid",
    "eventSeq": 1,
    "shardId": 0,
    "accountIdHash": "hex string",
    "rejectCode": "RISK_BUYING_POWER",
    "terminalAt": "ISO-8601 instant"
  }
}
```

- `schemaVersion` is mandatory on every event. Bump it for breaking
  changes.
- `eventSeq` is `orders.version` at the moment the event was emitted.
  Consumers may use it to dedupe.
- `accountIdHash` is the hashed account id. Raw `accountId` is **never**
  on the wire.

## Emit points

| Event              | Emitted from                        | Trigger                                                  |
|--------------------|-------------------------------------|----------------------------------------------------------|
| `OrderAccepted`    | `OrderIngressService.persistAccepted` | Same Postgres transaction as `orders` + `control_outbox` insert; reconciler delivers after commit. |
| `OrderRejected`    | `ControlTailer.apply`               | Same transaction as successful CAS to `REJECTED` (stale queue, buying power, ledger error). |
| `OrderWorking`     | `ControlTailer.apply`               | Same transaction as successful CAS to `WORKING`.                               |
| `OrderPartiallyFilled` | `ExecutionReportApplier.applyTrade` | After successful CAS to `PARTIALLY_FILLED` (slice 3). |
| `OrderFilled`      | `ExecutionReportApplier.applyTrade` | After successful CAS to `FILLED` (slice 3). |
| `OrderCancelled`   | `ExecutionReportApplier.applyCancel`| After successful CAS to `CANCELLED` (slice 3). |
| Execution events   | Slice 4+ FIX gateway                | Same applier path as simulated fills once FIX lands.                       |

## Mandatory rule

Domain events MUST be written to `domain_event_outbox` only inside the
originating Postgres transaction, and handed to NATS only after commit by
`DomainFanoutReconciler`. This mirrors the invariant that gates Chronicle appends.

## Subscriber expectations

- Treat `eventSeq` as monotonic per `orderId`. Drop events whose
  `(orderId, eventSeq)` has already been processed.
- Tolerate duplicates. Re-emit on subscriber side is allowed when in
  doubt.
- Tolerate ordering relaxation across `orderId`s. Per-order ordering is
  guaranteed; cross-order is not.

## External drop copy

The FIX drop-copy session is a separate consumer of the same NATS subject
tree. The mapping from `OrderAccepted` / `OrderWorking` / etc. to FIX
ER messages lives in the slice-2 FIX gateway service.

## Slice 8 — durable archive consumer (deferred)

A **long-retention JetStream consumer** (or object-store archive) that copies the same envelopes to compliance storage is **not** bundled in the OMS JAR. **Formal defer:** [plans/oms-realignment-2026-05-07.md](../../../system-documentation/plans/oms-realignment-2026-05-07.md) §5.1 — owner **compliance + platform**; gate **retention policy + infra** (S3 prefix, idempotent sink, bounded batch) before in-process wiring.

Operators may run a **sidecar** subscriber using this document as the schema contract; OMS continues to publish via **`DomainFanoutReconciler`** + optional NATS JetStream only.
