# Drop-copy and domain events

The OMS publishes domain events for downstream consumers — desk live feed,
internal drop copy, ops dashboards, external drop copy (via FIX, slice 2+).

Slice 1 ships the contract and a no-op publisher. **Slice 1.5** adds an optional
NATS JetStream publisher plus **`OrderRejected`** and **`OrderWorking`** emission
from `ControlTailer` after successful reject / working CAS. External FIX drop copy is slice 2+.

## Envelope (wire schema)

```json
{
  "schemaVersion": 1,
  "type": "OrderAccepted",
  "orderId": "uuid",
  "eventSeq": 1,
  "shardId": 0,
  "accountIdHash": "hex string",
  "side": "BUY",
  "instrumentSymbol": "AAPL",
  "quantity": "10",
  "limitPrice": "150.00",
  "timeInForce": "DAY",
  "acceptedAt": "ISO-8601 instant"
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
| `OrderAccepted`    | `OrdersController.createOrder`      | After Postgres commit, before HTTP response is returned. |
| `OrderRejected`    | `ControlTailer.apply`               | After successful CAS sets `status=REJECTED` (stale queue, buying power, ledger error). |
| `OrderWorking`     | `ControlTailer.apply`               | After successful CAS sets `status=WORKING`.                               |
| Execution events   | Slice 2+ FIX gateway                | After Ledger settlement leg lands.                       |

## Mandatory rule

Domain events MUST be published only after the originating Postgres
transaction has committed. This is the same invariant that gates
Chronicle appends.

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
