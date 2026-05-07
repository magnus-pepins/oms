# PII policy

Mirrors plan §14.14. The implementation enforcement points are
`PiiHash`, `MetricsConfig` (Micrometer filter) and `MetricsLabelsPiiTest`.

## Default everywhere

- `account_id` — **hashed** (xxh64 keyed by `oms.pii.hash-secret`).
- All other identifiers (order ids, execution ids, instrument symbols)
  are not PII and may appear directly.

## Where raw `account_id` is allowed

- Postgres rows in `orders` and `control_outbox`. This is the system of
  record; access is gated by DB-level controls.
- Audit-grade trace logs **only when** `oms.pii.audit-trace-enabled=true`.
  This flag is `false` in dev and prod by default and is changed only for
  short, time-bounded investigations (incident triage, regulator request).

## Where raw `account_id` is forbidden

- Micrometer labels. A `MeterFilter` in `MetricsConfig` strips any meter
  whose tag key is in `MetricsConfig.FORBIDDEN_LABELS`.
- Default-level structured logs.
- Domain-event payloads on NATS / drop copy. We ship `accountIdHash`
  (already hashed) on `OrderAcceptedEvent`.

## Test coverage

- `MetricsLabelsPiiTest` walks every registered meter at the end of a
  Spring Boot context start and asserts none of them carry a forbidden
  label.
- `OrdersRepository` insert path computes the hash exactly once and
  stores it on the row, so downstream metrics never need to re-hash.
