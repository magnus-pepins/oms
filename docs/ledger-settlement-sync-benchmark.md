# Ledger settlement outbox sync benchmark (11.9.4)

## Purpose

Record **p50 / p95** latency from OMS Postgres commit of `ledger_settlement_outbox` rows to **`posted_at`** non-null after Ledger HTTP ACK, before Phase 1 sign-off.

## Procedure

1. Enable **`OMS_LEDGER_SETTLEMENT_OUTBOX_ENABLED=true`** on a **non-prod** stack with representative data volume (or load generator creating `settled` TRADE rows).
2. Enable **`OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED=true`** with production-intent batch size / interval.
3. Capture Prometheus timer **`oms_ledger.settlement.outbox.post`** (Micrometer name uses dots; includes failed posts too) **or** run the SQL below for `posted_at - created_at`:

```sql
SELECT
  percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (posted_at - created_at))) AS p50_s,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (posted_at - created_at))) AS p95_s
FROM ledger_settlement_outbox
WHERE posted_at IS NOT NULL
  AND created_at > now() - interval '1 hour';
```

4. Paste results + environment (region, DB size, reconciler settings) into the Phase 1 exit ticket and **`plans/oms-phase1-exit-actions.md`** evidence index when owners attach the bundle.

### Prometheus (Micrometer timer)

The reconciler records **`oms.ledger.settlement.outbox.post`** as a Micrometer `Timer` with **`publishPercentileHistogram()`**. On Prometheus scrape, the usual name is:

- **`oms_ledger_settlement_outbox_post_seconds`** — `_bucket` / `_sum` / `_count` suffixes.

Example **p95 over 1h** from histogram buckets (adjust `le` labels to match your scrape; classic histogram quantile):

```promql
histogram_quantile(
  0.95,
  sum(rate(oms_ledger_settlement_outbox_post_seconds_bucket[1h])) by (le)
)
```

Use the same pattern for **0.50** quantile. Cross-check against the SQL in §Procedure when both are available.

## Measured results (appendix — owner fills after benchmark run)

| Environment | Reconciler batch / interval | p50 (s) | p95 (s) | Notes |
|-------------|-----------------------------|---------|---------|--------|
| *TBD* | *TBD* | *TBD* | *TBD* | Paste from Phase 1 exit ticket when finance + eng sign off. |

## Acceptance

Finance + eng agree the numbers meet the interim SLO documented in the master plan row; if not, file a follow-up slice (do not silently widen timeouts).
