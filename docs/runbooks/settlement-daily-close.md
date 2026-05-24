# Settlement daily close (Phase C exit)

Post-EOD automation reconciles parsed broker files without manual `curl` reconcile steps.

## Components

| Piece | Env / config | Role |
|-------|----------------|------|
| Drop-folder ingest | `OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_ENABLED=true` | Routes all envelope types via `SettlementFileIngestRouter` |
| Daily close job | `OMS_SETTLEMENT_DAILY_CLOSE_ENABLED=true` | Cron `OMS_SETTLEMENT_DAILY_CLOSE_CRON` (default `0 0 22 * * *`) |
| Lookback | `OMS_SETTLEMENT_DAILY_CLOSE_LOOKBACK_HOURS` (default 48) | Parsed batches eligible per step |
| Batch cap | `OMS_SETTLEMENT_DAILY_CLOSE_BATCH_LIMIT` (default 20) | Per-step limit per tick |

## Drop-folder layout

```
/opt/broker-eod/          # OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_PATH
  confirm.json
  positions.json
  cash.json
  corporate-actions.json
  fails.json
  .oms-done/              # successful ingests
  .oms-failed/            # rejected/failed ingests
```

Copy broker EOD JSON into the watch directory (SFTP sidecar / rclone). Supported envelopes: v0/v2 trade confirms, position snapshot (`rows[].quantityTotal`), cash statement (`movements`), corporate actions (`events`), settlement fails (`fails`).

## Daily close steps (in order)

1. **Trade confirms** — `BrokerTradeConfirmBatchLifecycleService.processAllParsedBatches()`
2. **Position** — `POST …/broker-position-snapshots/batches/{id}/reconcile` for parsed batches without a report in the lookback window
3. **Cash** — same for cash statements (includes Ledger nostro compare when `oms.ledger.enabled=true`)
4. **Corporate actions** — apply batch, then `POST …/broker-corporate-actions/batches/{id}/reconcile`
5. **Settlement fails** — `POST …/broker-settlement-fails/batches/{id}/apply`

Micrometer: `oms_settlement_daily_close_total{step,outcome}`.

## Ops checks (Beard Admin)

- `/settlement-recon` — position/cash/CA batch lists and reconciliation reports
- `/settlement-ops` — stuck outbox, open breaks
- `GET /internal/v1/settlement/ops-metrics/summary` — aggregate gauges

## Pop enablement (operator)

```bash
# oms-ingress env (ecosystem.config.cjs or host env)
export OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_ENABLED=true
export OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_PATH=/path/to/broker-eod
export OMS_SETTLEMENT_DAILY_CLOSE_ENABLED=true
```

Restart **oms-ingress** only (`pm2 delete` + `pm2 start ecosystem.config.cjs --only oms-ingress`).
