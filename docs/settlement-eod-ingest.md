# Broker EOD / confirms file ingest

Slice 6 ships **HTTP** `POST /internal/v1/settlement/broker-confirms/import-json` for dev/UAT fixtures and **`POST /internal/v1/settlement/file-import`** (multipart JSON, same **`rows`** model) for **durable ingest** with **`settlement_file_import_batch`** SHA-256 idempotency.

**Optional drop-folder poller (v1):** when **`OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_ENABLED=true`** and **`OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_PATH`** points at a directory, **`SettlementFileDropFolderIngestScheduler`** ingests each `*.json` file through the same pipeline as multipart upload, then moves the file to **`.oms-done/`** or **`.oms-failed/`** under that directory. This is the supported way to bridge **SFTP/S3** without embedding cloud SDKs in OMS: run **rclone**, **aws s3 sync**, or a vendor agent that lands JSON into the watched folder.

Full contract: [broker-eod-file-contract.md](broker-eod-file-contract.md).

## Goals

- **Idempotency:** same file bytes (or same logical batch id from broker) must not double-insert `broker_settlement_confirm` or mutate positions twice.
- **Observability:** every run is a **batch row** (`settlement_file_import_batch`) with status, row counts, and error summary.
- **Mapping:** broker file columns → OMS `executions.id` (by venue ref + account, or by explicit execution id column per broker contract — TBD with counterparty). **v1 file-import** accepts the same JSON **`rows`** as **`import-json`**.

## Schema (Flyway V14)

Table **`settlement_file_import_batch`** (see migration) holds:

- **`file_sha256_hex`** — unique constraint for **at-most-once** acceptance of identical file content (adjust if broker supplies a stronger idempotency key).
- **`status`** — lifecycle: `received` → `parsing` → `applied` | `failed` (string for now; tighten to ENUM in a later migration if desired).

## Implementation phases (engineering)

1. **Ingest worker** — **Shipped (v1):** `POST /internal/v1/settlement/file-import` streams file bytes to SHA-256, inserts batch row `ON CONFLICT DO NOTHING` / duplicate short-circuit, parses JSON, updates status (`received` → `parsing` → `applied` | `failed`).
2. **Validate** — **v1:** JSON parse + non-empty **`rows`**; reject batch with `failed` + `error_summary`. Schema per broker remains product work.
3. **Map rows** — **v1:** reuse **`BrokerFixtureRow`** resolution (**`SettlementConfirmProcessor#resolveBrokerFixtureRows`**); register confirms in **transaction slices** (**`OMS_SETTLEMENT_FILE_IMPORT_REGISTER_SLICE_SIZE`**).
4. **Ops UI** — Beard Admin recon slice: list batches (**`GET …/file-import-batches`** when exposed); retry failed (policy-gated).

## Related

- [settlement.md](settlement.md) — current internal HTTP surface.
- Master plan §12.6 — EOD trades file matching.
