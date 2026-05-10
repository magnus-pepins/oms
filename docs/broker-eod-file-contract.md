# Broker EOD confirms file contract (OMS v1)

Formal contract for files ingested via **`POST /internal/v1/settlement/file-import`** (multipart), **`POST …/broker-confirms/import-json`**, or the optional **drop-folder poller** (same bytes → same outcome). Counterparty-specific CSV/XML is **out of scope** for v1: translate upstream files to this JSON envelope in a broker adapter or ops pipeline.

## Envelope

UTF-8 JSON object with a single array field:

```json
{
  "rows": [
    { "executionId": 12345 },
    { "accountId": "550e8400-e29b-41d4-a716-446655440000", "venueExecRef": "BROKER-EXEC-9f3a" }
  ]
}
```

| Field | Type | Required | Meaning |
|--------|------|----------|---------|
| `rows` | array | yes | Non-empty after parse. Max length from **`OMS_SETTLEMENT_FILE_IMPORT_MAX_ROWS`**. |

## Row (`BrokerFixtureRow`)

Exactly one of:

| Variant | Fields | Resolution |
|---------|--------|------------|
| **A** | `executionId` (positive integer) | Direct **`executions.id`**. Must exist and refer to a **TRADE** row OMS accepts for broker confirm registration. |
| **B** | `accountId` (UUID), `venueExecRef` (non-blank string) | Lookup **`executions`** for matching venue execution reference and account. |

Invalid shapes (both missing, both set inconsistently, null IDs) are counted as **skipped-invalid** and do not fail the whole batch unless **zero** rows resolve (implementation registers valid subset; empty `rows` fails batch with `failed`).

## Idempotency

1. **File-level:** SHA-256 of raw file bytes → unique **`settlement_file_import_batch.file_sha256_hex`**. Duplicate upload returns **`duplicate: true`** with existing **`batchId`**; no second **`broker_settlement_confirm`** registration for the same bytes.
2. **Row-level:** **`broker_settlement_confirm`** insert uses **`ON CONFLICT DO NOTHING`** on **`execution_id`** (see Flyway V12). Re-importing the same resolved execution ids is a no-op at the confirm queue.

## Status lifecycle (`settlement_file_import_batch.status`)

`received` → `parsing` → `applied` | `failed`

| Status | Meaning |
|--------|---------|
| `applied` | JSON valid, at least one row; confirm rows registered in slices (see **`OMS_SETTLEMENT_FILE_IMPORT_REGISTER_SLICE_SIZE`**). |
| `failed` | Parse error, empty `rows`, or unexpected runtime error; **`error_summary`** truncated per **`OMS_SETTLEMENT_FILE_IMPORT_ERROR_SUMMARY_MAX_CHARS`**. |

## Error taxonomy (operator-facing)

| Situation | Batch status | HTTP (multipart) | Notes |
|-----------|--------------|------------------|-------|
| File too large | — | **413** | Before SHA; see **`OMS_SETTLEMENT_FILE_IMPORT_MAX_BYTES`**. |
| Duplicate bytes | prior row | **200** | `duplicate: true` in JSON body. |
| Invalid JSON / empty rows | `failed` | **200** | Body includes `status: failed`, `errorSummary`. |
| Partial invalid rows | `applied` | **200** | Counters: `skippedInvalid`, `skippedUnresolved`. |

## Transport matrix

| Channel | Auth | When to use |
|---------|------|----------------|
| **Multipart HTTP** | **`X-OMS-Internal-Key`** | Automation with object storage presign → proxy upload; CI/UAT. |
| **import-json** | Internal key | Dev quick paths (no SHA batch row unless you migrate to file-import only). |
| **Drop folder** | Filesystem + host hardening | **`OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_ENABLED`**; see [settlement-eod-ingest.md](settlement-eod-ingest.md). SFTP/S3 native poller is **not** in OMS v1: use **rclone**, **vendor SFTP→NFS**, or sidecar that writes `.json` into the watched directory. |

## Broker column mapping (product / §16)

Document per counterparty: file column → variant **A** or **B** fields. OMS does not interpret broker CSV natively in v1.

## Related

- [settlement-eod-ingest.md](settlement-eod-ingest.md)
- [settlement.md](settlement.md)
- Sample: `src/test/resources/settlement/broker-fixture-sample.json`
