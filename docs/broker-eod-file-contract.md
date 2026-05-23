# Broker EOD confirms file contract (OMS v1)

Formal contract for files ingested via **`POST /internal/v1/settlement/file-import`** (multipart), **`POST …/broker-confirms/import-json`**, or the optional **drop-folder poller** (same bytes → same outcome). Counterparty-specific CSV/XML is **out of scope** for v1: translate upstream files to this JSON envelope in a broker adapter or ops pipeline.

> **Heads-up (in progress, 2026-05-23):** the row shape below carries only the broker's reference to an existing OMS execution; it is **not** the broker's economic record. A versioned **economic confirm contract** is being added per [`system-documentation/plans/stock-settlement-production-gap-plan.md`](../../system-documentation/plans/stock-settlement-production-gap-plan.md) §5.1 — see **"v2 economic confirm contract"** below for the in-progress shape and the new tables in Flyway **`V54__broker_trade_confirms.sql`**.

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

---

## v2 economic confirm contract (in progress)

**Status:** schema + envelope records landed; parser, repository, controller, and matcher land in follow-up slices. Tracked in [`system-documentation/plans/stock-settlement-production-gap-plan.md`](../../system-documentation/plans/stock-settlement-production-gap-plan.md) §5.1.

The v1 fixture rows above only identify executions. The v2 envelope additionally carries the broker's authoritative trade record so trade matching (gap plan §5.2) can detect mismatches before the existing `broker_settlement_confirm` queue advances `executions.settlement_status`.

### Envelope (`BrokerTradeConfirmEnvelope`)

```json
{
  "schemaVersion": 1,
  "brokerId": "broker_x",
  "fileId": "BROKERX-TRADES-20260523-001",
  "businessDate": "2026-05-23",
  "generatedAt": "2026-05-23T21:15:00Z",
  "rows": [
    {
      "brokerTradeId": "BT123",
      "venueExecRef": "EX123",
      "accountId": "550e8400-e29b-41d4-a716-446655440000",
      "brokerAccount": "BALH-OMNI-001",
      "custodyAccountId": "a0000001-0000-4000-8000-000000000001",
      "instrument": {
        "symbol": "ERIC-B.ST",
        "isin": "SE0000108656",
        "mic": "XSTO",
        "currency": "SEK"
      },
      "side": "BUY",
      "quantity": "10",
      "price": "78.42",
      "grossAmount": "784.20",
      "fees": [
        { "type": "commission", "amount": "1.00", "currency": "SEK", "chargedTo": "customer" }
      ],
      "tradeDate": "2026-05-23",
      "settlementDate": "2026-05-27",
      "settlementCurrency": "SEK",
      "status": "confirmed",
      "correctionType": "new"
    }
  ]
}
```

Schema version `1` is the first version of the **economic** envelope; the existing fixture format (above) is implicit v0 of the fixture path. Bump and add an explicit converter when the row shape changes; never reuse a number for an incompatible shape.

### Field rules (envelope)

| Field | Required | Notes |
|-------|----------|-------|
| `schemaVersion` | yes | Must equal `BrokerTradeConfirmEnvelope.CURRENT_SCHEMA_VERSION`. |
| `brokerId` | yes | Identifies the originating broker partner. Used in `(broker_id, broker_file_id)` idempotency. |
| `fileId` | yes | Broker-side file identifier; unique per `brokerId`. |
| `businessDate` | yes | Business date the file represents. |
| `generatedAt` | no | Wall-clock time the broker generated the file. |
| `rows` | yes | Non-empty after parse. Max length from `OMS_SETTLEMENT_FILE_IMPORT_MAX_ROWS` (shared with v1 fixture path until split). |

### Field rules (row)

| Field | Required | Notes |
|-------|----------|-------|
| `brokerTradeId` | yes | Broker-side trade identifier; unique per `brokerId`. |
| `venueExecRef` | yes for resolution | Primary key for matching against `executions`. |
| `accountId` | yes for resolution | Customer account UUID. |
| `brokerAccount` | no | Broker-side account/omnibus reference. |
| `custodyAccountId` | no | UUID into `custody_accounts`; defaults to broker's omnibus row if absent at matcher time. |
| `instrument.symbol` | yes | Trading symbol used by OMS. |
| `instrument.isin` / `.mic` / `.currency` | no | Supplied where the broker has it; used by the matcher to detect identifier drift. |
| `side` | yes | `BUY` or `SELL`. |
| `quantity` / `price` | yes | Decimal strings; parsed as `BigDecimal`. |
| `grossAmount` | no | If supplied, matcher compares against `quantity × price` within configured tolerance. |
| `fees[]` | no | Typed fee rows (see [`BrokerTradeConfirmFeeRow`](../src/main/java/com/balh/oms/settlement/BrokerTradeConfirmFeeRow.java)). |
| `tradeDate` / `settlementDate` | yes | ISO dates; matcher compares `settlementDate` against the per-instrument calendar (gap plan §5.3). |
| `settlementCurrency` | no | Defaults to `instrument.currency` if absent. |
| `status` | no | Broker lifecycle hint (e.g. `confirmed`, `settled`, `failed`); informational until the broker-side state-machine work in gap plan §5.8. |
| `correctionType` | yes | `new`, `amend`, `cancel`, or `bust`. Amends/cancels reference the original via `(brokerId, brokerTradeId)`. |

### Persistence (Flyway `V54__broker_trade_confirms.sql`)

- `broker_confirm_batch` — one row per ingested file; unique on `file_sha256_hex` **and** on `(broker_id, broker_file_id)`. Status lifecycle: `received → parsing → parsed → matching → applied | failed`.
- `broker_trade_confirm` — one row per broker trade; unique on `(broker_id, broker_trade_id)`. `match_status` lifecycle: `pending → matched | mismatch | unresolved | waived`. `resolved_execution_id` links to `executions` once the matcher resolves the row.
- `broker_trade_confirm_fee` — typed fee rows per confirm; `charged_to` is `customer | bank | tax_authority`.

### Out of this slice

The v2 envelope **parser**, the `POST /internal/v1/settlement/broker-trade-confirms/import-json` endpoint, the file-import path, the matcher (gap plan §5.2), and the `reconciliation_breaks` table are tracked as follow-up slices of Phase A.

## Related

- [settlement-eod-ingest.md](settlement-eod-ingest.md)
- [settlement.md](settlement.md)
- [system-documentation/plans/stock-settlement-production-gap-plan.md](../../system-documentation/plans/stock-settlement-production-gap-plan.md)
- Sample: `src/test/resources/settlement/broker-fixture-sample.json`
