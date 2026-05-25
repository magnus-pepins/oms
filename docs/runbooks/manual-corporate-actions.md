# Manual corporate actions (Phase 1 templates)

Ops-only path for mergers, spin-offs, and bankruptcy/delistings when broker CA files are unavailable.

Rights issues and tender offers remain **automated** via customer elections + broker apply — do not use manual templates for those.

## API (OMS internal, API key)

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/internal/v1/corporate-actions/manual-events` | Create pending event |
| `POST` | `/internal/v1/corporate-actions/manual-events/{id}/approve` | Four-eyes approve → inserts `corporate_action_event` |
| `GET` | `/internal/v1/corporate-actions/manual-events` | List recent |

### Create body

```json
{
  "templateType": "MERGER",
  "createdBy": "ops.alice",
  "payloadJson": {
    "instrumentSymbol": "OLD.ST",
    "effectiveDate": "2026-06-01",
    "survivorSymbol": "NEW.ST",
    "notes": "Broker merger notice ref 123"
  }
}
```

Templates: `MERGER` (requires `survivorSymbol`), `SPIN_OFF` (requires `spunOffSymbol`), `BANKRUPTCY_DELISTING`.

### Approve body

```json
{ "approver": "ops.bob" }
```

**Four-eyes:** approver must differ from `createdBy`.

Broker election handoff: `GET /internal/v1/corporate-action-events/{id}/elections/export` returns approved customer/ops elections as JSON for broker file/API submission (Phase 2, 2026-05-25).

After approve, run the normal CA processor (`CorporateActionProcessorJob`) or wait for the scheduler to process the new `corporate_action_event`. Processors for `MERGER`, `SPIN_OFF`, and `BANKRUPTCY_DELISTING` landed in Slice 17 (2026-05-25).

## When to use manual vs broker file

| Scenario | Path |
|----------|------|
| Broker sends CA JSON | Drop-folder / `POST …/broker-corporate-actions/import-json` |
| Merger/spin-off/bankruptcy, no broker file | Manual template + approve |
| Dividend/split/symbol change | Broker file or existing automated ingest |

See also: [settlement-daily-close.md](./settlement-daily-close.md).
