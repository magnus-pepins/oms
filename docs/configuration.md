# Configuration reference

Every named limit in the OMS slice 1, with defaults and meaning. Mirrors
[.env.example](../.env.example) and `application.yaml`.

Per the Balh ecosystem rule (`config-and-limits.mdc`), no bare numeric
literals appear in code for timeouts, retries, batch sizes, etc. Add a
new key here when introducing one.

## HTTP

| Key                         | Default | Meaning                                                    |
|-----------------------------|---------|------------------------------------------------------------|
| `OMS_HTTP_PORT`             | `8088`  | Bind port for the internal HTTP surface (default avoids **8080**, used by the marketing site on many dev hosts). |
| `OMS_INTERNAL_API_KEY`      | (empty) | Shared secret for `/internal/v1/**`. Empty rejects all.    |

## Postgres

| Key                              | Default                                  | Meaning                                  |
|----------------------------------|------------------------------------------|------------------------------------------|
| `OMS_PG_URL`                     | `jdbc:postgresql://localhost:5432/oms`   | JDBC URL.                                |
| `OMS_PG_USER`                    | `oms`                                    | DB user.                                 |
| `OMS_PG_PASSWORD`                | `oms`                                    | DB password.                             |
| `OMS_PG_POOL_MAX_SIZE`           | `20`                                     | HikariCP max pool size.                  |
| `OMS_PG_POOL_MIN_IDLE`           | `5`                                      | HikariCP min idle.                       |
| `OMS_PG_CONNECTION_TIMEOUT_MS`   | `2000`                                   | HikariCP connection timeout.             |

## Sharding

| Key                | Default | Meaning                                          |
|--------------------|---------|--------------------------------------------------|
| `OMS_SHARD_ID`     | `0`     | Logical shard id of this instance.               |
| `OMS_SHARD_COUNT`  | `1`     | Total shard count for `xxh64 mod` mapping.       |

## Control plane

| Key                                | Default | Meaning                                                                                              |
|------------------------------------|---------|------------------------------------------------------------------------------------------------------|
| `OMS_CONTROL_MAX_JOB_AGE_MS`       | `300000` | If a control event sits unprocessed past this (ms), reject with `RISK_STALE_QUEUE`. Interim default 5 min — [oms-phase0-interim-decisions.md](../../system-documentation/plans/oms-phase0-interim-decisions.md). |
| `OMS_TAILER_BATCH_SIZE`            | `100`   | Reserved for future control-plane batching (Disruptor slice); Chronicle tail batch uses `OMS_CHRONICLE_TAIL_BATCH_MAX_MESSAGES`. |

## Risk (slice 2)

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_RISK_INSTRUMENT_ALLOWLIST_ENABLED` | `false` | When `true`, only symbols in `OMS_RISK_ALLOWED_INSTRUMENT_SYMBOLS` may pass control. |
| `OMS_RISK_ALLOWED_INSTRUMENT_SYMBOLS` | (empty) | Comma-separated list (e.g. `AAPL,MSFT`). Compared uppercased. |
| `OMS_RISK_INSTRUMENT_TRADABILITY_CHECK_ENABLED` | `false` | When `true`, symbol must appear in the tradable set (CSV and/or Marketdata HTTP cache; see next rows) or control rejects **`RISK_INSTRUMENT_NOT_ALLOWED`**. |
| `OMS_RISK_INSTRUMENT_TRADABILITY_FROM_MARKETDATA_ENABLED` | `false` | When **`true`** with **`OMS_MARKETDATA_HTTP_ENABLED`** and a non-empty instruments cache, tradability uses **`MarketdataInstrumentsCache`** instead of **`OMS_RISK_TRADABLE_INSTRUMENT_SYMBOLS`** (if cache is empty, CSV applies). |
| `OMS_RISK_TRADABLE_INSTRUMENT_SYMBOLS` | (empty) | Comma-separated tradable symbols when tradability check is enabled (used alone or as fallback when Marketdata cache is empty). |
| `OMS_RISK_INSTRUMENT_SYMBOL_HALT_CHECK_ENABLED` | `false` | When `true`, symbols in **`OMS_RISK_HALTED_INSTRUMENT_SYMBOLS`** reject with **`RISK_SYMBOL_HALT`** (before allowlist / tradability). |
| `OMS_RISK_HALTED_INSTRUMENT_SYMBOLS` | (empty) | Comma-separated halted symbols when halt check is enabled. |
| `OMS_RISK_FAT_FINGER_MAX_LIMIT_PRICE` | `0` | Max limit price per order; `0` disables. |
| `OMS_RISK_FAT_FINGER_MAX_ORDER_QUANTITY` | `0` | Max order quantity; `0` disables. |
| `OMS_RISK_MAX_ORDER_NOTIONAL` | `0` | Max `quantity × limit_price`; `0` disables. |
| `OMS_RISK_SANCTIONS_RECHECK_ENABLED` | `false` | When **`true`**, runs **`SanctionsExecutionGate`** before other risk checks. |
| `OMS_RISK_SANCTIONS_RECHECK_STRICT` | `false` | When **`true`** with recheck enabled, rejects every order with **`RISK_COMPLIANCE_SANCTIONS`** until a screening client is integrated. |
| `OMS_SANCTIONS_CACHE_MAX_AGE_S` | `3600` | Permissive cache: seconds between re-screen touches per account (see **`SanctionsExecutionGate`**). |
| `OMS_RISK_ORDER_MIN_INTERVAL_MS_PER_ACCOUNT` | `0` | Wall-clock spacing between control evaluations per account; **`0`** disables; otherwise **`RISK_RATE_LIMIT`**. |
| `OMS_RISK_TICK_SIZE_CHECK_ENABLED` | `false` | When **`true`**, limit price must align to **`OMS_RISK_TICK_SIZE_INCREMENT`**. |
| `OMS_RISK_TICK_SIZE_INCREMENT` | `0` | Tick grid increment; **`0`** disables tick enforcement. |
| `OMS_RISK_STP_GATE_ENABLED` | `false` | STP / venue-calendar placeholder. |
| `OMS_RISK_STP_GATE_REJECT_ALL` | `false` | When **`true`** with STP gate enabled, rejects with **`RISK_STP_GATE`** (testing only). |
| `OMS_RISK_FIX_ROUTE_SEND_ENABLED_CHECK_ENABLED` | `false` | When **`true`** with **`OMS_ROUTING_BACKEND=fix`**, rejects with **`RISK_MARKET_SESSION_CLOSED`** if **`fix_route_state.send_enabled`** is false for **`OMS_FIX_ROUTE_KEY`**. |
| `OMS_RISK_MAX_AGGREGATE_POSITION_QUANTITY_CHECK_ENABLED` | `false` | When **`true`**, BUY orders that would push **`positions.quantity_total`** above **`OMS_RISK_MAX_AGGREGATE_POSITION_QUANTITY`** reject with **`RISK_CONCENTRATION_LIMIT`** (default custody row). |
| `OMS_RISK_MAX_AGGREGATE_POSITION_QUANTITY` | `0` | Max position quantity per account+symbol; **`0`** disables the check even when the check flag is **`true`**. |

## Marketdata HTTP (optional — slice 5)

Transport choice vs MQTT: [marketdata-ingestion-path.md](marketdata-ingestion-path.md).

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_MARKETDATA_HTTP_ENABLED` | `false` | When **`true`**, wires **`RestClient`** + **`MarketdataPlatformHttpClient`** against **`OMS_MARKETDATA_HTTP_BASE_URL`**. |
| `OMS_MARKETDATA_HTTP_BASE_URL` | `http://localhost:6001` | Root URL for instruments/NBBO HTTP paths. |
| `OMS_MARKETDATA_HTTP_API_KEY` | (empty) | Value for **`X-Marketdata-Key`** on each request. |
| `OMS_MARKETDATA_HTTP_CONNECT_TIMEOUT_MS` | `2000` | HTTP connect timeout. |
| `OMS_MARKETDATA_HTTP_READ_TIMEOUT_MS` | `5000` | HTTP read timeout. |
| `OMS_MARKETDATA_HTTP_INSTRUMENTS_PATH` | `/internal/v0/instruments` | Path for **`GET`** instruments list (JSON array, **`symbols`** array, or **`instruments`** objects with **`symbol`**). |
| `OMS_MARKETDATA_HTTP_NBBO_PATH` | `/internal/v0/nbbo` | Path for **`GET`** NBBO with **`symbol`** query param; JSON **`bid`**, **`ask`**, optional **`asOf`**. |
| `OMS_MARKETDATA_HTTP_INSTRUMENTS_REFRESH_INTERVAL_MS` | `60000` | **`MarketdataInstrumentsCache`** refresh interval. |
| `OMS_MARKETDATA_HTTP_NBBO_IN_MARKET_CONTEXT_ENABLED` | `false` | When **`true`** with **`OMS_ROUTING_NBBO_REFERENCE_IN_MARKET_CONTEXT_ENABLED`**, trade apply prefers HTTP NBBO before stub bid/ask (**`quoteClass=NBBO_MARKETDATA_HTTP`** in **`nbboClassReference`**). |

## Routing / return path (slice 3)

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_ROUTING_BACKEND` | `noop` | `noop` — no post-`WORKING` dispatch. `simulated` — `SimulatedBrokerDispatcher` + `SimulatedReturnPathProjectionWorker` emit synthetic fills. `fix` — `FixRouteDispatcher` queues ids for QuickFIX/J outbound (slice 4; initiator send next). |
| `OMS_MARKET_CONTEXT_STUB_JSON` | `{"stub":true}` | Base JSON merged into `market_context.snapshot_json` on each trade apply; venue ER fields (`instrumentSymbol`, `venueId`, `venueExecRef`, fills, …) are merged on top until marketdata/NBBO integration (slice 5). |
| `OMS_ROUTING_NBBO_REFERENCE_IN_MARKET_CONTEXT_ENABLED` | `false` | When **`true`**, each trade apply may add **`nbboClassReference`** to venue evidence JSON: HTTP NBBO when **`OMS_MARKETDATA_HTTP_NBBO_IN_MARKET_CONTEXT_ENABLED`**, else stub bid/ask (**`quoteClass=NBBO_STUB`**). |
| `OMS_ROUTING_NBBO_STUB_BID_PRICE` | `0` | Stub NBBO bid; **`0`** skips NBBO block. |
| `OMS_ROUTING_NBBO_STUB_ASK_PRICE` | `0` | Stub NBBO ask; **`0`** skips NBBO block. |
| `OMS_SIMULATED_VENUE_ID` | `SIM` | `venue_id` on synthetic executions. |
| `OMS_SIMULATED_QUEUE_CAPACITY` | `10000` | Bounded queue for `WORKING` order ids awaiting simulation. |
| `OMS_SIMULATED_POLL_INTERVAL_MS` | `50` | `@Scheduled` drain interval when simulated backend is enabled. |
| `OMS_SIMULATED_SCHEDULER_ENABLED` | `true` | When `false`, only explicit `SimulatedReturnPathProjectionWorker.processPendingQueueOnce()` drains the queue (used in integration tests). |

## Settlement / positions (slice 6)

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_SETTLEMENT_DEFAULT_CUSTODY_ACCOUNT_ID` | `a0000001-0000-4000-8000-000000000001` | UUID of the **`custody_accounts`** row used when applying **trade** fills to **`positions`** (must match DB; Flyway **V11** seeds the default omnibus). See [settlement.md](settlement.md). |
| `OMS_SETTLEMENT_BROKER_CONFIRM_RECONCILER_ENABLED` | `false` | When **`true`**, **`BrokerSettlementConfirmScheduler`** drains **`broker_settlement_confirm`** on a fixed delay. |
| `OMS_SETTLEMENT_BROKER_CONFIRM_RECONCILER_INTERVAL_MS` | `10000` | Delay between scheduler passes (ms; minimum **100** in `OmsConfig`). |
| `OMS_SETTLEMENT_BROKER_CONFIRM_RECONCILER_BATCH_SIZE` | `50` | Max pending confirm rows processed per **`process-pending`** / scheduler tick. |
| `OMS_SETTLEMENT_BROKER_CONFIRM_HTTP_MAX_EXECUTION_IDS` | `100` | Max **`executionIds`** elements on **`POST /internal/v1/settlement/broker-confirms`**. |
| `OMS_SETTLEMENT_FILE_IMPORT_MAX_BYTES` | `10000000` | Max uploaded file bytes for **`POST /internal/v1/settlement/file-import`** (aligned with **`spring.servlet.multipart.max-file-size`**). |
| `OMS_SETTLEMENT_FILE_IMPORT_MAX_ROWS` | `10000` | Max **`rows`** entries read from one JSON file body. |
| `OMS_SETTLEMENT_FILE_IMPORT_REGISTER_SLICE_SIZE` | `50` | Max execution ids passed per DB transaction when registering **`broker_settlement_confirm`** during file ingest. |
| `OMS_SETTLEMENT_FILE_IMPORT_ERROR_SUMMARY_MAX_CHARS` | `2000` | Truncation bound for **`settlement_file_import_batch.error_summary`**. |
| `OMS_SETTLEMENT_FILE_IMPORT_LIST_MAX_LIMIT` | `200` | Max **`limit`** on **`GET /internal/v1/settlement/file-import-batches`**. |
| `OMS_SETTLEMENT_FILE_IMPORT_LIST_DEFAULT_LIMIT` | `50` | Default **`limit`** when the query param is omitted. |
| `OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_ENABLED` | `false` | When **`true`**, poll **`OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_PATH`** for `*.json` and ingest via **`SettlementFileImportService`** (see [broker-eod-file-contract.md](broker-eod-file-contract.md), [settlement-eod-ingest.md](settlement-eod-ingest.md)). |
| `OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_PATH` | _(empty)_ | Directory to watch; subdirs **`.oms-done/`** and **`.oms-failed/`** are created for archives. |
| `OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_POLL_INTERVAL_MS` | `30000` | Scheduler fixed delay between polls (minimum `5000`). |
| `OMS_SETTLEMENT_FILE_IMPORT_DROP_FOLDER_MAX_FILES_PER_POLL` | `20` | Max JSON files processed per wake (cap `500`). |
| `OMS_SETTLEMENT_MANUAL_ACTION_LIST_MAX_LIMIT` | `200` | Max **`limit`** on **`GET /internal/v1/settlement/manual-actions`**. |
| `OMS_SETTLEMENT_MANUAL_ACTION_LIST_DEFAULT_LIMIT` | `50` | Default page size when **`limit`** is omitted. |
| `OMS_SETTLEMENT_MANUAL_ACTION_TYPE_MAX_LENGTH` | `128` | Max length of **`action_type`** on create (capped at **512** in `OmsConfig`). |
| `OMS_SETTLEMENT_MANUAL_ACTION_PAYLOAD_JSON_MAX_CHARS` | `20000` | Max characters accepted for **`payload_json`** text on create (capped at **500000** in `OmsConfig`). |
| `OMS_SETTLEMENT_MANUAL_ACTION_AUTO_APPLY_ENABLED` | `true` | When **`true`**, **`POST …/manual-actions/{id}/approve`** runs supported **`action_type`** handlers in the **same** transaction as the approve CAS (see [settlement.md](settlement.md) — **`ManualSettlementActionTypes`**). Set **`false`** to record approvals only (legacy behaviour). |
| `OMS_SETTLEMENT_FREE_RIDING_ATTRIBUTION_ENABLED` | `false` | When **`true`**, BUY **`TRADE`** fills may merge prior unsettled BUY execution ids into **`executions.unsettled_funded_by_exec_ids`** (stub query — finance replaces rules). Increments **`oms_free_riding_attribution_merges_total`** when a merge runs. |
| `OMS_SETTLEMENT_FREE_RIDING_ATTRIBUTION_MAX_FUNDING_EXECUTIONS` | `8` | Max prior execution ids appended per fill (capped at **256** in `OmsConfig`). |

## Corporate actions (slice 8 stub)

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_CORPORATE_ACTION_PROCESSOR_ENABLED` | `false` | When **`true`**, **`CorporateActionProcessorJob`** marks unprocessed **`corporate_action_event`** rows **`processed_at`** (no-op stub; increments **`oms_corporate_action_events_processed_total`**). |
| `OMS_CORPORATE_ACTION_PROCESSOR_INTERVAL_MS` | `60000` | Fixed delay between processor wakes (minimum **5000**). |
| `OMS_CORPORATE_ACTION_PROCESSOR_BATCH_SIZE` | `50` | Max rows examined per wake (capped at **500**). |
| `OMS_CORPORATE_ACTION_INGEST_INSTRUMENT_SYMBOL_MAX_LENGTH` | `64` | Max **`instrument_symbol`** on **`POST /internal/v1/corporate-action-events`**. |
| `OMS_CORPORATE_ACTION_INGEST_ACTION_TYPE_MAX_LENGTH` | `64` | Max **`action_type`** on ingest. |
| `OMS_CORPORATE_ACTION_INGEST_PAYLOAD_JSON_MAX_CHARS` | `16000` | Max serialized **`payloadJson`** characters on ingest. |
| `OMS_CORPORATE_ACTION_LIST_MAX_LIMIT` | `200` | Max **`limit`** on **`GET /internal/v1/corporate-action-events`**. |
| `OMS_CORPORATE_ACTION_LIST_DEFAULT_LIMIT` | `50` | Default page size when **`limit`** is omitted on list. |

## Desk snapshot (internal — trading desk BFF)

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_DESK_SNAPSHOT_ENABLED` | `false` | When **`false`**, **`GET /internal/v1/desk/orders/snapshot`** returns **403** (feature off). When **`true`**, returns bounded open/recent orders for attendant read models. |
| `OMS_DESK_SNAPSHOT_MAX_LIMIT` | `50` | Max **`limit`** query capping (config clamps **1–500**). |
| `OMS_DESK_SNAPSHOT_MAX_AGE_HOURS` | `24` | Rows older than this window excluded from snapshot (config clamps **1–168** hours). |

## FX module flag (§11.5 stub)

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_FX_MODULE_ENABLED` | `false` | Master flag for internal **`/internal/v1/fx/**`** read surfaces. When **`true`**, **`GET /internal/v1/fx/health`** reports **`module_enabled_pending_impl`** plus a **`tracks`** map (M3 slice). When **`false`** → **`not_enabled`**. See [fx-architecture-slice8.md](fx-architecture-slice8.md) and [fx-backend-slice-m3.md](fx-backend-slice-m3.md). |
| `OMS_FX_QUOTE_STUB_ENABLED` | `false` | When **`true`** with module on, **`GET /internal/v1/fx/quotes`** returns a versioned **stub** quote row (`schemaVersion` from `OMS_FX_QUOTE_STUB_SCHEMA_VERSION`). |
| `OMS_FX_QUOTE_STUB_SCHEMA_VERSION` | `1` | Integer echoed on stub quote payload for contract tests. |
| `OMS_FX_NOSTRO_READ_ENABLED` | `false` | When **`true`** with module on, **`GET /internal/v1/fx/nostro/snapshot`** aggregates Ledger **`GET /balances/{id}`** rows for ids in `OMS_FX_NOSTRO_BALANCE_IDS_CSV` (requires **`OMS_LEDGER_ENABLED=true`**). |
| `OMS_FX_NOSTRO_BALANCE_IDS_CSV` | (empty) | Comma-separated Ledger `balance_id` values for the nostro snapshot. |
| `OMS_FX_MULTI_LEG_ATOMICITY_STUB_ENABLED` | `false` | When **`true`**, **`FxMultiLegAtomicityStubService`** may persist two-leg rows in **`fx_stub_leg_*`** (integration / rollback harness only). |
| `OMS_FX_HEDGE_HOOKS_ENABLED` | `false` | When **`true`**, **`GET /internal/v1/fx/hedge/hooks-status`** returns stub pause/kill-switch JSON and increments **`oms.fx.hedge_hook.probe`**. |
| `OMS_FX_EOD_FLATTEN_ENABLED` | `false` | When **`true`** with module on, schedules **`FxEodFlattenScheduler`** ticks at `OMS_FX_EOD_FLATTEN_INTERVAL_MS` (finance-gated log only). |
| `OMS_FX_EOD_FLATTEN_INTERVAL_MS` | `86400000` | Minimum **60000** ms in config clamp; delay between EOD flatten **stub** scheduler invocations. |

## FIX (slice 4+)

Used when `OMS_ROUTING_BACKEND=fix`. See [fix-out.md](fix-out.md) for session maps and runbooks.

| Key | Default | Meaning |
|-----|---------|---------|
| `OMS_FIX_AUTO_START` | `false` | When `true` (and routing `fix`), starts QuickFIX/J `SocketInitiator` and the outbound drain scheduler. |
| `OMS_FIX_OUTBOUND_QUEUE_CAPACITY` | `10000` | Bounded queue for `WORKING` order ids awaiting NOS. |
| `OMS_FIX_FILE_STORE_PATH` | `./queues/fix` | QuickFIX/J file store path. |
| `OMS_FIX_SOCKET_CONNECT_HOST` | `127.0.0.1` | Acceptor host. |
| `OMS_FIX_SOCKET_CONNECT_PORT` | `9876` | Acceptor port. |
| `OMS_FIX_SENDER_COMP_ID` | `OMS_INIT` | Session sender comp id. |
| `OMS_FIX_TARGET_COMP_ID` | `BROKER_ACCEPT` | Session target comp id. |
| `OMS_FIX_HEART_BT_INT` | `30` | Heartbeat interval (seconds). |
| `OMS_FIX_OUTBOUND_POLL_INTERVAL_MS` | `100` | Delay between outbound worker ticks. |
| `OMS_FIX_MAX_OUTBOUND_JOB_AGE_MS` | `0` | **0** = off; else reject stale `WORKING` at dequeue (`FIX_OUTBOUND_JOB_EXPIRED`). |
| `OMS_FIX_VENUE_ID_FOR_EXECUTIONS` | `FIX` | `venue_id` on executions from inbound ERs. |
| `OMS_FIX_USE_DATA_DICTIONARY` | `false` | QuickFIX/J data dictionary flag. |
| `OMS_FIX_SYMBOL_MAP_JSON` | (empty) | JSON object mapping OMS `instrument_symbol` → broker FIX `Symbol` on NOS (case-insensitive keys). Empty → identity. |
| `OMS_FIX_ROUTE_KEY` | `default` | `fix_route_state.route_key` read by the outbound worker; toggle send via internal API or SQL. |
| `OMS_FIX_OUTBOUND_TOKENS_PER_SECOND` | `0` | NOS token bucket refill rate; **`<= 0` disables** pacing. |
| `OMS_FIX_OUTBOUND_TOKEN_BURST` | `100` | Bucket capacity when rate limiting is enabled (minimum **1** in config). |
| `OMS_FIX_SESSION_STORE_TYPE` | `file` | **`file`** — `FileStoreFactory`; **`jdbc`** — `JdbcStoreFactory` (Flyway **V9** `oms_fix_sessions` / `oms_fix_messages`). |
| `OMS_FIX_SESSION_JDBC_DATASOURCE_ENABLED` | `false` | When **`true`** with JDBC store, QuickFIX uses **`OMS_FIX_SESSION_JDBC_*`** pool instead of the application `DataSource` (plan 6.4). |
| `OMS_FIX_SESSION_JDBC_URL` | (empty) | JDBC URL for the dedicated FIX session pool; **required** when session JDBC datasource is enabled. |
| `OMS_FIX_SESSION_JDBC_USER` / `OMS_FIX_SESSION_JDBC_PASSWORD` | (empty) | Credentials for the dedicated pool. |
| `OMS_FIX_SESSION_JDBC_POOL_MAX_SIZE` | `5` | HikariCP max pool size for the dedicated pool. |
| `OMS_FIX_SESSION_JDBC_POOL_MIN_IDLE` | `1` | HikariCP min idle. |
| `OMS_FIX_SESSION_JDBC_CONNECTION_TIMEOUT_MS` | `2000` | HikariCP connection timeout (ms; minimum **250** in `OmsConfig`). |
| `OMS_FIX_MASS_CANCEL_ON_DISCONNECT_ENABLED` | `false` | When **`true`**, initiator **`onLogout`** increments **`oms_fix_mass_cancel_disconnect_signal_total`** (observability only). **Venue cancel fan-out is deferred** until broker contract — see [fix-out.md](fix-out.md) § “Mass cancel on disconnect” and [realignment §5.1](../../../system-documentation/plans/oms-realignment-2026-05-07.md). |
| `OMS_FIX_MANUAL_MASS_CANCEL_ENABLED` | `false` | When **`true`**, **`POST /internal/v1/fix/mass-cancel-request`** is accepted (records intent + metrics; default **does not** wire QuickFIX **`MassCancelRequest`** until **`OMS_FIX_MANUAL_MASS_CANCEL_WIRE_ENABLED=true`**). |
| `OMS_FIX_MANUAL_MASS_CANCEL_WIRE_ENABLED` | `false` | When **`true`** (and manual mass cancel enabled), service may emit venue mass cancel (broker contract required in prod). |
| `OMS_FIX_MANUAL_MASS_CANCEL_REASON_MAX_CHARS` | `512` | Max **`reason`** length on manual mass cancel body (config clamps **32–4000**). |
| `OMS_FIX_ROUTE_STATE_SOD_ENABLED` | `false` | When **`true`**, cron job sets `fix_route_state.send_enabled=true` on all rows (`FixRouteStateSodScheduler`). |
| `OMS_FIX_ROUTE_STATE_SOD_CRON` | `0 0 6 * * *` | Spring six-field cron for SOD reconciliation (only when SOD enabled). |
| `OMS_FIX_ROUTE_STATE_SOD_POLICY_MODE` | `always` | **`always`** — every cron tick runs SOD (legacy). **`weekdays`** — Mon–Fri only in **`OMS_FIX_ROUTE_STATE_SOD_POLICY_ZONE_ID`**. **`region_calendar`** — JSON **`OMS_FIX_ROUTE_STATE_SOD_POLICY_CALENDAR_JSON`** (`FixSodPolicyEngine`). Skips increment **`oms_fix_route_state_sod_skipped_total`** with tag **`reason`**. |
| `OMS_FIX_ROUTE_STATE_SOD_POLICY_ZONE_ID` | `UTC` | IANA zone id for weekday / region-calendar date evaluation. |
| `OMS_FIX_ROUTE_STATE_SOD_POLICY_CALENDAR_JSON` | `{}` | When mode is **`region_calendar`**: `activeRegionId` + `regions` map; each region may set **`allowedWeekdays`** (ISO **1=Mon** … **7=Sun**), **`blockedDates`**, **`forcedDates`** (`YYYY-MM-DD`). |
| `OMS_FIX_SOCKET_USE_SSL` | `false` | QuickFIX **`SocketUseSSL`** for initiator TLS to broker. |
| `OMS_FIX_SOCKET_KEY_STORE` | (empty) | Keystore path when TLS enabled. |
| `OMS_FIX_SOCKET_KEY_STORE_PASSWORD` | (empty) | Keystore password. |
| `OMS_FIX_SOCKET_TRUST_STORE` | (empty) | Truststore path when TLS enabled. |
| `OMS_FIX_SOCKET_TRUST_STORE_PASSWORD` | (empty) | Truststore password. |
| `OMS_FIX_ENABLED_SSL_PROTOCOLS` | (empty) | Optional; passed to QuickFIX **`EnabledProtocols`** (e.g. `TLSv1.2`). |

## Outbox / reconciler

| Key                                   | Default | Meaning                                                                            |
|---------------------------------------|---------|------------------------------------------------------------------------------------|
| `OMS_OUTBOX_RECONCILER_AGE_MS`        | `2000`  | Minimum age before a control outbox row is eligible for Chronicle append (avoids races).  |
| `OMS_OUTBOX_RECONCILER_BATCH_SIZE`    | `100`   | Rows fetched per reconciler tick.                                                  |
| `OMS_OUTBOX_RECONCILER_INTERVAL_MS`   | `500`   | Scheduled-task interval. Drives `oms_control_outbox_appended_total` cadence.       |

## Domain fanout outbox

| Key                                      | Default | Meaning                                                                 |
|------------------------------------------|---------|-------------------------------------------------------------------------|
| `OMS_DOMAIN_EVENTS_RECONCILER_AGE_MS`    | `2000`  | Minimum age before a `domain_event_outbox` row is eligible for NATS delivery. |
| `OMS_DOMAIN_EVENTS_RECONCILER_BATCH_SIZE` | `100` | Rows fetched per `DomainFanoutReconciler` tick.                         |
| `OMS_DOMAIN_EVENTS_RECONCILER_INTERVAL_MS` | `500` | `@Scheduled` fixed delay for domain fanout drain.                       |

## Chronicle

| Key                                   | Default            | Meaning                                                                 |
|---------------------------------------|--------------------|-------------------------------------------------------------------------|
| `OMS_CHRONICLE_ENABLED`               | `true`             | When `false`, no `ChronicleQueue` bean; `NoOpControlJournal` is used.    |
| `OMS_CHRONICLE_QUEUE_DIR`             | `./queues/control` | Directory for Chronicle Queue files.                                    |
| `OMS_CHRONICLE_ROLL_CYCLE`            | `DAILY`            | `MINUTELY` / `HOURLY` / `DAILY` use `LegacyRollCycles`. Also: `FAST_DAILY`, `DEFAULT`. |
| `OMS_CHRONICLE_TAIL_POLL_INTERVAL_MS` | `50`             | Delay between scheduled `ChronicleControlTailReader` polls (ms).        |
| `OMS_CHRONICLE_TAIL_BATCH_MAX_MESSAGES` | `200`          | Max excerpts read per poll tick.                                        |

## NATS (fanout)

| Key                          | Default                | Meaning                                                                   |
|------------------------------|------------------------|---------------------------------------------------------------------------|
| `OMS_NATS_ENABLED`           | `false`                | When true, wires `NatsFanoutClient` (JetStream) instead of the no-op `FanoutClient`. |
| `OMS_NATS_URL`               | `nats://localhost:4222`| NATS connection URL.                                                      |
| `OMS_NATS_SUBJECT_PREFIX`    | `oms.events`           | Subject prefix; events publish to `${prefix}.${type}`.                    |
| `OMS_NATS_STREAM_NAME`       | `OMS_EVENTS`           | JetStream stream name created on startup (idempotent if it exists).       |
| `OMS_NATS_CONNECTION_TIMEOUT_MS` | `5000`            | NATS TCP connect timeout.                                                 |

## Ledger (buying power)

| Key                              | Default                 | Meaning                                                                 |
|----------------------------------|-------------------------|-------------------------------------------------------------------------|
| `OMS_LEDGER_ENABLED`             | `false`                 | When true, wires `RestLedgerBalanceClient` and enables the BUY gate in `ControlTailer`. |
| `OMS_LEDGER_BASE_URL`          | `http://localhost:5001` | Ledger HTTP root (routes mounted at `/`, e.g. `/balances/{id}`).        |
| `OMS_LEDGER_API_KEY`           | (empty)                 | Value for `X-Ledger-Key` on balance reads. Required when enabled.         |
| `OMS_LEDGER_CONNECT_TIMEOUT_MS`  | `2000`                  | HTTP connect timeout for Ledger calls.                                  |
| `OMS_LEDGER_READ_TIMEOUT_MS`     | `5000`                  | HTTP read timeout for Ledger calls.                                      |
| `OMS_LEDGER_INFLIGHT_RESERVATION_ENABLED` | `false`          | When true, OMS places a Ledger BUY inflight hold at order accept (idempotent `reference` `oms:order:{uuid}`). Requires `OMS_LEDGER_INFLIGHT_HOLD_DESTINATION_BALANCE_ID`. |
| `OMS_LEDGER_INFLIGHT_ASYNC_ENABLED` | `false` | When **true** (and inflight reservation enabled), the hold is **written to `ledger_inflight_outbox` in the same Postgres transaction** as the order; `LedgerInflightOutboxReconciler` calls Ledger **after** commit. When **false**, the Ledger HTTP call runs **synchronously inside** the accept transaction (default). |
| `OMS_LEDGER_INFLIGHT_OUTBOX_RECONCILER_AGE_MS` | `2000` | Only process outbox rows with `created_at` at least this old (avoids racing uncommitted readers). |
| `OMS_LEDGER_INFLIGHT_OUTBOX_RECONCILER_BATCH_SIZE` | `50` | Max rows per reconciler tick. |
| `OMS_LEDGER_INFLIGHT_OUTBOX_RECONCILER_INTERVAL_MS` | `500` | `fixedDelay` between reconciler runs (Spring `@Scheduled`). |
| `OMS_LEDGER_INFLIGHT_HOLD_DESTINATION_BALANCE_ID` | (empty) | Ledger `balance_id` for the hold leg (bank suspense / OMS hold account). Required when inflight reservation is enabled. |
| `OMS_LEDGER_INFLIGHT_RESERVATION_CURRENCY` | `EUR`        | ISO currency code for the inflight hold `amount`.                        |
| `OMS_LEDGER_INFLIGHT_RESERVATION_PRECISION` | `100`       | Ledger amount scaling (e.g. 100 = cents).                                |
| `OMS_LEDGER_SETTLEMENT_OUTBOX_ENABLED` | `false` | When **`true`**, OMS inserts **`ledger_settlement_outbox`** in the same transaction as **`settled`** on **`TRADE`** executions. |
| `OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED` | `false` | When **`true`** (requires **`OMS_LEDGER_ENABLED=true`**), drains **`ledger_settlement_outbox`** to Ledger HTTP and sets **`posted_at`** on success. |
| `OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_AGE_MS` | `2000` | Only rows with **`created_at`** at least this old are eligible (avoids racing uncommitted writers). |
| `OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_BATCH_SIZE` | `50` | Max rows locked per reconciler tick. |
| `OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_INTERVAL_MS` | `500` | **`@Scheduled`** fixed delay between reconciler runs. |
| `OMS_LEDGER_SETTLEMENT_POSTING_HTTP_PATH` | `/internal/v0/settlement-outbox` | Path appended to **`OMS_LEDGER_BASE_URL`** for settlement outbox **`POST`** (Ledger must expose a compatible handler). |

## Observability (Micrometer)

These metrics are always registered when the corresponding code paths run; they are **not** separate env toggles. Use them for best-ex / apply latency and Ledger settlement post evidence (histograms include service-level percentiles in Prometheus scrape configs).

| Meter name | Type | When it records | Related config |
|------------|------|------------------|----------------|
| `oms_control_decisions_recorded_total` | `Counter` (tags **`outcome`**, **`reject_code`**) | After each successful **`INSERT`** into **`control_decisions`** via **`ControlDecisionsRepository.record`** (one per control PASS/REJECT audit row). **`reject_code`** tag is **`NONE`** when the persisted reject code is SQL `NULL` (PASS outcomes). | N/A — see [risk-checks.md](risk-checks.md). |
| `oms.trade.apply` | `Timer` | Each successful **`ExecutionReportApplier.applyTrade`** after pre-checks through DB + market-context merge + execution insert + position update + order CAS + domain outbox (trade ER apply / best-ex evidence path). | **`OMS_ROUTING_NBBO_REFERENCE_IN_MARKET_CONTEXT_ENABLED`**, **`OMS_MARKETDATA_HTTP_*`** — wider path when NBBO evidence is merged. |
| `oms.marketdata.nbbo.fetch` | `Timer` (`tag` **`source=http`**) | HTTP NBBO fetch inside **`resolveNbbo`** when **`OMS_MARKETDATA_HTTP_NBBO_IN_MARKET_CONTEXT_ENABLED`** and **`OMS_MARKETDATA_HTTP_ENABLED`** are **`true`**. | **`OMS_MARKETDATA_HTTP_BASE_URL`**, timeouts, **`OMS_MARKETDATA_HTTP_NBBO_PATH`**. |
| `oms.ledger.settlement.outbox.post` | `Timer` | Per-row **`LedgerSettlementPostingClient.postSettlementOutbox`** plus **`markPosted`** when **`OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED`** is **`true`** (success path only; failures increment **`oms_ledger_settlement_outbox_failed_total`**). | **`OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_*`**, **`OMS_LEDGER_SETTLEMENT_OUTBOX_ENABLED`**. |

**Clock skew:** there is no `oms_clock_offset_ms` gauge in OMS yet; compare venue **`SendingTime`** / ER timestamps to server clock in logs or derive offset in your metrics stack if you need best-ex wall-clock budgets.

## PII

| Key                              | Default              | Meaning                                                                                  |
|----------------------------------|----------------------|------------------------------------------------------------------------------------------|
| `OMS_PII_AUDIT_TRACE_ENABLED`    | `false`              | When true, audit-grade trace logs MAY include raw `account_id`. Off in dev/prod.         |
| `OMS_PII_HASH_SECRET`            | `dev-only-change-me` | Seeds `xxh64` for `account_id` hashing. Must be rotated and managed in deploy tooling.   |
