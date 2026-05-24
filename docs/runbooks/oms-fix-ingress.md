# oms-fix-ingress runbook

QuickFIX/J **acceptor** role for external FIX 4.4 clients. Submits `AcceptOrderCommand` / lifecycle commands to the Aeron cluster and returns synchronous admission `ExecutionReport` (NEW/REJECTED). Fills and cancels are published by `OmsFixInReturnService` from the cluster events recording.

## Start locally

1. Cluster node + projector (or test stack) running with events recording.
2. Flyway through `V67` applied; seed a session (see `src/test/resources/db/fix-in-uat-seed.sql`).
3. `./gradlew bootRunFixIngress`
4. Point `OMS_FIX_INGRESS_CLUSTER_CLIENT_*` at the cluster media driver (same as ingress replica).

Default listen: `0.0.0.0:9877` (`OMS_FIX_IN_ACCEPT_PORT`). Client initiator targets `BALH_OMS` / `LOOPBACK_CLIENT` per seed.

## Profiles

- **Mutually exclusive** with `oms-ingress-replica`, `oms-postgres-projector`, `oms-fix-egress` (`TopologyWorkerProfiles`).
- Requires `oms.cluster.client.enabled=true`.

## Admin HTTP (Ops Console proxy)

| Endpoint | Purpose |
|----------|---------|
| `GET /internal/v1/fix-in/sessions` | Enriched list (config + runtime seq / loggedOn) |
| `GET /internal/v1/fix-in/sessions/{id}` | Single session detail |
| `PATCH /internal/v1/fix-in/sessions/{id}/enabled` | Enable/disable |
| `POST /internal/v1/fix-in/sessions/{id}/logout` | Force logout (audited) |
| `POST /internal/v1/fix-in/sessions/{id}/credential-rotation` | Staged password hash (audited) |
| `POST /internal/v1/fix-in/sessions/{id}/sequence-reset` | Audited seq reset (`oms_fix_session_admin_actions`) |
| `GET /internal/v1/fix-in/admin-actions` | Admin action history |
| `GET /internal/v1/fix-in/message-audit` | Redacted audit search |
| `GET /internal/v1/fix-in/message-audit/{id}` | Audit row + optional JDBC raw (redacted) |

Ops Console: `/api/oms-trading/fix/*` and `/api/oms-trading/fix-in/*`. Set `OMS_FIX_INGRESS_INTERNAL_BASE_URL` when fix-ingress is a separate JVM (default: `OMS_INTERNAL_BASE_URL`). RBAC: `fix_admin` or `admin` for mutating routes; see [oms-fix-in-mtls-and-secrets.md](./oms-fix-in-mtls-and-secrets.md).

UAT soak (read-only + JSON report): `system-documentation/scripts/smoke/fix-in-uat-soak.sh`. Conformance checklist: [fix-in-conformance-pack.md](../fix-in-conformance-pack.md).

## FIX-in session provisioning (operator)

External counterparties (non-loopback) use **`system-documentation/scripts/oms-fix-in-provision-session.sh`**:

```bash
source ~/.oms-bench.env
cd ~/system-documentation

# List all FIX-in sessions (ORDER_ENTRY + DROP_COPY)
bash scripts/oms-fix-in-provision-session.sh --list

# Order-entry session + account binding (required for D/F/G)
bash scripts/oms-fix-in-provision-session.sh --session-mode ORDER_ENTRY \
  --sender CLIENT_OE --counterparty-name "Client Legal Name" \
  --bind-account a0000001-0000-4000-8000-000000000001

bash scripts/oms-fix-in-provision-session.sh --commit --session-mode ORDER_ENTRY \
  --sender CLIENT_OE --counterparty-name "Client Legal Name" \
  --bind-account a0000001-0000-4000-8000-000000000001

# Extra account binding on an existing order-entry session
bash scripts/oms-fix-in-provision-session.sh --commit --bind-only \
  --session-id <uuid> --bind-account <oms_account_uuid> [--fix-account-tag TAG]
```

Dry-run by default; `--commit` applies SQL. Restart **`oms-fix-ingress`** after a **new** session row. Prod guard: `FIX_IN_SESSION_PROD_OVERRIDE=1`.

Legacy alias: `oms-fix-in-add-drop-copy-session.sh` (wrapper → `--session-mode DROP_COPY`).

## Loopback counterparty (bench / pop UAT)

Seed file: `src/test/resources/db/fix-in-uat-seed.sql` defines:

| Session UUID | Mode | CompIDs (client → OMS) | Purpose |
|--------------|------|------------------------|---------|
| `00000001-0000-4000-8000-000000000001` | `ORDER_ENTRY` | `LOOPBACK_CLIENT` → `BALH_OMS` | Orders, soak reconnect, conformance 6/8/11 |
| `00000002-0000-4000-8000-000000000002` | `DROP_COPY` | `LOOPBACK_DROP` → `BALH_OMS` | Drop-copy logon + conformance 7 |

Apply on bench (idempotent):

```bash
source ~/.oms-bench.env
pg_url="${OMS_PG_URL#jdbc:postgresql://}"
pg_url="${pg_url%%\?*}"
psql "postgresql://${OMS_PG_USER:-oms}:${OMS_PG_PASSWORD:-oms}@${pg_url}" \
  -v ON_ERROR_STOP=1 -f src/test/resources/db/fix-in-uat-seed.sql
```

If the seed inserts a **new** session row (`INSERT 0 1` in psql output), restart **`oms-fix-ingress`** — the acceptor loads enabled sessions at startup only:

```bash
pm2 restart oms-fix-ingress
```

### Order-entry loopback client (persistent)

Run a persistent initiator so mutating soak can exercise logout + reconnect:

```bash
./gradlew fixInLoopbackClient
# or PM2: oms-fix-in-loopback-client (ecosystem.config.cjs)
```

Env: `FIX_IN_CLIENT_CONNECT_HOST`, `FIX_IN_CLIENT_CONNECT_PORT` (default **9877**), `FIX_IN_CLIENT_SENDER` (default `LOOPBACK_CLIENT`), `FIX_IN_CLIENT_TARGET` (default `BALH_OMS`), `FIX_IN_CLIENT_FILE_STORE`.

### Conformance probe (scenarios 6, 7, 8, 11)

One-shot Gradle task against a **live** acceptor (not embedded Spring ITs):

```bash
source ~/.oms-bench.env
export OMS_INTERNAL_API_KEY OMS_FIX_INGRESS_INTERNAL_BASE_URL=http://127.0.0.1:8095

# Avoid CompID contention with PM2 loopback client on LOOPBACK_CLIENT
pm2 stop oms-fix-in-loopback-client

./gradlew fixInLoopbackConformanceProbe
# Report: build/fix-in-conformance-probe-report.json (override: FIX_IN_CONFORMANCE_REPORT_PATH)

pm2 start oms-fix-in-loopback-client
```

| Env | Default | Purpose |
|-----|---------|---------|
| `OMS_INTERNAL_API_KEY` | (required) | Admin API for scenario 8 (logout + sequence-reset) |
| `OMS_FIX_INGRESS_INTERNAL_BASE_URL` | `http://127.0.0.1:8095` | fix-ingress admin base |
| `FIX_IN_CLIENT_CONNECT_PORT` | `9877` | Acceptor port |
| `FIX_IN_ORDER_ENTRY_SESSION_ID` | `00000001-…0001` | Order-entry session for scenario 8 |
| `FIX_IN_DROP_COPY_SENDER` | `LOOPBACK_DROP` | Drop-copy CompID for scenario 7 |
| `FIX_IN_PROBE_STALE_MS` | `125000` | Stale `SendingTime` offset for scenario 11 |
| `FIX_IN_PROBE_FILE_STORE` | `./build/fix-in-conformance-probe` | Isolated QuickFIX file stores |

Scenario **11** on bench: QuickFIX may reject very stale `SendingTime` at the **session** layer (`35=3` / logout) before the app emits `BusinessMessageReject message_too_old`. The probe accepts either outcome.

Via soak (applies seed, stops/restarts PM2 loopback client, merges probe JSON into soak report):

```bash
FIX_SOAK_CONFORMANCE=1 FIX_SOAK_RUN_GRADLE=1 \
  bash system-documentation/scripts/smoke/fix-in-uat-soak.sh
```

`FIX_SOAK_RUN_GRADLE=1` runs the conformance probe by default. Spring wire ITs (`FixInFullRoundTripIT`, etc.) run only with **`FIX_SOAK_GRADLE_WIRE_IT=1`** and an **isolated** `OMS_CI_JDBC_URL` — do not point that at live pop Postgres (ITs truncate orders).

### Pop soak (full internal UAT)

```bash
source ~/.oms-bench.env
export OMS_FIX_INGRESS_INTERNAL_BASE_URL=http://127.0.0.1:8095
export OMS_FIX_IN_SESSION_STORE_TYPE=jdbc
export OMS_REPO=~/oms

FIX_SOAK_MUTATE=1 FIX_SOAK_REQUIRE_RECONNECT=1 \
FIX_SOAK_CONFORMANCE=1 FIX_SOAK_RUN_GRADLE=1 \
FIX_IN_SESSION_ID=00000001-0000-4000-8000-000000000001 \
  bash system-documentation/scripts/smoke/fix-in-uat-soak.sh
```

Requires `oms-fix-in-loopback-client` PM2 for reconnect probes. Conformance probe briefly stops that process to exclusive-use `LOOPBACK_CLIENT` / `LOOPBACK_DROP`.

## Drop copy session

Drop copy is a **separate** FIX-in session (`session_mode=DROP_COPY`), not a flag on order entry. The client is still a FIX 4.4 **initiator**; the acceptor rejects inbound order entry and may **fan out** eligible `ExecutionReport` / `OrderCancelReject` copies to entitled sessions.

### Provision (operator)

**Script (recommended):** `system-documentation/scripts/oms-fix-in-provision-session.sh --session-mode DROP_COPY` (or legacy wrapper `oms-fix-in-add-drop-copy-session.sh`):

```bash
source ~/.oms-bench.env

# List drop-copy sessions only
bash scripts/oms-fix-in-provision-session.sh --list --session-mode DROP_COPY

# Dry-run then apply (creates counterparty + session + optional entitlements)
bash scripts/oms-fix-in-provision-session.sh --session-mode DROP_COPY \
  --sender CLIENT_DC_SENDER \
  --counterparty-name "Client Legal Name" \
  --entitle-account a0000001-0000-4000-8000-000000000001

bash scripts/oms-fix-in-provision-session.sh --commit --session-mode DROP_COPY \
  --sender CLIENT_DC_SENDER \
  --counterparty-name "Client Legal Name" \
  --entitle-account a0000001-0000-4000-8000-000000000001

# Entitlements only (session already provisioned)
bash scripts/oms-fix-in-provision-session.sh --commit --entitle-only \
  --session-id <drop_copy_session_uuid> \
  --entitle-account <oms_account_uuid>
```

Dry-run by default; `--commit` applies SQL. The script prints `pm2 restart oms-fix-ingress` as the next step — **not** run automatically. Prod guard: `FIX_IN_SESSION_PROD_OVERRIDE=1` (legacy alias: `DROP_COPY_SESSION_PROD_OVERRIDE=1`).

**Manual SQL** (same tables):

1. **Counterparty** — reuse or insert `oms_fix_in_counterparty`.
2. **Session row** — `oms_fix_in_session` with `session_mode=DROP_COPY`, unique `(sender_comp_id, target_comp_id)`:

   ```sql
   INSERT INTO oms_fix_in_session (
       id, counterparty_id, environment, session_mode,
       sender_comp_id, target_comp_id, heartbeat_seconds, enabled)
   VALUES (
       '<uuid>', '<counterparty_uuid>', 'UAT', 'DROP_COPY',
       'CLIENT_DC_SENDER', 'BALH_OMS', 30, TRUE);
   ```

3. **Restart `oms-fix-ingress`** after inserting a new enabled session (acceptor reads DB at boot).
4. **Entitlements (optional, for outbound ER fanout)** — link drop-copy session to OMS accounts whose lifecycle events should be mirrored:

   ```sql
   INSERT INTO oms_fix_drop_copy_entitlement (drop_copy_session_id, oms_account_id)
   VALUES ('<drop_copy_session_uuid>', '<oms_account_uuid>')
   ON CONFLICT DO NOTHING;
   ```

   Without entitlements, the session can log on and reject `D/F/G` (conformance scenario 7) but **`FixInReturnPublisher` will not send ER copies** to it.

5. **No account binding** on drop-copy sessions — `oms_fix_in_account_binding` is order-entry only.

### Start the client (loopback example)

Use a **separate QuickFIX file store** from order entry (sequence state must not be shared):

```bash
FIX_IN_CLIENT_SENDER=LOOPBACK_DROP \
FIX_IN_CLIENT_TARGET=BALH_OMS \
FIX_IN_CLIENT_CONNECT_PORT=9877 \
FIX_IN_CLIENT_FILE_STORE=./build/fix-in-drop-copy-client \
  ./gradlew fixInLoopbackClient
```

External counterparties: same CompIDs as the DB row, connect to acceptor host/port, `ResetOnLogon=Y` (matches loopback seed).

### Verify

| Check | How |
|-------|-----|
| Logged on | `GET /internal/v1/fix-in/sessions/{drop_copy_session_id}` → `runtime.loggedOn=true` |
| Rejects order entry | Send `35=D` → `35=j` text `drop_copy_session_order_entry_forbidden` (scenario 7 / `./gradlew fixInLoopbackConformanceProbe`) |
| Receives ER fanout | Entitlement row present; place order on **order-entry** session for entitled account; drop-copy session receives fill/cancel ERs on wire |

Negative-only testing does not require entitlements. See [fix-in-conformance-pack.md](../fix-in-conformance-pack.md) scenario 7.

## Session store (JDBC)

Production/bench default: `OMS_FIX_IN_SESSION_STORE_TYPE=jdbc` (uses application Postgres + Flyway **V9** `oms_fix_sessions` / `oms_fix_messages`). Local dev may use `file`. Optional isolation: `OMS_FIX_IN_SESSION_JDBC_DATASOURCE_ENABLED=true` + dedicated URL — same pattern as FIX-out ([fix-session-store-isolation.md](../fix-session-store-isolation.md)).

## Sequence reset guardrail

Session must be **logged out** before `sequence-reset` mutates QuickFIX store seq nums. Ops workflow: disable → logout → reset → smoke logon.

## Return publisher

`oms.fix-in.return-publisher.enabled=true` (default). Uses Aeron replay stream id `4324` (configurable). Cursor table: `oms_fix_in_return_cursor`. Drop-copy fanout: `oms_fix_drop_copy_entitlement` + `FixInReturnPublisher` (see **Drop copy session** above).
