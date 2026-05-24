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

## Loopback counterparty (bench / pop UAT)

Seed session `LOOPBACK_CLIENT` → `BALH_OMS` (`src/test/resources/db/fix-in-uat-seed.sql`). Run a persistent initiator so mutating soak can exercise logout + reconnect:

```bash
./gradlew fixInLoopbackClient
# or PM2: oms-fix-in-loopback-client (ecosystem.config.cjs)
```

Env: `FIX_IN_CLIENT_CONNECT_HOST`, `FIX_IN_CLIENT_CONNECT_PORT` (default **9877**), `FIX_IN_CLIENT_SENDER`, `FIX_IN_CLIENT_TARGET`, `FIX_IN_CLIENT_FILE_STORE`.

Then on pop:

```bash
FIX_SOAK_MUTATE=1 FIX_SOAK_REQUIRE_RECONNECT=1 FIX_IN_SESSION_ID=00000001-0000-4000-8000-000000000001 \
  bash system-documentation/scripts/smoke/fix-in-uat-soak.sh
```

## Session store (JDBC)

Production/bench default: `OMS_FIX_IN_SESSION_STORE_TYPE=jdbc` (uses application Postgres + Flyway **V9** `oms_fix_sessions` / `oms_fix_messages`). Local dev may use `file`. Optional isolation: `OMS_FIX_IN_SESSION_JDBC_DATASOURCE_ENABLED=true` + dedicated URL — same pattern as FIX-out ([fix-session-store-isolation.md](../fix-session-store-isolation.md)).

## Sequence reset guardrail

Session must be **logged out** before `sequence-reset` mutates QuickFIX store seq nums. Ops workflow: disable → logout → reset → smoke logon.

## Return publisher

`oms.fix-in.return-publisher.enabled=true` (default). Uses Aeron replay stream id `4324` (configurable). Cursor table: `oms_fix_in_return_cursor`.

## Drop copy

`session_mode=DROP_COPY` sessions reject `D/F/G` with `BusinessMessageReject`. Outbound copies use `oms_fix_drop_copy_entitlement` per `oms_account_id`.
