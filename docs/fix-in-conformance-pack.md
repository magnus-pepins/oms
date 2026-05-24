# FIX-in counterparty conformance pack

Operator and certification checklist for external FIX 4.4 clients connecting to **`oms-fix-ingress`**.

**Automation entrypoints:**

- Gradle wire ITs (dev Mac / CI): `./gradlew test --tests 'com.balh.oms.fixin.FixInFullRoundTripIT'` (and cancel/replace / JDBC / mutating soak ITs).
- Live acceptor probe (bench/pop): `./gradlew fixInLoopbackConformanceProbe` — scenarios **6, 7, 8, 11**.
- Soak + probe: `system-documentation/scripts/smoke/fix-in-uat-soak.sh` with `FIX_SOAK_CONFORMANCE=1` and/or `FIX_SOAK_RUN_GRADLE=1`.

Runbook (loopback client, drop copy, pop commands): [runbooks/oms-fix-ingress.md](./runbooks/oms-fix-ingress.md).

## Prerequisites

| Item | Expected |
|------|----------|
| Flyway | Through **V67** (`oms_fix_in_*`, `oms_fix_message_audit`, `oms_fix_drop_copy_entitlement`) |
| Session store | **JDBC** in bench/prod (`OMS_FIX_IN_SESSION_STORE_TYPE=jdbc`, Flyway **V9** tables) |
| Seed | Order-entry session + binding; drop-copy session for scenario 7 — see `src/test/resources/db/fix-in-uat-seed.sql` |
| CompIDs | Client `SenderCompID` / OMS `TargetCompID` match DB row |
| Cluster | Aeron cluster + `oms-fix-ingress` cluster client connected |

## Scenario matrix

| # | Scenario | Pass criteria | Automation |
|---|----------|---------------|------------|
| 1 | **Logon** | Session `loggedOn=true` in Ops Console; no `RejectLogon` | Soak §1, manual |
| 2 | **New order (35=D)** | Sync `ExecutionReport` `ExecType=0` (NEW); `oms_fix_in_order_map` row; cluster admit | `FixInClusterAdmissionIT`, `FixInFullRoundTripIT` |
| 3 | **Broker fill round trip** | FIX-in D → egress NOS → broker ER → FIX-in `PartialFill`/`Fill` on wire | `FixInFullRoundTripIT` |
| 4 | **Cancel (35=F)** | `OrderCancelReject` or canceled ER; cluster cancel command | `FixInCancelReplaceRoundTripIT` |
| 5 | **Replace (35=G)** | Replace ER or reject; cluster replace command | `FixInCancelReplaceRoundTripIT` |
| 6 | **Duplicate ClOrdID** | Idempotent reject or duplicate handling per `(session_id, ClOrdID)` | `fixInLoopbackConformanceProbe`, soak (`FIX_SOAK_CONFORMANCE=1`) |
| 7 | **Drop copy session** | `session_mode=DROP_COPY` rejects D/F/G with `BusinessMessageReject` | `fixInLoopbackConformanceProbe` + seed (`LOOPBACK_DROP`) |
| 8 | **Sequence reset** | Audited row in `oms_fix_session_admin_actions`; session logged out first | `fixInLoopbackConformanceProbe`, Ops Console / API |
| 9 | Forced logout | Session drops; client must re-logon | Ops Console / soak (`FIX_SOAK_MUTATE=1`), `FixInMutatingSoakIT` (logout + audit) |
| 10 | **JDBC store / resend** | Rows in `oms_fix_sessions` + `oms_fix_messages`; seq recovery after acceptor restart | `FixInJdbcSessionStoreIT`, soak §3 |
| 11 | **Rate limit** | Stale `SendingTime` or burst → `BusinessMessageReject` | `fixInLoopbackConformanceProbe` (stale `SendingTime`); burst requires lowered `oms.fix-in.max-app-messages-per-second` |
| 12 | **Message audit** | `oms_fix_message_audit` rows; redacted fetch via admin API | Soak §2 |

## Loopback conformance probe (scenarios 6, 7, 8, 11)

Gradle task: **`fixInLoopbackConformanceProbe`** (`FixInLoopbackConformanceProbeMain`).

**Before run:** stop PM2 `oms-fix-in-loopback-client` (or any initiator using `LOOPBACK_CLIENT` / `LOOPBACK_DROP`) to avoid CompID contention.

```bash
source ~/.oms-bench.env
export OMS_INTERNAL_API_KEY
export OMS_FIX_INGRESS_INTERNAL_BASE_URL=http://127.0.0.1:8095

pm2 stop oms-fix-in-loopback-client
./gradlew fixInLoopbackConformanceProbe
pm2 start oms-fix-in-loopback-client
```

| Scenario | Probe behaviour | Pass criteria |
|----------|-----------------|---------------|
| **6** | Two `35=D` with same `ClOrdID` on order-entry session | Two `ExecType=NEW` ERs, same `OrderID(37)` |
| **7** | `35=D` on `DROP_COPY` session (`LOOPBACK_DROP`) | `BusinessMessageReject` ref `D`, text contains `drop_copy_session_order_entry_forbidden` |
| **8** | HTTP force logout → `POST .../sequence-reset` → client re-logon | `SEQUENCE_RESET` in admin-actions; initiator logs on again |
| **11** | `35=D` with stale `SendingTime` (~125s) | `BusinessMessageReject message_too_old` **or** session `Reject` / logout for `SendingTime accuracy problem` (QuickFIX session guard) |

Report: `build/fix-in-conformance-probe-report.json` (env `FIX_IN_CONFORMANCE_REPORT_PATH`).

Integrated into soak when `FIX_SOAK_CONFORMANCE=1` or `FIX_SOAK_RUN_GRADLE=1` (see runbook). Soak also applies `fix-in-uat-seed.sql` and restarts `oms-fix-ingress` when a new session row is inserted.

## Drop copy (scenario 7 and production)

1. Insert `oms_fix_in_session` with `session_mode=DROP_COPY` (seed: `LOOPBACK_DROP` → `BALH_OMS`, UUID `00000002-…`).
2. Restart **`oms-fix-ingress`** so the acceptor registers the session.
3. Start client initiator with drop-copy CompIDs and **separate file store** (see runbook).
4. **Negative test (scenario 7):** inbound `D/F/G` → `BusinessMessageReject` — no entitlement required.
5. **Positive fanout (optional):** insert `oms_fix_drop_copy_entitlement` linking session to `oms_account_id`; orders on order-entry session for that account mirror ERs to drop copy via `FixInReturnPublisher`.

## Message expectations (order entry)

- **Inbound:** FIX 4.4 `D` / `F` / `G` with required tags per `FixInNewOrderSingleParser`.
- **Admission:** OMS returns synchronous `8` with `ExecType=NEW` or `REJECTED` before broker ack.
- **Returns:** Fills/cancels via `OmsFixInReturnService` replay (no duplicate NEW when FIX-in metadata on admit).
- **OrderID(37):** OMS order UUID string.
- **ClOrdID(11):** Client-supplied; dedupe key `(session_id, ClOrdID)`.

## External counterparty (non-loopback)

The conformance probe defaults assume the bundled loopback initiators (`LOOPBACK_CLIENT` for order
entry, `LOOPBACK_DROP` for drop copy, target `BALH_OMS`, port `9877`, session UUID
`00000001-0000-4000-8000-000000000001`). For a real counterparty (broker, algo desk) you must
override the matching env vars **and** seed the session row first.

| Env var | Default (loopback) | External counterparty |
|---------|--------------------|------------------------|
| `FIX_IN_CLIENT_CONNECT_HOST` | `127.0.0.1` | Acceptor host reachable from the **probe** runner |
| `FIX_IN_CLIENT_CONNECT_PORT` | `9877` (`OMS_FIX_IN_ACCEPT_PORT`) | Per-environment |
| `FIX_IN_CLIENT_SENDER` | `LOOPBACK_CLIENT` | Counterparty order-entry `SenderCompID` |
| `FIX_IN_DROP_COPY_SENDER` | `LOOPBACK_DROP` | Counterparty drop-copy `SenderCompID` (or omit + skip scenario 7) |
| `FIX_IN_CLIENT_TARGET` | `BALH_OMS` | OMS `TargetCompID` from the seeded row |
| `FIX_IN_ORDER_ENTRY_SESSION_ID` | `00000001-…0001` | UUID of the order-entry `oms_fix_in_session` row |
| `OMS_INTERNAL_API_KEY` | from `~/.oms-bench.env` | per environment |
| `OMS_FIX_INGRESS_INTERNAL_BASE_URL` | `http://127.0.0.1:8095` | Internal base URL of `oms-fix-ingress` for admin probes |
| `FIX_IN_PROBE_STALE_MS` | `125000` (just past QuickFIX's 120s session-layer guard) | Tune if counterparty uses different tolerance |

**Scope notes for external runs:**

- The probe sends real FIX messages and will produce an `oms_orders` row for scenario 6 (idempotent
  duplicate). Run only against a non-production OMS acceptor or after coordinating a maintenance
  window with the counterparty.
- Scenario 7 requires the counterparty to actually log on as the drop-copy `SenderCompID`. If they
  cannot, restrict the probe to scenarios 6, 8, 11 by editing `FixInLoopbackConformanceProbeMain`
  or running scenarios manually per the matrix above.
- Scenario 8 mutates state via the internal admin API (`force logout`, `sequence reset`). It does
  **not** require the operator to log a row in `oms_fix_session_admin_actions` — the API writes one
  with `requestedBy=conformance-probe`. Filter audit queries to exclude probe-origin rows when
  compiling counterparty sign-off evidence.

## Pop / bench soak flags

| Flag | Effect |
|------|--------|
| `FIX_SOAK_MUTATE=1` | Force logout, runtime drop, reconnect, admin-action audit (scenario 9) |
| `FIX_SOAK_REQUIRE_RECONNECT=1` | Fail if loopback client does not re-logon (needs PM2 `oms-fix-in-loopback-client`) |
| `FIX_SOAK_CONFORMANCE=1` | Apply UAT seed, run `fixInLoopbackConformanceProbe`, merge report |
| `FIX_SOAK_RUN_GRADLE=1` | Same conformance probe; wire ITs only if `FIX_SOAK_GRADLE_WIRE_IT=1` + isolated JDBC |
| `FIX_IN_SESSION_ID` | Order-entry session UUID (default: first enabled session) |

Pop sign-off evidence: [fix-in-conformance-sign-off-pop-2026-05-24.md](./fix-in-conformance-sign-off-pop-2026-05-24.md).

## Operator sign-off template

```
Counterparty: _______________  Environment: _______________  Date: _______________
Session UUID: _______________  CompIDs: _______________ → _______________

[ ] 1 Logon/logout
[ ] 2 New order NEW ack
[ ] 3 Fill return on FIX-in wire
[ ] 4 Cancel
[ ] 5 Replace
[ ] 6 Duplicate ClOrdID
[ ] 7 Drop copy negative (if entitled)
[ ] 8 Sequence reset (audited)
[ ] 9 Acceptor restart + seq recovery (JDBC)
[ ] 10 Cluster restart — no lost admits (soak §5)

Signed: _______________
```

## Related docs

- [oms-fix-ingress runbook](./runbooks/oms-fix-ingress.md)
- [mTLS and secrets](./runbooks/oms-fix-in-mtls-and-secrets.md)
- [FIX session store isolation](./fix-session-store-isolation.md)
- Soak: `system-documentation/scripts/smoke/fix-in-uat-soak.sh`
