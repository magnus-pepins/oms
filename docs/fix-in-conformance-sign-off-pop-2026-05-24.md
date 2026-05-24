# FIX-in pop UAT certification sign-off (automated baseline)

**Environment:** pop (bench)  
**Date:** 2026-05-24 (updated after conformance probe land)  
**Order-entry session:** `00000001-0000-4000-8000-000000000001` — `LOOPBACK_CLIENT` → `BALH_OMS`  
**Drop-copy session:** `00000002-0000-4000-8000-000000000002` — `LOOPBACK_DROP` → `BALH_OMS`  
**Acceptor:** `oms-fix-ingress` port **9877** (internal admin **8095**)

This sign-off records **automated** evidence from deploy smoke, Gradle wire ITs (dev/CI), and the **live-acceptor conformance probe** (`fixInLoopbackConformanceProbe`). External production counterparties still require a separate live sign-off below.

## Automated results

| # | Scenario | Evidence | Result |
|---|----------|----------|--------|
| 1 | Logon | `fix-in-uat-soak.sh` acceptor_port + list_sessions | PASS |
| 2 | New order (35=D) | `FixInFullRoundTripIT` — sync NEW on wire | PASS (Gradle, dev/CI) |
| 3 | Broker fill round trip | `FixInFullRoundTripIT` — partial fill on FIX-in wire | PASS (Gradle, dev/CI) |
| 4 | Cancel (35=F) | `FixInCancelReplaceRoundTripIT` cancel after partial fill | PASS (Gradle, dev/CI) |
| 5 | Replace (35=G) | `FixInCancelReplaceRoundTripIT` replace on wire | PASS (Gradle, dev/CI) |
| 6 | Duplicate ClOrdID | `fixInLoopbackConformanceProbe` — same OrderID on duplicate NEW | **PASS** (pop, 2026-05-24) |
| 7 | Drop copy session | `fixInLoopbackConformanceProbe` — BMR on `35=D` for `LOOPBACK_DROP` | **PASS** (pop; requires seed + fix-ingress restart) |
| 8 | Sequence reset | `fixInLoopbackConformanceProbe` — audited `SEQUENCE_RESET` + re-logon | **PASS** (pop) |
| 9 | Forced logout + reconnect | `FixInMutatingSoakIT`; soak `FIX_SOAK_MUTATE=1` + `FIX_SOAK_REQUIRE_RECONNECT=1` + PM2 loopback client | **PASS** (pop) |
| 10 | JDBC store / resend | `FixInJdbcSessionStoreIT` + soak `jdbc_store_config` | PASS |
| 11 | Rate limit / stale time | `fixInLoopbackConformanceProbe` — session Reject for stale `SendingTime` | **PASS** (pop) |
| 12 | Message audit | soak `message_audit` probe | PASS (pop) |

## Pop soak commands (reference)

```bash
source ~/.oms-bench.env
export OMS_FIX_INGRESS_INTERNAL_BASE_URL=http://127.0.0.1:8095
export OMS_FIX_IN_SESSION_STORE_TYPE=jdbc
export OMS_INTERNAL_API_KEY="${OMS_INTERNAL_API_KEY:-$OMS_INTERNAL_KEY}"
export OMS_REPO=~/oms

# Read-only baseline
bash system-documentation/scripts/smoke/fix-in-uat-soak.sh

# Mutating probes (logout + reconnect + audit) — needs oms-fix-in-loopback-client PM2
FIX_SOAK_MUTATE=1 FIX_SOAK_REQUIRE_RECONNECT=1 \
FIX_IN_SESSION_ID=00000001-0000-4000-8000-000000000001 \
  bash system-documentation/scripts/smoke/fix-in-uat-soak.sh

# Conformance scenarios 6/7/8/11 + optional Gradle hook (wire ITs opt-in)
FIX_SOAK_MUTATE=1 FIX_SOAK_REQUIRE_RECONNECT=1 \
FIX_SOAK_CONFORMANCE=1 FIX_SOAK_RUN_GRADLE=1 \
FIX_IN_SESSION_ID=00000001-0000-4000-8000-000000000001 \
  bash system-documentation/scripts/smoke/fix-in-uat-soak.sh

# Conformance probe alone (stop PM2 loopback client first)
pm2 stop oms-fix-in-loopback-client
cd ~/oms && ./gradlew fixInLoopbackConformanceProbe
pm2 start oms-fix-in-loopback-client

# Spring wire ITs on pop — only with isolated test Postgres, NOT live OMS DB
FIX_SOAK_RUN_GRADLE=1 FIX_SOAK_GRADLE_WIRE_IT=1 OMS_CI_JDBC_URL=... \
  bash system-documentation/scripts/smoke/fix-in-uat-soak.sh
```

Drop-copy session setup: [runbooks/oms-fix-ingress.md](./runbooks/oms-fix-ingress.md) § Drop copy session.

## Internal loopback UAT status

**Signed off for internal bench UAT** — scenarios 1–12 covered by automated soak + conformance probe + Gradle ITs (ITs on dev/CI). Pop admission probes may cause transient `oms-cluster-reconcile` drift (`open_orders`); not a FIX-in wire failure.

## External counterparty live certification

**Status:** **Not signed off.** Repeat scenarios 1–12 with the external counterparty CompIDs and operator witness. Use the blank template below.

## Operator sign-off (live counterparty — blank)

```
Counterparty: _______________  Environment: production / staging  Date: _______________
Session UUID: _______________  CompIDs: _______________ → _______________

[ ] 1 Logon/logout (live)
[ ] 2 New order NEW ack (live)
[ ] 3 Fill return on FIX-in wire (live)
[ ] 4 Cancel (live)
[ ] 5 Replace (live)
[ ] 6 Duplicate ClOrdID
[ ] 7 Drop copy negative (if entitled)
[ ] 8 Sequence reset (audited, live)
[ ] 9 Acceptor restart + seq recovery (JDBC, live)
[ ] 10 Cluster restart — no lost admits

Signed: _______________
```
