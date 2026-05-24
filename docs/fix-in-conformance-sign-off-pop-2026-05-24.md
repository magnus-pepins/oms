# FIX-in pop UAT certification sign-off (automated baseline)

**Environment:** pop (bench)  
**Date:** 2026-05-24  
**Session UUID:** `00000001-0000-4000-8000-000000000001`  
**CompIDs:** `LOOPBACK_CLIENT` → `BALH_OMS`  
**Acceptor:** `oms-fix-ingress` port **9877** (internal admin **8095**)

This sign-off records **automated** evidence from deploy smoke and Gradle wire ITs. Scenarios marked **manual** still require a live external counterparty before production certification.

## Automated results

| # | Scenario | Evidence | Result |
|---|----------|----------|--------|
| 1 | Logon | `fix-in-uat-soak.sh` acceptor_port + list_sessions (2026-05-24 deploy) | PASS |
| 2 | New order (35=D) | `FixInFullRoundTripIT` — sync NEW on wire | PASS (Gradle) |
| 3 | Broker fill round trip | `FixInFullRoundTripIT` — partial fill on FIX-in wire | PASS (Gradle + pop `FIX_SOAK_RUN_GRADLE=1`) |
| 4 | Cancel (35=F) | `FixInCancelReplaceRoundTripIT.fixInClientCancel_afterPartialFillReturnsCanceledOnFixInWire` | PASS (Gradle) |
| 5 | Replace (35=G) | `FixInCancelReplaceRoundTripIT.fixInClientReplace_returnsReplacedOnFixInWire` | PASS (Gradle) |
| 6 | Duplicate ClOrdID | — | **Manual** — not in automated pack |
| 7 | Drop copy session | — | **Manual** — seed + negative test |
| 8 | Sequence reset | Admin API exists; audited in `oms_fix_session_admin_actions` | **Manual** on live counterparty |
| 9 | Forced logout + reconnect | `FixInMutatingSoakIT` (logout + audit); pop soak with `FIX_SOAK_MUTATE=1` (+ optional `FIX_SOAK_REQUIRE_RECONNECT=1` when LOOPBACK_CLIENT live) | PASS (Gradle logout); pop logout POST **PASS** (2026-05-24); reconnect **not observed** without live initiator |
| 10 | JDBC store / resend | `FixInJdbcSessionStoreIT` + soak jdbc_store_config | PASS |
| 11 | Rate limit | — | **Manual** |
| 12 | Message audit | soak message_audit probe (CAST fix deployed) | PASS (pop) |

## Pop soak commands (reference)

```bash
source ~/.oms-bench.env
export OMS_FIX_INGRESS_INTERNAL_BASE_URL=http://127.0.0.1:8095
export OMS_FIX_IN_SESSION_STORE_TYPE=jdbc
export OMS_INTERNAL_API_KEY="${OMS_INTERNAL_API_KEY:-$OMS_INTERNAL_KEY}"

# Read-only baseline
bash system-documentation/scripts/smoke/fix-in-uat-soak.sh

# Mutating probes (logout + runtime drop; reconnect when LOOPBACK_CLIENT initiator is connected)
export FIX_SOAK_MUTATE=1
export FIX_IN_SESSION_ID=00000001-0000-4000-8000-000000000001
# export FIX_SOAK_REQUIRE_RECONNECT=1   # fail if no re-logon within 90s
bash system-documentation/scripts/smoke/fix-in-uat-soak.sh

# Full wire IT suite from pop host (requires OMS repo + Postgres test infra — usually run on dev Mac)
export FIX_SOAK_RUN_GRADLE=1
export OMS_REPO=~/oms
bash system-documentation/scripts/smoke/fix-in-uat-soak.sh
```

## External counterparty live certification

**Status:** **Not signed off.** Conformance pack ([fix-in-conformance-pack.md](./fix-in-conformance-pack.md)) scenarios 6–8 and 11 remain operator/counterparty manual steps. Automated Gradle + soak evidence above is sufficient for **internal loopback UAT** only.

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
