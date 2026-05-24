# FIX-in counterparty conformance pack

Operator and certification checklist for external FIX 4.4 clients connecting to **`oms-fix-ingress`**. Run automated wire checks with `./gradlew test --tests 'com.balh.oms.fixin.FixInFullRoundTripIT'` and the soak script below.

## Prerequisites

| Item | Expected |
|------|----------|
| Flyway | Through **V67** (`oms_fix_in_*`, `oms_fix_message_audit`) |
| Session store | **JDBC** in bench/prod (`OMS_FIX_IN_SESSION_STORE_TYPE=jdbc`, Flyway **V9** tables) |
| Seed | At least one `oms_fix_in_session` + `oms_fix_in_account_binding` row |
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
| 6 | **Duplicate ClOrdID** | Idempotent reject or duplicate handling per `(session_id, ClOrdID)` | Manual |
| 7 | **Drop copy session** | `session_mode=DROP_COPY` rejects D/F/G with `BusinessMessageReject` | Manual + seed |
| 8 | **Sequence reset** | Audited row in `oms_fix_session_admin_actions`; session logged out first | Ops Console / API |
| 9 | Forced logout | Session drops; client must re-logon | Ops Console / soak (`FIX_SOAK_MUTATE=1`), `FixInMutatingSoakIT` (logout + audit) |
| 10 | **JDBC store / resend** | Rows in `oms_fix_sessions` + `oms_fix_messages`; seq recovery after acceptor restart | `FixInJdbcSessionStoreIT`, soak §3 |
| 11 | **Rate limit** | Stale `SendingTime` or burst → `BusinessMessageReject` | Config + manual |
| 12 | **Message audit** | `oms_fix_message_audit` rows; redacted fetch via admin API | Soak §2 |

## Message expectations (order entry)

- **Inbound:** FIX 4.4 `D` / `F` / `G` with required tags per `FixInNewOrderSingleParser`.
- **Admission:** OMS returns synchronous `8` with `ExecType=NEW` or `REJECTED` before broker ack.
- **Returns:** Fills/cancels via `OmsFixInReturnService` replay (no duplicate NEW when FIX-in metadata on admit).
- **OrderID(37):** OMS order UUID string.
- **ClOrdID(11):** Client-supplied; dedupe key `(session_id, ClOrdID)`.

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
