# FIX broker UAT soak (operational)

This is a **human-run** checklist for validating the OMS QuickFIX/J initiator against a broker **UAT** or paper environment after config changes (TLS, comp IDs, store type, seq). Automated soak (multi-hour traffic replay) is **not** implemented in CI; use this in change windows before production.

## Preconditions

- `OMS_ROUTING_BACKEND=fix`, `OMS_FIX_AUTO_START=true` in the soak environment only.
- `OMS_INTERNAL_API_KEY` set; route gate defaults to send enabled (`fix_route_state`) unless you intentionally halt.
- Broker UAT host/port, comp IDs, and **TLS** material (`OMS_FIX_SOCKET_USE_SSL`, keystores/truststores) match the venue worksheet.
- **Session store:** `OMS_FIX_SESSION_STORE_TYPE=file` or `jdbc`. If `jdbc`, Flyway **V9** has been applied (`oms_fix_sessions` / `oms_fix_messages`) on the database QuickFIX will use (application DB by default, or the DB behind **`OMS_FIX_SESSION_JDBC_URL`** when **`OMS_FIX_SESSION_JDBC_DATASOURCE_ENABLED=true`**). **Optional isolation:** second Postgres — [fix-session-store-isolation.md](fix-session-store-isolation.md).

## Soak procedure (minimal)

1. **Start OMS** with UAT config; confirm **logon** (acceptor/gateway logs + OMS logs: initiator started).
2. **Place a small limit** order via internal ingress; confirm **NOS** leaves OMS (broker audit) and **ExecutionReport** returns; order reaches **FILLED** or expected terminal state in Postgres.
3. **Halt route** with `PATCH /internal/v1/fix/route-state/{routeKey}` (`sendEnabled: false`); confirm **no further NOS** while halting; queue depth stable (orders stay **WORKING** until re-enabled).
4. **Re-enable** send; confirm a queued order **drains** and completes.
5. **Restart OMS** once; confirm **seq recovery** matches broker (no duplicate `MsgSeqNum` / no unexpected reset). If using **JDBC store**, verify rows exist in `oms_fix_sessions` for the session key.
6. **Optional SOD:** if `OMS_FIX_ROUTE_STATE_SOD_ENABLED=true`, confirm cron and `oms_fix_route_state_sod_reconciliations_total` align with ops expectation (typically re-opens send at start of day).

## Evidence to capture

- Prometheus scrape: `oms_fix_nos_sent_total`, `oms_fix_inbound_execution_reports_total`, `oms_fix_outbound_route_disabled_skips_total`, `oms_fix_outbound_throttled_requeues_total`, `oms_fix_route_state_sod_reconciliations_total`.
- Broker UAT session logs for the same window (seq, rejects, disconnects).

## Exit criteria

- No unexplained **disconnect loops**; heartbeats stable for the soak window.
- No **unauthorized** or **seq** rejects attributable to OMS misconfiguration.
- **DR note:** after restoring Postgres or the FIX JDBC store, coordinate **seq reset** with the broker before resuming send (see master plan 6.4 / 14.6).
