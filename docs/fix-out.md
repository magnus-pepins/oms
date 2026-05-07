# FIX outbound and inbound (slice 4+)

This document will hold session config, `NewOrderSingle` / `ExecutionReport` field maps,
environment variables, and runbooks as **QuickFIX/J** is wired to `FixRouteDispatcher` and
`ExecutionReportApplier`.

## Current state (slice 4 start)

- **Dependencies:** `org.quickfixj:quickfixj-core` + `quickfixj-messages-fix44` (see `build.gradle.kts`).
- **Smoke test:** `FixLogonSmokeTest` — embedded acceptor + initiator logon on a loopback port.
- **Outbound queue:** `FixRouteDispatcher` implements `RouteDispatcher` when `OMS_ROUTING_BACKEND=fix`
  and enqueues `WORKING` order ids; **Initiator send + inbound `fromApp` → applier** is the next wiring step.

## Configuration

| Key | Meaning |
|-----|---------|
| `OMS_ROUTING_BACKEND` | Set to **`fix`** to enable `FixRouteDispatcher` (default remains **`noop`** / **`simulated`** for labs). |

Further `OMS_FIX_*` keys will appear here as session store, data dictionary, and broker endpoints land.
