# FIX session store on a separate Postgres (Slice 4 / plan §6.4)

QuickFIX/J **`JdbcStoreFactory`** needs **`oms_fix_sessions`** and **`oms_fix_messages`** (Flyway **V9**). For **operational isolation** from the orders SoR, run those tables on a **second** Postgres and point OMS at it via the dedicated pool envs.

## Local: Compose second Postgres

From the `oms/` repo:

```bash
docker compose --profile with-fix-session-db up -d postgres-fix-sessions
```

- **JDBC URL:** `jdbc:postgresql://localhost:5433/oms_fix_store`
- **Credentials:** `oms_fix` / `oms_fix` (see `docker-compose.yml`)

## Apply DDL on the FIX store database

The dedicated DB starts **empty**. Apply **only** the FIX store tables (same DDL as Flyway **V9**):

```bash
psql "postgresql://oms_fix:oms_fix@localhost:5433/oms_fix_store" \
  -f src/main/resources/db/migration/V9__oms_fix_session_store.sql
```

(Re-run is safe: `CREATE TABLE IF NOT EXISTS`.)

## OMS env (application process)

```bash
OMS_FIX_SESSION_STORE_TYPE=jdbc
OMS_FIX_SESSION_JDBC_DATASOURCE_ENABLED=true
OMS_FIX_SESSION_JDBC_URL=jdbc:postgresql://localhost:5433/oms_fix_store
OMS_FIX_SESSION_JDBC_USER=oms_fix
OMS_FIX_SESSION_JDBC_PASSWORD=oms_fix
# Optional tuning — see docs/configuration.md
```

Leave **`spring.datasource`** pointing at the **primary** OMS Postgres (orders, `fix_route_state`, etc.). QuickFIX uses the **second** pool only for session/messages.

## Production

Use a managed Postgres **instance** or **database** dedicated to FIX persistence; run the same **V9** DDL via your migration tool; coordinate **seq reset** with the broker after restore (see [fix-broker-uat-soak.md](fix-broker-uat-soak.md)).

## Evidence

Integration proof: **`FixRoundTripJdbcDedicatedSessionPoolSpringIntegrationTest`** (dedicated Hikari pool against Postgres).
