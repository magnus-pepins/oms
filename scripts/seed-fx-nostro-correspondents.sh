#!/usr/bin/env bash
#
# Seed fx_nostro_correspondent from the operator nostros reference table (V37).
# Idempotent: upserts on (currency, correspondent_code).
#
# Usage:
#   OMS_PG_PASSWORD=... ./scripts/seed-fx-nostro-correspondents.sh
#   source ~/.oms-bench.env && ./scripts/seed-fx-nostro-correspondents.sh
#
set -euo pipefail

PGHOST="${OMS_PG_HOST:-127.0.0.1}"
PGPORT="${OMS_PG_PORT:-5432}"
PGUSER="${OMS_PG_USER:-oms}"
PGDATABASE="${OMS_PG_DATABASE:-oms}"
PGPASSWORD="${OMS_PG_PASSWORD:-${1:-}}"

if [[ -z "${PGPASSWORD}" ]]; then
  echo "Set OMS_PG_PASSWORD or pass password as first argument" >&2
  exit 1
fi

export PGPASSWORD

echo "Syncing fx_nostro_correspondent from nostros (${PGHOST}:${PGPORT}/${PGDATABASE})..."

psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE}" -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO fx_nostro_correspondent (
    currency, correspondent_code, ledger_balance_id, priority, status, updated_at
)
SELECT
    upper(trim(currency)),
    trim(correspondent),
    trim(ledger_balance_id),
    1,
    CASE
        WHEN status IN ('active', 'demo') THEN 'active'
        WHEN status = 'frozen' THEN 'paused'
        ELSE 'drained'
    END,
    NOW()
FROM nostros
ON CONFLICT (currency, correspondent_code) DO UPDATE SET
    ledger_balance_id = EXCLUDED.ledger_balance_id,
    priority = EXCLUDED.priority,
    status = EXCLUDED.status,
    updated_at = NOW();
SQL

echo "Primary correspondents:"
psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE}" -c \
  "SELECT currency, correspondent_code, ledger_balance_id, priority, status FROM fx_nostro_correspondent ORDER BY currency, priority;"

# Demo failover rows: secondary correspondents paused until treasury promotes them.
psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE}" -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO fx_nostro_correspondent (
    currency, correspondent_code, ledger_balance_id, priority, status, updated_at
)
SELECT
    currency,
    CASE currency
        WHEN 'USD' THEN 'BNYM'
        WHEN 'EUR' THEN 'DB'
        WHEN 'GBP' THEN 'BARCLAYS'
        ELSE correspondent_code || '-STANDBY'
    END,
    ledger_balance_id,
    2,
    'paused',
    NOW()
FROM fx_nostro_correspondent
WHERE priority = 1
ON CONFLICT (currency, correspondent_code) DO NOTHING;
SQL

echo "Done. GET /internal/v1/fx/scale/status should list correspondents > 0."
