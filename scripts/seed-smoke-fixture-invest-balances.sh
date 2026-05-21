#!/usr/bin/env bash
# Seed minimal inv-{accountId}-USD balances for OMS smoke-test fixture accounts (Phase 2B).
#
# Creates the investor indicator balance (if missing) and credits $100.00 from @Nostro-USD-Bank
# so settlement outbox cash/fee legs stop logging unfunded_balance.
#
# Required: LEDGER_API_URL, LEDGER_KEY (or LEDGER_API_KEY)
# Optional: OMS_SMOKE_FIXTURE_ACCOUNTS — space-separated account UUIDs

set -euo pipefail

LEDGER_BASE="${LEDGER_API_URL:-http://127.0.0.1:30100}"
LEDGER_KEY="${LEDGER_KEY:-${LEDGER_API_KEY:-}}"
LEDGER_ID="${LEDGER_ID:-customer_ledger_id}"
NOSTRO_INDICATOR="${NOSTRO_USD_INDICATOR:-@Nostro-USD-Bank}"
SEED_AMOUNT_MAJOR="${OMS_SMOKE_FIXTURE_SEED_USD_MAJOR:-100}"
DESCRIPTION="OMS smoke fixture inv-USD seed 2026-05-20"

DEFAULT_ACCOUNTS=(
  "d2bc6c86-b79b-44df-ad5c-5f18fc43a67b"
  "5d88fdc6-859b-4e7e-a22d-b2442108c8e9"
)

if [[ -z "${LEDGER_KEY}" ]]; then
  echo "LEDGER_KEY or LEDGER_API_KEY required" >&2
  exit 2
fi

if [[ -n "${OMS_SMOKE_FIXTURE_ACCOUNTS:-}" ]]; then
  read -r -a ACCOUNTS <<< "${OMS_SMOKE_FIXTURE_ACCOUNTS}"
else
  ACCOUNTS=("${DEFAULT_ACCOUNTS[@]}")
fi

curl_ledger() {
  curl -sS -H "Authorization: Bearer ${LEDGER_KEY}" -H 'Content-Type: application/json' "$@"
}

find_balance() {
  local indicator="$1"
  curl_ledger "${LEDGER_BASE}/balances?indicator=${indicator}" \
    | python3 -c "import sys,json; rows=json.load(sys.stdin); print(rows[0]['balanceId'] if rows else '', end='')"
}

create_inv_balance() {
  local account_id="$1"
  local indicator="inv-${account_id}-USD"
  local existing
  existing="$(find_balance "${indicator}")"
  if [[ -n "${existing}" ]]; then
    echo "${existing}"
    return
  fi
  echo "Creating ${indicator}..." >&2
  curl_ledger -X POST "${LEDGER_BASE}/balances" \
    -d "{\"ledgerId\":\"${LEDGER_ID}\",\"currency\":\"USD\",\"indicator\":\"${indicator}\",\"identityId\":\"${account_id}\",\"metaData\":{\"kind\":\"investor\",\"managed_by\":\"oms-smoke-fixture-seed\"}}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['balanceId'], end='')"
}

fund_from_nostro() {
  local dest_id="$1"
  local account_id="$2"
  local nostro_id
  nostro_id="$(find_balance "${NOSTRO_INDICATOR}")"
  if [[ -z "${nostro_id}" ]]; then
    echo "Nostro ${NOSTRO_INDICATOR} not found — run seed-fx-nostros first" >&2
    exit 1
  fi
  local ref="oms-smoke-fixture-${account_id}"
  curl_ledger -X POST "${LEDGER_BASE}/transactions" \
    -d "{\"currency\":\"USD\",\"source\":\"${nostro_id}\",\"destination\":\"${dest_id}\",\"amount\":${SEED_AMOUNT_MAJOR},\"description\":\"${DESCRIPTION}\",\"reference\":\"${ref}\",\"sync\":true}" \
    | python3 -c "import sys,json; r=json.load(sys.stdin); print('funded', r.get('transactionId', r))"
}

echo "=== seed smoke fixture inv-USD (${#ACCOUNTS[@]} accounts, \$${SEED_AMOUNT_MAJOR} each) ==="
for acct in "${ACCOUNTS[@]}"; do
  bid="$(create_inv_balance "${acct}")"
  echo "  ${acct} -> ${bid}"
  fund_from_nostro "${bid}" "${acct}"
done
echo "=== done ==="
