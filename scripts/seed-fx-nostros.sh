#!/usr/bin/env bash
#
# Seed Bank Nostro balances in Ledger for the OMS FX demo.
#
# Follows the two-balance issuance convention from
# ledger/scripts/seed-demo-coin.ts (@Issuer-<CCY> carries the liability,
# @Nostro-<CCY>-Bank carries the treasury pool). For each currency:
#
#   1. Ensure @Issuer-<CCY> exists.
#   2. Ensure @Nostro-<CCY>-Bank exists (this is what OMS aggregates).
#   3. Issue starting funds by posting @Issuer-<CCY> -> @Nostro with
#      allowOverdraft=true (issuer goes negative, nostro positive). Skip
#      when the nostro already has the target balance.
#
# Idempotent: re-running skips creation + skips seeding when the target
# nostro already holds at least the target balance.
#
# Outputs the resulting balance ids in CSV form ready for
# OMS_FX_NOSTRO_BALANCE_IDS_CSV.

set -euo pipefail

LEDGER_BASE="${LEDGER_BASE:-http://127.0.0.1:30100}"
LEDGER_KEY="${LEDGER_KEY:-gf12umbgh}"
LEDGER_ID="${LEDGER_ID:-customer_ledger_id}"

NOSTROS=(
  "USD|10000000|@Nostro-USD-Bank|Bank USD nostro (demo)"
  "EUR|5000000|@Nostro-EUR-Bank|Bank EUR nostro (demo)"
  "GBP|3000000|@Nostro-GBP-Bank|Bank GBP nostro (demo)"
)

# Suspense balances per currency — booked as the bank's open FX exposure
# pre-PB-settlement. A cross-currency hedge through OMS resolves to two
# single-currency Ledger transactions, one against each suspense balance,
# because the underlying Ledger does not yet accept rate!=1 multi-currency
# transactions (J-7 ticket). With these in place, an EURUSD BUY hedge of
# 100k EUR becomes:
#   leg 1: @FX-Suspense-EUR -> @Nostro-EUR-Bank, +100k EUR
#   leg 2: @Nostro-USD-Bank  -> @FX-Suspense-USD, +108,717 USD
# The two suspense balances net-zero across all hedge actions once the PB
# leg confirms; in the meantime they represent the open FX position.
SUSPENSE=(
  "USD|0|@FX-Suspense-USD|FX hedge suspense USD"
  "EUR|0|@FX-Suspense-EUR|FX hedge suspense EUR"
  "GBP|0|@FX-Suspense-GBP|FX hedge suspense GBP"
)

curl_ledger() {
  curl -sS -H "Authorization: Bearer ${LEDGER_KEY}" -H 'Content-Type: application/json' "$@"
}

find_existing() {
  local indicator="$1"
  curl_ledger "${LEDGER_BASE}/balances?indicator=${indicator}" \
    | python3 -c "import sys,json; rows=json.load(sys.stdin); print(rows[0]['balanceId'] if rows else '', end='')"
}

balance_amount() {
  local balance_id="$1"
  curl_ledger "${LEDGER_BASE}/balances/${balance_id}" \
    | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('balance','0'), end='')"
}

create_balance() {
  local ccy="$1" indicator="$2" desc="$3"
  curl_ledger -X POST "${LEDGER_BASE}/balances" \
    -d "{\"ledgerId\":\"${LEDGER_ID}\",\"currency\":\"${ccy}\",\"indicator\":\"${indicator}\",\"metaData\":{\"kind\":\"nostro\",\"label\":\"${desc}\",\"managed_by\":\"oms-fx-demo\"}}" \
    | python3 -c "import sys,json; r=json.load(sys.stdin); print(r['balanceId'], end='')"
}

create_issuer() {
  local ccy="$1"
  local indicator="@Issuer-${ccy}"
  curl_ledger -X POST "${LEDGER_BASE}/balances" \
    -d "{\"ledgerId\":\"${LEDGER_ID}\",\"currency\":\"${ccy}\",\"indicator\":\"${indicator}\",\"metaData\":{\"kind\":\"issuer\",\"label\":\"Issuer for ${ccy} (lifetime liability)\",\"managed_by\":\"oms-fx-demo\"}}" \
    | python3 -c "import sys,json; r=json.load(sys.stdin); print(r['balanceId'], end='')"
}

issue_funds() {
  local issuer_indicator="$1" nostro_indicator="$2" amount="$3" ccy="$4"
  local reference="oms-fx-seed-${ccy}-$(date -u +%Y%m%d%H%M%S)"
  curl_ledger -X POST "${LEDGER_BASE}/transactions" \
    -d "{\"source\":\"${issuer_indicator}\",\"destination\":\"${nostro_indicator}\",\"amount\":${amount},\"currency\":\"${ccy}\",\"reference\":\"${reference}\",\"description\":\"OMS FX nostro seed\",\"sync\":true,\"allowOverdraft\":true,\"metaData\":{\"oms_fx_seed\":true}}"
}

CSV=""
for spec in "${NOSTROS[@]}"; do
  IFS='|' read -r ccy seed nostro_ind desc <<< "${spec}"
  issuer_ind="@Issuer-${ccy}"

  # Issuer
  issuer_id="$(find_existing "${issuer_ind}")"
  if [[ -z "${issuer_id}" ]]; then
    echo "Creating ${issuer_ind} (${ccy})..."
    issuer_id="$(create_issuer "${ccy}")"
    echo "  created issuer=${issuer_id}"
  fi

  # Nostro
  nostro_id="$(find_existing "${nostro_ind}")"
  if [[ -z "${nostro_id}" ]]; then
    echo "Creating ${nostro_ind} (${ccy})..."
    nostro_id="$(create_balance "${ccy}" "${nostro_ind}" "${desc}")"
    echo "  created nostro=${nostro_id}"
  fi

  current="$(balance_amount "${nostro_id}")"
  if [[ -n "${current}" ]] && awk -v c="${current}" -v s="${seed}" 'BEGIN{exit !(c+0 >= s+0)}'; then
    echo "  ${nostro_ind} already has ${current} ${ccy} (>= target ${seed}), skipping seed"
  else
    echo "  seeding ${seed} ${ccy} via ${issuer_ind} -> ${nostro_ind}"
    resp="$(issue_funds "${issuer_ind}" "${nostro_ind}" "${seed}" "${ccy}")"
    if [[ "${resp}" == *'"error"'* ]]; then
      echo "    seed FAILED: ${resp}" >&2
    fi
  fi

  if [[ -n "${CSV}" ]]; then CSV="${CSV},"; fi
  CSV="${CSV}${nostro_id}"
done

# Suspense balances — created but not seeded with starting funds. They
# carry negative balances on the EUR side and positive on the USD side
# as hedges accumulate; net-zero per (USD, EUR, GBP) currency pair.
SUSPENSE_CSV=""
for spec in "${SUSPENSE[@]}"; do
  IFS='|' read -r ccy _ ind desc <<< "${spec}"
  existing="$(find_existing "${ind}")"
  if [[ -z "${existing}" ]]; then
    echo "Creating ${ind} (${ccy})..."
    bid="$(create_balance "${ccy}" "${ind}" "${desc}")"
    echo "  created suspense=${bid}"
  else
    bid="${existing}"
    echo "${ind} already exists as ${bid}"
  fi
  if [[ -n "${SUSPENSE_CSV}" ]]; then SUSPENSE_CSV="${SUSPENSE_CSV},"; fi
  SUSPENSE_CSV="${SUSPENSE_CSV}${ccy}=${ind}"
done

echo
echo "=========================================================="
echo "OMS_FX_NOSTRO_BALANCE_IDS_CSV='${CSV}'"
echo "OMS_FX_SUSPENSE_INDICATORS_CSV='${SUSPENSE_CSV}'"
echo "=========================================================="
