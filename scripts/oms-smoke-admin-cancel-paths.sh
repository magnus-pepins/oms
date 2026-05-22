#!/usr/bin/env bash
# Smoke OMS admin cancel endpoints on a running ingress (pop bench).
#
# Partial coverage only — exercises admin force-cancel / postgres-only paths with
# real POSTs. The platform read-only gate is:
#   system-documentation/scripts/smoke/oms-end-to-end.sh
# Run ledger-end-to-end.sh first, then oms-end-to-end.sh, then this script if you
# need admin-path verification (plans/oms-cluster-recovery-and-hardening.md §5.2).
#
# Required env (from ~/.oms-bench.env):
#   OMS_INTERNAL_API_KEY
#
# Optional env:
#   OMS_INGRESS_BASE_URL     — default http://127.0.0.1:8088
#   OMS_SMOKE_WORKING_ORDER  — UUID of a WORKING order for force-cancel 200 test
#   OMS_SMOKE_TERMINAL_ORDER — UUID of a terminal order for force-cancel 409 test
#   OMS_SMOKE_INCLUDE_410    — if "true", place LIMIT then wipe journal (destructive; skip by default)
#
# Exit 0 when all enabled checks pass.

set -euo pipefail

BASE_URL="${OMS_INGRESS_BASE_URL:-http://127.0.0.1:8088}"
if [[ -z "${OMS_INTERNAL_API_KEY:-}" ]]; then
  echo "OMS_INTERNAL_API_KEY not set. Source ~/.oms-bench.env" >&2
  exit 2
fi

AUTH=(-H "X-OMS-Internal-Key: ${OMS_INTERNAL_API_KEY}" -H "Content-Type: application/json")

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

pass() {
  echo "PASS: $*"
}

echo "=== postgres-only unknown order → 404 ==="
code=$(curl -sS -o /tmp/oms-smoke-body.json -w "%{http_code}" -X POST \
  "${BASE_URL}/internal/v1/admin/orders/00000000-0000-0000-0000-000000000099/force-mark-cancelled-postgres-only" \
  "${AUTH[@]}" -d '{}')
[[ "$code" == "404" ]] || fail "postgres-only unknown expected 404 got ${code}: $(cat /tmp/oms-smoke-body.json)"
pass "postgres-only 404"

if [[ -n "${OMS_SMOKE_TERMINAL_ORDER:-}" ]]; then
  echo "=== force-cancel terminal → 409 ==="
  code=$(curl -sS -o /tmp/oms-smoke-body.json -w "%{http_code}" -X POST \
    "${BASE_URL}/internal/v1/admin/orders/${OMS_SMOKE_TERMINAL_ORDER}/force-cancel" \
    "${AUTH[@]}" -d '{}')
  [[ "$code" == "409" ]] || fail "force-cancel terminal expected 409 got ${code}: $(cat /tmp/oms-smoke-body.json)"
  pass "force-cancel 409 on terminal ${OMS_SMOKE_TERMINAL_ORDER}"
fi

if [[ -n "${OMS_SMOKE_WORKING_ORDER:-}" ]]; then
  echo "=== force-cancel working → 200 ==="
  code=$(curl -sS -o /tmp/oms-smoke-body.json -w "%{http_code}" -X POST \
    "${BASE_URL}/internal/v1/admin/orders/${OMS_SMOKE_WORKING_ORDER}/force-cancel" \
    "${AUTH[@]}" -d '{"reason":"oms-smoke-admin-cancel-paths"}')
  [[ "$code" == "200" ]] || fail "force-cancel working expected 200 got ${code}: $(cat /tmp/oms-smoke-body.json)"
  grep -q force_cancel_applied /tmp/oms-smoke-body.json \
    || fail "force-cancel body missing force_cancel_applied: $(cat /tmp/oms-smoke-body.json)"
  pass "force-cancel 200 on working ${OMS_SMOKE_WORKING_ORDER}"
fi

if [[ "${OMS_SMOKE_INCLUDE_410:-false}" == "true" ]]; then
  echo "=== 410 cluster_forgot (destructive — requires journal wipe) ==="
  echo "Set OMS_SMOKE_ORPHAN_ORDER to the WORKING uuid after wipe, then re-run with that id." >&2
  if [[ -n "${OMS_SMOKE_ORPHAN_ORDER:-}" ]]; then
    code=$(curl -sS -o /tmp/oms-smoke-body.json -w "%{http_code}" -X POST \
      "${BASE_URL}/internal/v1/admin/orders/${OMS_SMOKE_ORPHAN_ORDER}/force-cancel" \
      "${AUTH[@]}" -d '{}')
    [[ "$code" == "410" ]] || fail "force-cancel orphan expected 410 got ${code}: $(cat /tmp/oms-smoke-body.json)"
    grep -q cluster_forgot_order /tmp/oms-smoke-body.json \
      || fail "410 body missing cluster_forgot_order"
    code=$(curl -sS -o /tmp/oms-smoke-body.json -w "%{http_code}" -X POST \
      "${BASE_URL}/internal/v1/admin/orders/${OMS_SMOKE_ORPHAN_ORDER}/force-mark-cancelled-postgres-only" \
      "${AUTH[@]}" -d '{"reason":"smoke after 410"}')
    [[ "$code" == "200" ]] || fail "postgres-only after 410 expected 200 got ${code}: $(cat /tmp/oms-smoke-body.json)"
    pass "410 + postgres-only cleanup on ${OMS_SMOKE_ORPHAN_ORDER}"
  fi
fi

echo "=== done ==="
