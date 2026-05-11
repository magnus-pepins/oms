#!/usr/bin/env bash
# Minimal drill: health must stay up even if Chronicle directory is removed mid-run.
# Run while OMS is running elsewhere; this script only checks health once.
# Full scenario (delete queue dir, assert 201 on orders) is documented in docs/architecture.md.
set -euo pipefail
BASE="${OMS_BASE_URL:-http://localhost:8088}"
code=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/actuator/health")
if [[ "${code}" != "200" ]]; then
  echo "FAIL: health ${code} from ${BASE}/actuator/health"
  exit 1
fi
echo "OK: broken-chronicle drill baseline health -> ${code} (extend manually per runbook)"
