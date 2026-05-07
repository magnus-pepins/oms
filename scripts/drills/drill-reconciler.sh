#!/usr/bin/env bash
# Minimal drill: Prometheus endpoint exposes oms_* metrics (reconciler / tail counters when exercised).
set -euo pipefail
BASE="${OMS_BASE_URL:-http://localhost:8080}"
body=$(curl -sf "${BASE}/actuator/prometheus") || { echo "FAIL: could not scrape ${BASE}/actuator/prometheus"; exit 1; }
if ! echo "${body}" | grep -q '^jvm_memory_used_bytes'; then
  echo "FAIL: prometheus body missing expected jvm_memory_used_bytes series"
  exit 1
fi
echo "OK: prometheus scrape from ${BASE}/actuator/prometheus (extend with oms_control_* assertions under load)"
