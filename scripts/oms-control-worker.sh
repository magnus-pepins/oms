#!/usr/bin/env bash
# Local control-worker JVM: Spring profile oms-control-worker + Chronicle JVM flags (via Gradle bootRun).
set -euo pipefail
cd "$(dirname "$0")/.."
exec ./gradlew bootRunControlWorker "$@"
