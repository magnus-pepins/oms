#!/usr/bin/env bash
# Local FIX-worker JVM: Spring profile oms-fix-worker + Chronicle JVM flags (via Gradle bootRun).
set -euo pipefail
cd "$(dirname "$0")/.."
exec ./gradlew bootRunFixWorker "$@"
