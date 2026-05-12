#!/usr/bin/env bash
# Local ingress-replica JVM: Spring profile oms-ingress-replica + Chronicle JVM flags (via Gradle bootRun).
set -euo pipefail
cd "$(dirname "$0")/.."
exec ./gradlew bootRunIngressReplica "$@"
