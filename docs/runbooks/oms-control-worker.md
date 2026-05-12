# Runbook: `oms-control-worker` JVM

## Local (Gradle)

```bash
./scripts/oms-control-worker.sh
# or
./gradlew bootRunControlWorker
```

## Fat JAR (same artifact as monolith)

```bash
java -jar build/libs/oms-*-SNAPSHOT.jar --spring.profiles.include=oms-control-worker
```

## Control-worker fat JAR (`Start-Class` = `OmsControlWorkerBootstrap`)

Built by `./gradlew bootJarControlWorker` → `build/libs/oms-*-SNAPSHOT-control-worker.jar`.

```bash
java -jar build/libs/oms-*-SNAPSHOT-control-worker.jar
```

## Docker (optional)

Pre-build JARs, then from repo root:

```bash
./gradlew bootJar bootJarControlWorker
docker build -f docker/Dockerfile --build-arg JAR_FILE=build/libs/oms-0.1.0-SNAPSHOT.jar -t oms:local .
docker build -f docker/Dockerfile.control-worker --build-arg JAR_FILE=build/libs/oms-0.1.0-SNAPSHOT-control-worker.jar -t oms-control-worker:local .
```

`JAVA_TOOL_OPTIONS` in the Dockerfiles mirror Chronicle module opens from `build.gradle.kts`.

## Ports

- **HTTP:** `OMS_HTTP_PORT` (default `8088` in `application.yaml`).
- **Actuator / Prometheus:** `OMS_CONTROL_WORKER_MANAGEMENT_SERVER_PORT` (default `8089` in `application-oms-control-worker.yaml`).

## Verify effective topology

`GET` **`/actuator/info`** on the management port includes **`oms-topology`** (`OmsTopologyInfoContributor`): `control.postgres-write-path`, `chronicle.control-tail-enabled`, `control.chronicle-append-mode`, `grpc.enabled`, etc.

## Preconditions

- Postgres reachable; peers’ `control_outbox` rows to drain.
- **`oms.control.chronicle-append-mode=reconciler`** on this JVM (forced in `application-oms-control-worker.yaml`; validator rejects `ingress-after-commit` + this profile).
- **`oms.grpc.enabled=false`** on this JVM (validator rejects gRPC on with this profile).
- **Chronicle:** `OMS_CHRONICLE_QUEUE_DIR` must be a **shared** directory reachable from every ingress and every control/FIX JVM that participates in the same shard (NFS / PVC / host bind mount). Run **one** active `ChronicleControlTailReader` per queue directory unless you have an explicit multi-reader design (distinct `OMS_CHRONICLE_CONTROL_TAIL_ID` **and** separate queue paths or proven sharding — see `docs/chronicle-tail-driver.md`). Same tailer id on the same path from two processes can corrupt Chronicle tailer metadata.
